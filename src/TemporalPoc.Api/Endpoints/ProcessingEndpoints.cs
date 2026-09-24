using Microsoft.EntityFrameworkCore;
using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using Temporalio.Exceptions;
using TemporalPoc.Core.Configuration;
using TemporalPoc.Core.Data;
using TemporalPoc.Core.Domain;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Api.Endpoints;

public sealed record StartProcessingRequest(
    string? JobId = null,
    List<string>? SensorTypes = null,
    int BatchSize = 500,
    int? PipelineVersion = null,
    bool OnlyUncategorized = true,
    int MaxBatches = 0,
    int BatchesPerRun = 20);

public static class ProcessingEndpoints
{
    public static string WorkflowId(string jobId) => $"processing:{jobId}";

    public static void MapProcessingEndpoints(this IEndpointRouteBuilder app)
    {
        var jobs = app.MapGroup("/api/processing/jobs").WithTags("Processing");

        jobs.MapPost("/", async (StartProcessingRequest request, ITemporalClient client) =>
        {
            var baseId = request.JobId ?? $"job-{DateTime.UtcNow:yyyyMMdd-HHmmss}";
            var types = request.SensorTypes is { Count: > 0 } ? request.SensorTypes : SensorCatalog.Names.ToList();
            var unknown = types.Where(t => !SensorCatalog.Types.ContainsKey(t)).ToList();
            if (unknown.Count > 0)
            {
                return Results.BadRequest(new { error = $"Unknown sensor types: {string.Join(", ", unknown)}" });
            }

            // One long running workflow per sensor type: they run in parallel on the processing queue.
            var started = new List<object>();
            foreach (var type in types)
            {
                var input = new ProcessingJobInput
                {
                    JobId = $"{baseId}-{type}",
                    SensorType = type,
                    BatchSize = request.BatchSize,
                    PipelineVersion = request.PipelineVersion,
                    OnlyUncategorized = request.OnlyUncategorized,
                    MaxBatches = request.MaxBatches,
                    BatchesPerRun = request.BatchesPerRun,
                };
                var handle = await client.StartWorkflowAsync(
                    (SensorProcessingWorkflow wf) => wf.RunAsync(input),
                    new WorkflowOptions(WorkflowId(input.JobId), TaskQueues.Processing)
                    {
                        IdConflictPolicy = WorkflowIdConflictPolicy.UseExisting,
                        StaticSummary = $"Processing of {type} measurements",
                    });
                started.Add(new { jobId = input.JobId, workflowId = handle.Id, runId = handle.ResultRunId });
            }
            return Results.Accepted(value: started);
        }).WithSummary("Starts one processing job per sensor type (idempotent on JobId)");

        jobs.MapGet("/", async (PocDbContext db, CancellationToken ct) =>
            await db.ProcessingJobs.AsNoTracking().OrderByDescending(j => j.StartedAt).Take(200).ToListAsync(ct));

        jobs.MapGet("/{jobId}", async (string jobId, ITemporalClient client, PocDbContext db, CancellationToken ct) =>
        {
            var record = await db.ProcessingJobs.AsNoTracking().FirstOrDefaultAsync(j => j.JobId == jobId, ct);
            object? live = null;
            string? status = null;
            try
            {
                var handle = Handle(client, jobId);
                var description = await handle.DescribeAsync();
                status = description.Status.ToString();
                if (description.Status == WorkflowExecutionStatus.Running)
                {
                    live = await handle.QueryAsync(wf => wf.Status);
                }
            }
            catch (RpcException e) when (e.Code == RpcException.StatusCode.NotFound)
            {
            }
            return record is null && status is null ? Results.NotFound() : Results.Ok(new { workflowStatus = status, live, record });
        });

        jobs.MapGet("/{jobId}/results", async (string jobId, PocDbContext db, int take = 100, CancellationToken ct = default) =>
            await db.ProcessingResults.AsNoTracking().Where(r => r.JobId == jobId)
                .OrderBy(r => r.BatchNumber).ThenBy(r => r.SensorId).Take(Math.Clamp(take, 1, 5000)).ToListAsync(ct));

        jobs.MapPost("/{jobId}/pause", async (string jobId, ITemporalClient client) =>
        {
            await Handle(client, jobId).SignalAsync(wf => wf.PauseAsync());
            return Results.Accepted();
        });

        jobs.MapPost("/{jobId}/resume", async (string jobId, ITemporalClient client) =>
        {
            await Handle(client, jobId).SignalAsync(wf => wf.ResumeAsync());
            return Results.Accepted();
        });

        jobs.MapPost("/{jobId}/reload-pipeline", async (string jobId, ITemporalClient client) =>
        {
            await Handle(client, jobId).SignalAsync(wf => wf.ReloadPipelineAsync());
            return Results.Accepted();
        }).WithSummary("The running job switches to the latest pipeline version at the next batch");

        jobs.MapPut("/{jobId}/batch-size/{size:int}", async (string jobId, int size, ITemporalClient client) =>
        {
            try
            {
                var previous = await Handle(client, jobId).ExecuteUpdateAsync(wf => wf.SetBatchSizeAsync(size));
                return Results.Ok(new { previous, current = size });
            }
            catch (WorkflowUpdateFailedException e)
            {
                return Results.BadRequest(new { error = e.InnerException?.Message ?? e.Message });
            }
        });

        jobs.MapDelete("/{jobId}", async (string jobId, ITemporalClient client) =>
        {
            await Handle(client, jobId).CancelAsync();
            return Results.Accepted();
        });
    }

    private static WorkflowHandle<SensorProcessingWorkflow> Handle(ITemporalClient client, string jobId) =>
        client.GetWorkflowHandle<SensorProcessingWorkflow>(WorkflowId(jobId));
}
