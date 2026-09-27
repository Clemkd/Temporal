using Microsoft.EntityFrameworkCore;
using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using Temporalio.Exceptions;
using TemporalPoc.Core.Data;
using TemporalPoc.Core.Vehicles;

namespace TemporalPoc.Api.Endpoints;

public sealed record VehicleRequestBody(DateOnly From, DateOnly To, int Priority = 0, string? RequestId = null, string? Reason = null);

public sealed record BulkVehicleRequestBody(List<string> VehicleIds, DateOnly From, DateOnly To, int Priority = 0, string? RequestId = null, string? Reason = null);

public static class VehicleEndpoints
{
    public static void MapVehicleEndpoints(this IEndpointRouteBuilder app)
    {
        var vehicles = app.MapGroup("/api/vehicles").WithTags("Vehicles");

        vehicles.MapPost("/{vehicleId}/requests", async (string vehicleId, VehicleRequestBody body, ITemporalClient client) =>
        {
            // The caller may pass its own RequestId to make its retries idempotent.
            var request = new ProcessingRequest(body.RequestId ?? Guid.NewGuid().ToString("N"), vehicleId, body.From, body.To, body.Priority, body.Reason);
            try
            {
                return Results.Ok(await SubmitWithAckAsync(client, request));
            }
            catch (WorkflowUpdateFailedException e)
            {
                return Results.BadRequest(new { error = e.InnerException?.Message ?? e.Message });
            }
        }).WithSummary("Adds a range of days to the vehicle's queue (starts its workflow if needed) and returns what was merged");

        vehicles.MapPost("/requests", async (BulkVehicleRequestBody body, ITemporalClient client, CancellationToken ct) =>
        {
            var requestId = body.RequestId ?? Guid.NewGuid().ToString("N");
            var sent = 0;
            // Fire-and-forget, 50 vehicles at a time (Signal-With-Start: one atomic call per vehicle).
            await Parallel.ForEachAsync(body.VehicleIds.Distinct(), new ParallelOptions { MaxDegreeOfParallelism = 50, CancellationToken = ct }, async (vehicleId, _) =>
            {
                await SubmitSignalAsync(client, new ProcessingRequest(requestId, vehicleId, body.From, body.To, body.Priority, body.Reason));
                Interlocked.Increment(ref sent);
            });
            return Results.Accepted(value: new { requestId, vehicles = sent });
        }).WithSummary("Same request for many vehicles (e.g. after a configuration change)");

        vehicles.MapGet("/{vehicleId}", async (string vehicleId, ITemporalClient client, PocDbContext db, CancellationToken ct) =>
        {
            VehicleStatus? live = null;
            try
            {
                var handle = client.GetWorkflowHandle<VehicleProcessingWorkflow>(VehicleProcessingWorkflow.WorkflowId(vehicleId));
                if ((await handle.DescribeAsync()).Status == WorkflowExecutionStatus.Running)
                {
                    live = await handle.QueryAsync(wf => wf.Status);
                }
            }
            catch (RpcException e) when (e.Code == RpcException.StatusCode.NotFound)
            {
            }
            var days = await db.VehicleDayRuns.AsNoTracking().Where(r => r.VehicleId == vehicleId)
                .GroupBy(r => r.Status).Select(g => new { g.Key, Count = g.Count() }).ToDictionaryAsync(x => x.Key, x => x.Count, ct);
            return Results.Ok(new { vehicleId, running = live is not null, live, days });
        }).WithSummary("Live queue of the vehicle (pending days, day in progress, failures) and counts per status");

        vehicles.MapGet("/{vehicleId}/days", async (string vehicleId, DateOnly? from, DateOnly? to, PocDbContext db, CancellationToken ct) =>
        {
            var query = db.VehicleDayRuns.AsNoTracking().Where(r => r.VehicleId == vehicleId);
            if (from is { } f) query = query.Where(r => r.Day >= f);
            if (to is { } t) query = query.Where(r => r.Day <= t);
            return await query.OrderBy(r => r.Day).Take(1000).ToListAsync(ct);
        }).WithSummary("Status of each processed day");
    }

    /// <summary>Update-With-Start: starts the vehicle workflow if needed and returns the acknowledgement.</summary>
    public static async Task<SubmitAck> SubmitWithAckAsync(ITemporalClient client, ProcessingRequest request)
    {
        for (var attempt = 1; ; attempt++)
        {
            var start = WithStartWorkflowOperation.Create(
                (VehicleProcessingWorkflow wf) => wf.RunAsync(new VehicleWorkflowInput(request.VehicleId, null, null)),
                StartOptions(request.VehicleId));
            try
            {
                return await client.ExecuteUpdateWithStartWorkflowAsync(
                    (VehicleProcessingWorkflow wf) => wf.SubmitAsync(request),
                    new WorkflowUpdateWithStartOptions { StartWorkflowOperation = start });
            }
            catch (Exception e) when (attempt < 3 && e is WorkflowUpdateRpcTimeoutOrCanceledException or RpcException { Code: RpcException.StatusCode.NotFound })
            {
                // The workflow was completing (idle) at that moment: the same RequestId makes the retry safe.
                await Task.Delay(200 * attempt);
            }
        }
    }

    /// <summary>Signal-With-Start: atomic "start if needed + deliver the request", no acknowledgement.</summary>
    public static Task SubmitSignalAsync(ITemporalClient client, ProcessingRequest request)
    {
        var options = StartOptions(request.VehicleId);
        options.StartSignal = "SubmitRequest";
        options.StartSignalArgs = [request];
        return client.StartWorkflowAsync((VehicleProcessingWorkflow wf) => wf.RunAsync(new VehicleWorkflowInput(request.VehicleId, null, null)), options);
    }

    private static WorkflowOptions StartOptions(string vehicleId) => new(VehicleProcessingWorkflow.WorkflowId(vehicleId), VehicleProcessingWorkflow.TaskQueue)
    {
        IdConflictPolicy = WorkflowIdConflictPolicy.UseExisting,   // running: attach to it
        IdReusePolicy = WorkflowIdReusePolicy.AllowDuplicate,      // completed (idle): start a new run
        StaticSummary = $"Processing queue of vehicle {vehicleId}",
    };
}
