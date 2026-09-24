using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Temporalio.Api.Enums.V1;
using Temporalio.Api.TaskQueue.V1;
using Temporalio.Api.WorkflowService.V1;
using Temporalio.Client;
using TemporalPoc.Core.Chaos;
using TemporalPoc.Core.Configuration;
using TemporalPoc.Core.Data;
using TemporalPoc.Core.Storage;

namespace TemporalPoc.Api.Endpoints;

public static class OperationsEndpoints
{
    public static void MapOperationsEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/stats", async (IObjectStore store, PocDbContext db, ITemporalClient client, TemporalSettings temporal, string? filePrefix, CancellationToken ct) =>
        {
            var objects = new Dictionary<string, int>();
            foreach (var prefix in FileLayout.AllPrefixes)
            {
                objects[prefix] = await store.CountAsync(prefix + (filePrefix ?? ""), o => !o.Key.EndsWith(".error.json", StringComparison.Ordinal), ct);
            }

            var files = await db.Files.GroupBy(f => f.Status).Select(g => new { g.Key, Count = g.Count() }).ToDictionaryAsync(x => x.Key, x => x.Count, ct);
            var measurements = await db.Measurements
                .GroupBy(m => m.SensorType)
                .Select(g => new { g.Key, Total = g.LongCount(), Categorized = g.LongCount(m => m.Category != null) })
                .ToListAsync(ct);

            var workflows = new Dictionary<string, long>();
            foreach (var type in new[] { "FileIngestion", "SensorProcessing", "InboxWatcher" })
            {
                workflows[$"{type}.running"] = (await client.CountWorkflowsAsync($"WorkflowType = '{type}' AND ExecutionStatus = 'Running'")).Count;
            }
            workflows["FileIngestion.completed"] = (await client.CountWorkflowsAsync("WorkflowType = 'FileIngestion' AND ExecutionStatus = 'Completed'")).Count;
            workflows["FileIngestion.failed"] = (await client.CountWorkflowsAsync("WorkflowType = 'FileIngestion' AND ExecutionStatus = 'Failed'")).Count;

            return Results.Ok(new
            {
                at = DateTimeOffset.UtcNow,
                objects,
                files,
                measurements,
                workflows,
                taskQueues = await TaskQueueStatsAsync(client, temporal.Namespace),
            });
        }).WithTags("Operations").WithSummary("Counters (optionally restricted to files under filePrefix): objects per prefix, files per status, measurements, workflows, task queue backlogs");

        app.MapGet("/api/task-queues", async (ITemporalClient client, TemporalSettings temporal) =>
            await TaskQueueStatsAsync(client, temporal.Namespace)).WithTags("Operations");

        var chaos = app.MapGroup("/api/chaos").WithTags("Chaos");
        chaos.MapGet("/", (ChaosMonkey monkey) => monkey.Settings);
        chaos.MapPut("/", (ChaosSettings settings, ChaosMonkey monkey) =>
        {
            monkey.Settings.TransientFailureRate = Math.Clamp(settings.TransientFailureRate, 0, 1);
            monkey.Settings.PoisonMarker = settings.PoisonMarker;
            monkey.Settings.SlowMarker = settings.SlowMarker;
            monkey.Settings.SlowSeconds = settings.SlowSeconds;
            return Results.Ok(monkey.Settings);
        }).WithSummary("Changes fault injection of THIS process");
        chaos.MapPost("/crash", (int? delayMs) =>
        {
            _ = Task.Run(async () =>
            {
                await Task.Delay(delayMs ?? 100);
                Process.GetCurrentProcess().Kill(); // hard kill, no graceful shutdown
            });
            return Results.Accepted(value: new { crashingPid = Environment.ProcessId });
        }).WithSummary("Kills this process (SIGKILL-like) to test crash recovery");
    }

    private static async Task<Dictionary<string, object>> TaskQueueStatsAsync(ITemporalClient client, string ns)
    {
        var result = new Dictionary<string, object>();
        foreach (var queue in TaskQueues.All)
        {
            foreach (var type in new[] { TaskQueueType.Workflow, TaskQueueType.Activity })
            {
                try
                {
                    var response = await client.WorkflowService.DescribeTaskQueueAsync(new DescribeTaskQueueRequest
                    {
                        Namespace = ns,
                        TaskQueue = new TaskQueue { Name = queue, Kind = TaskQueueKind.Normal },
                        TaskQueueType = type,
                        ReportStats = true,
                    });
                    result[$"{queue}/{type}"] = new
                    {
                        backlog = response.Stats?.ApproximateBacklogCount ?? 0,
                        backlogAgeSeconds = response.Stats?.ApproximateBacklogAge?.ToTimeSpan().TotalSeconds ?? 0,
                        addRate = response.Stats?.TasksAddRate ?? 0,
                        dispatchRate = response.Stats?.TasksDispatchRate ?? 0,
                        pollers = response.Pollers.Count,
                    };
                }
                catch (Exception e)
                {
                    result[$"{queue}/{type}"] = new { error = e.Message };
                }
            }
        }
        return result;
    }
}
