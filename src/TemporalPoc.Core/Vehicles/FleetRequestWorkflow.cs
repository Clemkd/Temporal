using Microsoft.EntityFrameworkCore;
using Temporalio.Activities;
using Temporalio.Client;
using Temporalio.Common;
using Temporalio.Workflows;
using TemporalPoc.Core.Data;

namespace TemporalPoc.Core.Vehicles;

/// <summary>Same request for many vehicles (whole fleet after a configuration change, for instance).</summary>
/// <param name="VehicleIds">Explicit list; empty with <paramref name="AllKnownVehicles"/> = the whole fleet.</param>
/// <param name="VehiclesPerSecond">Submission pace: smooths the burst of work on the vehicle workflows.</param>
public sealed record FleetRequestInput(
    string RequestId,
    DateOnly From,
    DateOnly To,
    int Priority = 0,
    string? Reason = null,
    IReadOnlyList<string>? VehicleIds = null,
    bool AllKnownVehicles = false,
    int VehiclesPerSecond = 10,
    string? VehicleTaskQueue = null);

public sealed record FleetRequestProgress(string RequestId, int Vehicles, int Submitted);

public sealed record SubmitBatchInput(IReadOnlyList<string> VehicleIds, ProcessingRequest Template, string? VehicleTaskQueue);

/// <summary>Source of "all known vehicles". Replace by your vehicle registry.</summary>
public interface IVehicleRegistry
{
    Task<IReadOnlyList<string>> AllVehicleIdsAsync(CancellationToken ct);
}

/// <summary>Demo registry: every vehicle already processed at least once.</summary>
public sealed class ProcessedVehicleRegistry(PocDbContext db) : IVehicleRegistry
{
    public async Task<IReadOnlyList<string>> AllVehicleIdsAsync(CancellationToken ct) =>
        await db.VehicleDayRuns.Select(r => r.VehicleId).Distinct().OrderBy(v => v).ToListAsync(ct);
}

public sealed class FleetActivities(ITemporalClient client, IVehicleRegistry registry)
{
    [Activity("fleet.resolve-vehicles")]
    public Task<IReadOnlyList<string>> ResolveVehiclesAsync() =>
        registry.AllVehicleIdsAsync(ActivityExecutionContext.Current.CancellationToken);

    /// <summary>
    /// Sends the request to a batch of vehicles (Signal-With-Start). Idempotent: the vehicle workflow ignores
    /// a RequestId it has already seen, so a retried batch never duplicates work.
    /// </summary>
    [Activity("fleet.submit-batch")]
    public async Task<int> SubmitBatchAsync(SubmitBatchInput input)
    {
        var ctx = ActivityExecutionContext.Current;
        var sent = 0;
        // At most 10 calls in flight towards the Temporal server.
        await Parallel.ForEachAsync(input.VehicleIds, new ParallelOptions { MaxDegreeOfParallelism = 10, CancellationToken = ctx.CancellationToken }, async (vehicleId, _) =>
        {
            await VehicleWorkflowClient.SubmitSignalAsync(client, input.Template with { VehicleId = vehicleId }, input.VehicleTaskQueue);
            ctx.Heartbeat(Interlocked.Increment(ref sent));
        });
        return sent;
    }
}

/// <summary>
/// Durable fan-out of a request to many vehicles: if the process submitting it crashes, the workflow resumes
/// at the next batch (nothing lost, nothing sent twice thanks to the RequestId), and the submission is paced
/// (VehiclesPerSecond) instead of hitting all vehicle workflows at once.
/// </summary>
[Workflow("FleetRequest")]
public sealed class FleetRequestWorkflow
{
    private int _vehicles;
    private int _submitted;
    private string _requestId = "";

    public static string WorkflowId(string requestId) => $"fleet-request:{requestId}";

    [WorkflowRun]
    public async Task<FleetRequestProgress> RunAsync(FleetRequestInput input)
    {
        _requestId = input.RequestId;
        var options = new ActivityOptions
        {
            StartToCloseTimeout = TimeSpan.FromMinutes(2),
            HeartbeatTimeout = TimeSpan.FromSeconds(30),
            RetryPolicy = new RetryPolicy { InitialInterval = TimeSpan.FromSeconds(1), MaximumInterval = TimeSpan.FromMinutes(1), MaximumAttempts = 0 },
        };

        // Sorted + ordinal comparer: removes duplicates and gives a stable order (deterministic batches on replay).
        var vehicles = new SortedSet<string>(input.VehicleIds ?? [], StringComparer.Ordinal);
        if (input.AllKnownVehicles)
        {
            // The list is read by an activity (I/O) and recorded in the history: a replay reuses it as is.
            vehicles.UnionWith(await Workflow.ExecuteActivityAsync((FleetActivities a) => a.ResolveVehiclesAsync(), options));
        }
        var list = vehicles.ToList();
        _vehicles = list.Count;

        var batchSize = Math.Max(1, input.VehiclesPerSecond);
        // Same RequestId for every vehicle: each vehicle workflow dedups it on its own.
        var template = new ProcessingRequest(input.RequestId, "", input.From, input.To, input.Priority, input.Reason);
        for (var offset = 0; offset < list.Count; offset += batchSize)
        {
            var batch = list.Skip(offset).Take(batchSize).ToList();
            _submitted += await Workflow.ExecuteActivityAsync(
                (FleetActivities a) => a.SubmitBatchAsync(new SubmitBatchInput(batch, template, input.VehicleTaskQueue)), options);
            if (offset + batchSize < list.Count)
            {
                // Durable timer: survives a worker restart. One batch per second = VehiclesPerSecond.
                await Workflow.DelayAsync(TimeSpan.FromSeconds(1));
            }
        }
        return Progress;
    }

    [WorkflowQuery("Progress")]
    public FleetRequestProgress Progress => new(_requestId, _vehicles, _submitted);
}
