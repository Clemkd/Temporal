using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using Temporalio.Exceptions;
using Temporalio.Worker;
using TemporalPoc.Core.Vehicles;

namespace TemporalPoc.Tests;

/// <summary>
/// End-to-end tests of the vehicle workflow on a real (local) Temporal server, with the REAL activities
/// (retry classification, jitter) and a fake day processor.
/// </summary>
[Collection(WorkflowCollection.Name)]
public class VehicleProcessingWorkflowTests(WorkflowEnvironmentFixture fixture)
{
    private static readonly DateOnly Jan1 = new(2026, 1, 1);

    private static readonly VehicleProcessingSettings Fast = new()
    {
        IdleTimeout = TimeSpan.FromSeconds(1),
        RetryInitialInterval = TimeSpan.FromMilliseconds(50),
        RetryMaxInterval = TimeSpan.FromMilliseconds(200),
        HeartbeatTimeout = TimeSpan.FromSeconds(10),
        DayTimeout = TimeSpan.FromSeconds(30),
    };

    private sealed class FakeProcessor : IVehicleDayProcessor
    {
        public ConcurrentQueue<(DateOnly Day, int Attempt, DateTime At)> Calls { get; } = new();
        public Func<DateOnly, int, Exception?> Fail { get; set; } = (_, _) => null;
        public ConcurrentDictionary<DateOnly, TaskCompletionSource> Holds { get; } = new();
        public TaskCompletionSource<DateOnly> FirstStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<int> ProcessAsync(string vehicleId, DateOnly day, IReadOnlyList<string> requestIds, int attempt, CancellationToken ct)
        {
            Calls.Enqueue((day, attempt, DateTime.UtcNow));
            FirstStarted.TrySetResult(day);
            if (Holds.TryGetValue(day, out var hold))
            {
                await hold.Task.WaitAsync(ct);
            }
            if (Fail(day, attempt) is { } e)
            {
                throw e;
            }
            return 1;
        }

        public int CountOf(DateOnly day) => Calls.Count(c => c.Day == day);
    }

    private sealed class FakeStore : IVehicleDayRunStore
    {
        public ConcurrentQueue<VehicleDayFailure> Failures { get; } = new();

        public Task RecordFailureAsync(VehicleDayFailure failure, CancellationToken ct)
        {
            Failures.Enqueue(failure);
            return Task.CompletedTask;
        }
    }

    private sealed record Harness(string VehicleId, string Queue, FakeProcessor Processor, FakeStore Store, TemporalWorker Worker, ITemporalClient Client)
    {
        public WorkflowHandle<VehicleProcessingWorkflow> Handle => Client.GetWorkflowHandle<VehicleProcessingWorkflow>(VehicleProcessingWorkflow.WorkflowId(VehicleId));

        public ProcessingRequest Request(string id, DateOnly from, DateOnly to, int priority = 0) => new(id, VehicleId, from, to, priority);

        private WorkflowOptions Options() => new(VehicleProcessingWorkflow.WorkflowId(VehicleId), Queue)
        {
            IdConflictPolicy = WorkflowIdConflictPolicy.UseExisting,
            IdReusePolicy = WorkflowIdReusePolicy.AllowDuplicate,
        };

        public Task SignalAsync(ProcessingRequest r, VehicleProcessingSettings settings)
        {
            var options = Options();
            options.StartSignal = "SubmitRequest";
            options.StartSignalArgs = [r];
            return Client.StartWorkflowAsync((VehicleProcessingWorkflow wf) => wf.RunAsync(new VehicleWorkflowInput(VehicleId, settings, null)), options);
        }

        public Task<SubmitAck> SubmitAsync(ProcessingRequest r, VehicleProcessingSettings settings) =>
            Client.ExecuteUpdateWithStartWorkflowAsync(
                (VehicleProcessingWorkflow wf) => wf.SubmitAsync(r),
                new WorkflowUpdateWithStartOptions
                {
                    StartWorkflowOperation = WithStartWorkflowOperation.Create(
                        (VehicleProcessingWorkflow wf) => wf.RunAsync(new VehicleWorkflowInput(VehicleId, settings, null)), Options()),
                });
    }

    private Harness Create()
    {
        var processor = new FakeProcessor();
        var store = new FakeStore();
        var queue = $"vehicles-test-{Guid.NewGuid():N}";
        var activities = new VehicleDayActivities(processor, store, NullLogger<VehicleDayActivities>.Instance);
        var worker = new TemporalWorker(fixture.Env.Client, new TemporalWorkerOptions(queue)
            .AddWorkflow<VehicleProcessingWorkflow>()
            .AddAllActivities(activities));
        return new Harness($"veh-{Guid.NewGuid():N}"[..16], queue, processor, store, worker, fixture.Env.Client);
    }

    [Fact]
    public async Task Overlapping_requests_process_each_pending_day_once()
    {
        var h = Create();
        using var _ = h.Worker;
        // Both requests are merged before any processing (signals sent before the worker runs).
        await h.SignalAsync(h.Request("r1", Jan1, Jan1.AddDays(9)), Fast);
        await h.SignalAsync(h.Request("r2", Jan1.AddDays(4), Jan1.AddDays(14)), Fast);
        await h.SignalAsync(h.Request("r2", Jan1.AddDays(4), Jan1.AddDays(14)), Fast);   // same request delivered twice

        var status = await h.Worker.ExecuteAsync(() => h.Handle.GetResultAsync<VehicleStatus>());

        Assert.Equal(15, h.Processor.Calls.Count);
        Assert.All(Enumerable.Range(0, 15), i => Assert.Equal(1, h.Processor.CountOf(Jan1.AddDays(i))));
        Assert.Equal(15, status.Succeeded);
        Assert.Equal(0, status.PendingCount);
    }

    [Fact]
    public async Task Submit_acknowledges_merges_and_reprocesses_a_day_requested_during_its_processing()
    {
        var h = Create();
        using var _ = h.Worker;
        var jan5 = Jan1.AddDays(4);
        h.Processor.Holds[jan5] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await h.Worker.ExecuteAsync(async () =>
        {
            var a = await h.SubmitAsync(h.Request("A", Jan1, jan5), Fast);            // 1..5, most recent first -> 5 starts
            Assert.Equal(5, a.Added);
            Assert.Equal(jan5, await h.Processor.FirstStarted.Task);

            var b = await h.SubmitAsync(h.Request("B", Jan1.AddDays(2), Jan1.AddDays(7)), Fast);   // 3..8
            Assert.Equal(2, b.Merged);              // 3 and 4 already pending
            Assert.Equal(1, b.RequeuedInProgress);  // 5 is being processed
            Assert.Equal(3, b.Added);               // 6, 7, 8

            var again = await h.SubmitAsync(h.Request("B", Jan1.AddDays(2), Jan1.AddDays(7)), Fast);
            Assert.True(again.Duplicate);

            h.Processor.Holds[jan5].SetResult();
            await h.Handle.GetResultAsync<VehicleStatus>();
        });

        Assert.Equal(2, h.Processor.CountOf(jan5));    // processed, then processed again for request B
        Assert.All(new[] { 0, 1, 2, 3, 5, 6, 7 }, i => Assert.Equal(1, h.Processor.CountOf(Jan1.AddDays(i))));
    }

    [Fact]
    public async Task Urgent_day_is_processed_before_the_rest_of_a_bulk_request()
    {
        var h = Create();
        using var _ = h.Worker;
        var mar1 = new DateOnly(2026, 3, 1);
        var jan10 = Jan1.AddDays(9);
        h.Processor.Holds[jan10] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await h.Worker.ExecuteAsync(async () =>
        {
            await h.SubmitAsync(h.Request("bulk", Jan1, jan10, priority: 0), Fast);
            Assert.Equal(jan10, await h.Processor.FirstStarted.Task);
            await h.SubmitAsync(h.Request("urgent", mar1, mar1, priority: 10), Fast);
            h.Processor.Holds[jan10].SetResult();
            await h.Handle.GetResultAsync<VehicleStatus>();
        });

        var order = h.Processor.Calls.Select(c => c.Day).ToList();
        Assert.Equal([jan10, mar1, Jan1.AddDays(8)], order.Take(3));
    }

    [Fact]
    public async Task Transient_failures_are_retried_with_backoff_then_succeed()
    {
        var h = Create();
        using var _ = h.Worker;
        h.Processor.Fail = (_, attempt) => attempt < 3 ? new IOException("storage unavailable") : null;

        await h.SignalAsync(h.Request("r", Jan1, Jan1), Fast);
        var status = await h.Worker.ExecuteAsync(() => h.Handle.GetResultAsync<VehicleStatus>());

        var calls = h.Processor.Calls.ToList();
        Assert.Equal([1, 2, 3], calls.Select(c => c.Attempt));
        Assert.Equal(1, status.Succeeded);
        Assert.Empty(h.Store.Failures);
        // Retry delays follow the activity's backoff (>= half the base delay).
        Assert.True((calls[1].At - calls[0].At).TotalMilliseconds >= 20);
    }

    [Fact]
    public async Task Persistent_transient_failure_stops_after_5_retries_and_the_vehicle_moves_on()
    {
        var h = Create();
        using var _ = h.Worker;
        var jan2 = Jan1.AddDays(1);
        h.Processor.Fail = (day, _) => day == jan2 ? new TimeoutException("timeout") : null;

        await h.SignalAsync(h.Request("r", Jan1, jan2), Fast);
        var status = await h.Worker.ExecuteAsync(() => h.Handle.GetResultAsync<VehicleStatus>());

        Assert.Equal(6, h.Processor.CountOf(jan2));      // 1 attempt + 5 retries
        Assert.Equal(1, h.Processor.CountOf(Jan1));      // the other day is still processed
        var failure = Assert.Single(h.Store.Failures);
        Assert.Equal((jan2, VehicleErrorTypes.Transient, 6), (failure.Day, failure.ErrorType, failure.Attempts));
        Assert.Equal((1, 1), (status.Succeeded, status.Failed));
    }

    [Theory]
    [InlineData(true, VehicleErrorTypes.Business)]
    [InlineData(false, VehicleErrorTypes.Application)]
    public async Task Business_and_application_errors_are_not_retried(bool business, string expectedType)
    {
        var h = Create();
        using var _ = h.Worker;
        h.Processor.Fail = (day, _) => day != Jan1 ? null
            : business ? new VehicleBusinessException("configuration missing") : new InvalidCastException("bug");

        await h.SignalAsync(h.Request("r", Jan1, Jan1.AddDays(2)), Fast);
        var status = await h.Worker.ExecuteAsync(() => h.Handle.GetResultAsync<VehicleStatus>());

        Assert.Equal(1, h.Processor.CountOf(Jan1));
        Assert.Equal(expectedType, Assert.Single(h.Store.Failures).ErrorType);
        Assert.Equal((2, 1), (status.Succeeded, status.Failed));
    }

    [Fact]
    public async Task Continue_as_new_keeps_the_queue_and_the_deduplication()
    {
        var h = Create();
        using var _ = h.Worker;
        var settings = Fast with { MaxDaysPerRun = 3 };
        await h.SignalAsync(h.Request("r1", Jan1, Jan1.AddDays(9)), settings);

        var status = await h.Worker.ExecuteAsync(async () =>
        {
            await h.Processor.FirstStarted.Task;
            // Same request id sent again later (possibly after a continue-as-new): ignored.
            await h.SignalAsync(h.Request("r1", Jan1, Jan1.AddDays(9)), settings);
            return await h.Handle.GetResultAsync<VehicleStatus>();
        });

        Assert.All(Enumerable.Range(0, 10), i => Assert.Equal(1, h.Processor.CountOf(Jan1.AddDays(i))));
        Assert.Equal(10, status.Succeeded);
        Assert.True(status.Runs >= 4, $"runs = {status.Runs}");
    }

    [Fact]
    public async Task Idle_workflow_completes_and_a_later_request_starts_a_new_run()
    {
        var h = Create();
        using var _ = h.Worker;
        await h.Worker.ExecuteAsync(async () =>
        {
            await h.SubmitAsync(h.Request("jan", Jan1, Jan1.AddDays(30)), Fast);
            var first = await h.Handle.GetResultAsync<VehicleStatus>();
            Assert.Equal(31, first.Succeeded);

            // Weeks later: a request for 2026-03-01 on the same vehicle.
            var ack = await h.SubmitAsync(h.Request("mar", new DateOnly(2026, 3, 1), new DateOnly(2026, 3, 1)), Fast);
            Assert.Equal(1, ack.Added);
            var second = await h.Handle.GetResultAsync<VehicleStatus>();
            Assert.Equal(1, second.Succeeded);
        });
        Assert.Equal(1, h.Processor.CountOf(new DateOnly(2026, 3, 1)));
    }

    [Fact]
    public async Task Invalid_request_is_rejected_by_the_update_validator()
    {
        var h = Create();
        using var _ = h.Worker;
        await h.Worker.ExecuteAsync(async () =>
        {
            await h.SubmitAsync(h.Request("ok", Jan1, Jan1), Fast);
            await Assert.ThrowsAsync<WorkflowUpdateFailedException>(() =>
                h.SubmitAsync(h.Request("bad", Jan1.AddDays(5), Jan1), Fast));   // To < From
            await h.Handle.GetResultAsync<VehicleStatus>();
        });
        Assert.Single(h.Processor.Calls);
    }

    [Fact]
    public async Task History_replays_deterministically()
    {
        var h = Create();
        using var _ = h.Worker;
        await h.SignalAsync(h.Request("r1", Jan1, Jan1.AddDays(4)), Fast);
        await h.SignalAsync(h.Request("r2", Jan1.AddDays(2), Jan1.AddDays(6), priority: 5), Fast);
        await h.Worker.ExecuteAsync(() => h.Handle.GetResultAsync<VehicleStatus>());

        var history = await h.Handle.FetchHistoryAsync();
        var replayer = new WorkflowReplayer(new WorkflowReplayerOptions().AddWorkflow<VehicleProcessingWorkflow>());
        var result = await replayer.ReplayWorkflowAsync(history);
        Assert.Null(result.ReplayFailure);
    }
}
