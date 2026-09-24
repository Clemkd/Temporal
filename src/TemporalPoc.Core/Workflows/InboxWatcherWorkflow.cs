using Temporalio.Workflows;
using TemporalPoc.Core.Activities;

namespace TemporalPoc.Core.Workflows;

/// <summary>
/// Never ending workflow that polls the incoming/ prefix and starts one FileIngestion workflow per file.
/// Its configuration can be changed at runtime (update), it can be paused/resumed/poked (signals) and it
/// uses continue-as-new to keep its history bounded however long it runs.
/// Periodically it also re-dispatches "orphan" files (claimed but without running workflow).
/// </summary>
[Workflow("InboxWatcher")]
public sealed class InboxWatcherWorkflow
{
    public const string WorkflowId = "inbox-watcher";
    private const int IterationsPerRun = 200;
    private const int ReconcileEvery = 8;

    private WatcherState _state = null!;
    private bool _poked;

    [WorkflowRun]
    public async Task RunAsync(WatcherState state)
    {
        _state = state;
        for (var iteration = 1; ; iteration++)
        {
            await Workflow.WaitConditionAsync(() => !_state.Paused);
            _poked = false;

            var config = _state.Config;
            var scan = await Workflow.ExecuteActivityAsync(
                (DispatchActivities a) => a.ScanInboxAsync(new ScanRequest(config.Pipeline, config.MaxFilesPerScan, config.MaxInFlight)),
                ActivityOptionsFactory.Infrastructure(timeoutSeconds: 300));

            var reDispatched = 0;
            if (iteration % ReconcileEvery == 0)
            {
                var reconcile = await Workflow.ExecuteActivityAsync(
                    (DispatchActivities a) => a.ReconcileAsync(new ReconcileRequest(config.Pipeline, config.OrphanAfterMinutes)),
                    ActivityOptionsFactory.Infrastructure(timeoutSeconds: 300));
                reDispatched = reconcile.ReDispatched;
            }

            _state = _state with
            {
                TotalScans = _state.TotalScans + 1,
                TotalDispatched = _state.TotalDispatched + scan.Dispatched,
                TotalReDispatched = _state.TotalReDispatched + reDispatched,
                LastScanAt = Workflow.UtcNow,
            };

            if (iteration >= IterationsPerRun || Workflow.ContinueAsNewSuggested)
            {
                await Workflow.WaitConditionAsync(() => Workflow.AllHandlersFinished);
                var next = _state with { Generation = _state.Generation + 1 };
                throw Workflow.CreateContinueAsNewException((InboxWatcherWorkflow wf) => wf.RunAsync(next));
            }

            if (!scan.HasMore)
            {
                await Workflow.WaitConditionAsync(() => _poked, TimeSpan.FromSeconds(_state.Config.ScanIntervalSeconds));
            }
        }
    }

    /// <summary>Triggers an immediate scan (e.g. after an upload or an S3 event notification).</summary>
    [WorkflowSignal]
    public Task PokeAsync()
    {
        _poked = true;
        return Task.CompletedTask;
    }

    [WorkflowSignal]
    public Task PauseAsync()
    {
        _state = _state with { Paused = true };
        return Task.CompletedTask;
    }

    [WorkflowSignal]
    public Task ResumeAsync()
    {
        _state = _state with { Paused = false };
        _poked = true;
        return Task.CompletedTask;
    }

    /// <summary>Changes the configuration of the running watcher. Applied at the next iteration.</summary>
    [WorkflowUpdate]
    public Task<WatcherConfig> ConfigureAsync(WatcherConfig config)
    {
        _state = _state with { Config = config };
        _poked = true;
        return Task.FromResult(config);
    }

    [WorkflowUpdateValidator(nameof(ConfigureAsync))]
    public void ValidateConfigure(WatcherConfig config)
    {
        if (config.ScanIntervalSeconds is < 1 or > 3600)
        {
            throw new ArgumentException("ScanIntervalSeconds must be in [1, 3600]");
        }
        if (config.MaxFilesPerScan is < 1 or > 50_000)
        {
            throw new ArgumentException("MaxFilesPerScan must be in [1, 50000]");
        }
        if (config.MaxInFlight < 0 || config.OrphanAfterMinutes < 1 || string.IsNullOrWhiteSpace(config.Pipeline))
        {
            throw new ArgumentException("Invalid MaxInFlight / OrphanAfterMinutes / Pipeline");
        }
    }

    [WorkflowQuery]
    public WatcherState State => _state;
}
