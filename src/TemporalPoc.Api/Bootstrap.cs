using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using TemporalPoc.Core.Configuration;
using TemporalPoc.Core.Data;
using TemporalPoc.Core.Storage;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Api;

/// <summary>
/// Startup: creates the database schema and the bucket (blocking, before the workers start), then
/// makes sure the inbox watcher workflow is running (idempotent: fixed workflow id + UseExisting).
/// </summary>
public sealed class Bootstrap(IServiceProvider services, WatcherSettings watcher, ILogger<Bootstrap> logger) : IHostedService
{
    private readonly CancellationTokenSource _stopping = new();

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using (var scope = services.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<PocDbContext>();
            await DatabaseInitializer.InitializeAsync(db, logger, cancellationToken);
        }

        var store = services.GetRequiredService<IObjectStore>();
        await RetryAsync("object storage", () => store.EnsureReadyAsync(cancellationToken), cancellationToken);

        // Hosted workers fail (and stop the host) if Temporal is unreachable when they start:
        // wait for it here, hosted services start sequentially.
        var client = services.GetRequiredService<ITemporalClient>();
        await RetryAsync("temporal", async () =>
        {
            if (!await client.Connection.CheckHealthAsync(options: new() { CancellationToken = cancellationToken }))
            {
                throw new InvalidOperationException("Temporal is not serving");
            }
        }, cancellationToken);

        if (watcher.AutoStart)
        {
            _ = Task.Run(() => RetryAsync("inbox watcher", () => EnsureWatcherAsync(client, watcher), _stopping.Token));
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _stopping.Cancel();
        return Task.CompletedTask;
    }

    public static async Task EnsureWatcherAsync(ITemporalClient client, WatcherSettings settings)
    {
        var state = new WatcherState
        {
            Config = new WatcherConfig(settings.ScanIntervalSeconds, settings.MaxFilesPerScan, settings.MaxInFlight, settings.OrphanAfterMinutes, settings.Pipeline),
        };
        await client.StartWorkflowAsync(
            (InboxWatcherWorkflow wf) => wf.RunAsync(state),
            new WorkflowOptions(InboxWatcherWorkflow.WorkflowId, TaskQueues.Control)
            {
                IdConflictPolicy = WorkflowIdConflictPolicy.UseExisting,
                IdReusePolicy = WorkflowIdReusePolicy.AllowDuplicate,
                StaticSummary = "Scans incoming/ and dispatches one ingestion workflow per file",
            });
    }

    private async Task RetryAsync(string what, Func<Task> action, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await action();
                logger.LogInformation("{What} ready", what);
                return;
            }
            catch (Exception e) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning("{What} not ready (attempt {Attempt}): {Message}", what, attempt, e.Message);
                await Task.Delay(TimeSpan.FromSeconds(Math.Min(10, attempt * 2)), ct);
            }
        }
    }
}
