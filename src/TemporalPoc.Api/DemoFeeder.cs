using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using Temporalio.Client.Schedules;
using Temporalio.Exceptions;
using TemporalPoc.Api.Endpoints;
using TemporalPoc.Core.Configuration;
using TemporalPoc.Core.Domain;
using TemporalPoc.Core.Storage;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Api;

public sealed class DemoSettings
{
    public const string Section = "Demo";

    /// <summary>Generates files automatically (enabled by docker compose, disabled by default).</summary>
    public bool Enabled { get; set; }

    /// <summary>Burst written once, on the very first start (a marker object prevents repeating it).</summary>
    public int InitialFiles { get; set; } = 300;

    /// <summary>Continuous flow afterwards (17/min ~ 1000 files/hour). 0 = no flow.</summary>
    public int FilesPerMinute { get; set; } = 17;

    public int RowsPerFile { get; set; } = 100;
    public double InvalidRatio { get; set; } = 0.05;
    public double PoisonRatio { get; set; } = 0.01;
    public double SlowRatio { get; set; } = 0.02;

    /// <summary>
    /// Interval of the Temporal Schedules (one per sensor type) that start processing jobs on the
    /// uncategorized measurements. 0 = no schedule.
    /// </summary>
    public int ProcessingEveryMinutes { get; set; } = 5;
}

/// <summary>
/// Demo mode: fills incoming/ so that the watcher starts ingestion workflows on its own right after
/// "docker compose up", keeps a steady flow of files, and creates Temporal Schedules for processing jobs.
/// </summary>
public sealed class DemoFeeder(DemoSettings settings, IObjectStore store, ITemporalClient client, ILogger<DemoFeeder> logger) : BackgroundService
{
    private const string Marker = "demo/.initial-burst-done";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!settings.Enabled)
        {
            return;
        }

        // Bootstrap (bucket, schema, watcher) runs before; give the workers a few seconds to connect.
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        await RunSafelyAsync("initial burst", InitialBurstAsync, stoppingToken);

        if (settings.ProcessingEveryMinutes > 0)
        {
            await RunSafelyAsync("processing schedules", EnsureProcessingSchedulesAsync, stoppingToken);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);

            if (settings.FilesPerMinute > 0)
            {
                await RunSafelyAsync("flow", ct => GenerateAsync(settings.FilesPerMinute, $"demo/flow/{DateTime.UtcNow:yyyyMMdd-HHmm}", ct), stoppingToken);
            }
        }
    }

    private async Task InitialBurstAsync(CancellationToken ct)
    {
        if (settings.InitialFiles <= 0 || await store.StatAsync(Marker, ct) is not null)
        {
            return;
        }
        await GenerateAsync(settings.InitialFiles, $"demo/burst-{DateTime.UtcNow:yyyyMMdd-HHmmss}", ct);
        await store.PutBytesAsync(Marker, [], "text/plain", null, ct);
    }

    private async Task GenerateAsync(int count, string prefix, CancellationToken ct)
    {
        var (_, kinds) = await FileSimulator.GenerateAsync(store, new SimulationRequest(
            count, settings.RowsPerFile, settings.InvalidRatio, settings.PoisonRatio, settings.SlowRatio, Prefix: prefix), ct);
        logger.LogInformation("Demo: {Count} files written to incoming/{Prefix}/ ({Kinds})",
            count, prefix, string.Join(", ", kinds.Select(k => $"{k.Key}={k.Value}")));
        await WatcherEndpoints.TryPokeAsync(client);
    }

    /// <summary>
    /// One Temporal Schedule per sensor type. The server starts the workflows (no timer in the app):
    /// they keep firing while the app is down, and are visible/pausable in the Temporal UI (Schedules)
    /// or with "temporal schedule list|describe|toggle|trigger".
    /// </summary>
    private async Task EnsureProcessingSchedulesAsync(CancellationToken ct)
    {
        foreach (var type in SensorCatalog.Names)
        {
            var input = new ProcessingJobInput { JobId = "", SensorType = type, BatchSize = 500 };
            var action = ScheduleActionStartWorkflow.Create(
                (SensorProcessingWorkflow wf) => wf.RunAsync(input),
                new WorkflowOptions($"processing:demo-{type}", TaskQueues.Processing)); // the server appends the run time
            var schedule = new Schedule(action, new ScheduleSpec
            {
                Intervals = [new ScheduleIntervalSpec(TimeSpan.FromMinutes(settings.ProcessingEveryMinutes))],
            })
            {
                // A run still going when the next one is due: skip it rather than stacking jobs.
                Policy = new SchedulePolicy { Overlap = ScheduleOverlapPolicy.Skip },
            };
            try
            {
                await client.CreateScheduleAsync($"demo-processing-{type}", schedule);
                logger.LogInformation("Demo: schedule demo-processing-{Type} created (every {Minutes} min)", type, settings.ProcessingEveryMinutes);
            }
            catch (ScheduleAlreadyRunningException)
            {
                // Created by a previous start: keep it (it may have been paused or edited on purpose).
            }
        }
    }

    private async Task RunSafelyAsync(string what, Func<CancellationToken, Task> action, CancellationToken ct)
    {
        try
        {
            await action(ct);
        }
        catch (Exception e) when (!ct.IsCancellationRequested)
        {
            logger.LogWarning("Demo {What} failed, will retry later: {Message}", what, e.Message);
        }
    }
}
