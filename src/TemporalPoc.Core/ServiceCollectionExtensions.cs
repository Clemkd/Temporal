using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Temporalio.Extensions.Hosting;
using TemporalPoc.Core.Activities;
using TemporalPoc.Core.Chaos;
using TemporalPoc.Core.Configuration;
using TemporalPoc.Core.Data;
using TemporalPoc.Core.Pipelines;
using TemporalPoc.Core.Storage;
using TemporalPoc.Core.Vehicles;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Core;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddTemporalPoc(this IServiceCollection services, IConfiguration configuration)
    {
        var temporal = configuration.GetSection(TemporalSettings.Section).Get<TemporalSettings>() ?? new();
        var worker = configuration.GetSection(WorkerSettings.Section).Get<WorkerSettings>() ?? new();
        var storage = configuration.GetSection(StorageSettings.Section).Get<StorageSettings>() ?? new();
        var chaos = configuration.GetSection(ChaosSettings.Section).Get<ChaosSettings>() ?? new();

        services.AddSingleton(temporal);
        services.AddSingleton(worker);
        services.AddSingleton(storage);
        services.AddSingleton(chaos);
        services.AddSingleton(configuration.GetSection(WatcherSettings.Section).Get<WatcherSettings>() ?? new());
        services.AddSingleton<ChaosMonkey>();

        services.AddSingleton<IObjectStore>(_ => storage.Provider.Equals("FileSystem", StringComparison.OrdinalIgnoreCase)
            ? new FileSystemObjectStore(storage.RootPath)
            : new S3ObjectStore(storage));

        services.AddDbContext<PocDbContext>(o => o.UseNpgsql(
            configuration.GetConnectionString("Postgres") ?? "Host=localhost;Port=55432;Database=temporal_poc;Username=poc;Password=poc",
            npgsql => npgsql.EnableRetryOnFailure(0)));
        services.AddScoped<PipelineRepository>();
        services.AddScoped<IVehicleDayProcessor, MeasurementVehicleDayProcessor>();
        services.AddScoped<IVehicleDayRunStore, EfVehicleDayRunStore>();

        // Lazy client: the process starts even if Temporal is not reachable yet.
        services.AddTemporalClient(temporal.Address, temporal.Namespace);

        if (worker.Enabled)
        {
            foreach (var queue in worker.EffectiveTaskQueues)
            {
                var builder = services
                    .AddHostedTemporalWorker(queue)
                    .ConfigureOptions(o =>
                    {
                        o.MaxConcurrentActivities = worker.MaxConcurrentActivities;
                        o.MaxConcurrentWorkflowTasks = worker.MaxConcurrentWorkflowTasks;
                        o.GracefulShutdownTimeout = TimeSpan.FromSeconds(worker.GracefulShutdownSeconds);
                        o.Identity = $"{Environment.MachineName}-{Environment.ProcessId}@{queue}";
                    });

                switch (queue)
                {
                    case TaskQueues.Ingestion:
                        builder.AddWorkflow<FileIngestionWorkflow>()
                            .AddScopedActivities<IngestionActivities>()
                            .AddScopedActivities<PipelineActivities>();
                        break;
                    case TaskQueues.Processing:
                        builder.AddWorkflow<SensorProcessingWorkflow>()
                            .AddScopedActivities<ProcessingActivities>()
                            .AddScopedActivities<PipelineActivities>();
                        break;
                    case TaskQueues.Control:
                        builder.AddWorkflow<InboxWatcherWorkflow>()
                            .AddScopedActivities<DispatchActivities>();
                        break;
                    case TaskQueues.VehicleProcessing:
                        // Separate queue: bulk reprocessing never delays file ingestion.
                        builder.AddWorkflow<VehicleProcessingWorkflow>()
                            .AddScopedActivities<VehicleDayActivities>();
                        break;
                    default:
                        throw new InvalidOperationException($"Unknown task queue '{queue}'");
                }
            }
        }

        return services;
    }
}
