namespace TemporalPoc.Core.Configuration;

/// <summary>Connection settings for the Temporal cluster.</summary>
public sealed class TemporalSettings
{
    public const string Section = "Temporal";

    public string Address { get; set; } = "localhost:7233";
    public string Namespace { get; set; } = "default";
}

/// <summary>Names of the task queues. Ingestion and processing are isolated so one cannot starve the other.</summary>
public static class TaskQueues
{
    public const string Ingestion = "file-ingestion";
    public const string Processing = "sensor-processing";
    public const string Control = "control";

    public static readonly string[] All = [Ingestion, Processing, Control];
}

/// <summary>Worker settings. The same binary can run as API only, worker only or both.</summary>
public sealed class WorkerSettings
{
    public const string Section = "Worker";

    /// <summary>Hosts Temporal workers in this process.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Queues polled by this process (subset of <see cref="TaskQueues.All"/>, empty = all).</summary>
    public string[] TaskQueues { get; set; } = [];

    public IEnumerable<string> EffectiveTaskQueues =>
        (TaskQueues.Length == 0 ? Configuration.TaskQueues.All : TaskQueues).Distinct(StringComparer.Ordinal);

    public int MaxConcurrentActivities { get; set; } = 32;
    public int MaxConcurrentWorkflowTasks { get; set; } = 32;

    /// <summary>Time given to running activities to finish on a graceful shutdown (SIGTERM).</summary>
    public int GracefulShutdownSeconds { get; set; } = 10;
}

/// <summary>Object storage settings (S3 compatible or local file system).</summary>
public sealed class StorageSettings
{
    public const string Section = "Storage";

    /// <summary>"S3" or "FileSystem".</summary>
    public string Provider { get; set; } = "S3";

    public string Bucket { get; set; } = "sensor-files";
    public string ServiceUrl { get; set; } = "http://localhost:8333";
    public string AccessKey { get; set; } = "poc";
    public string SecretKey { get; set; } = "poc-secret";
    public string Region { get; set; } = "us-east-1";

    /// <summary>Root directory when <see cref="Provider"/> is FileSystem.</summary>
    public string RootPath { get; set; } = "./data/objects";
}

/// <summary>Settings of the long running inbox watcher workflow.</summary>
public sealed class WatcherSettings
{
    public const string Section = "Watcher";

    public bool AutoStart { get; set; } = true;
    public int ScanIntervalSeconds { get; set; } = 15;
    public int MaxFilesPerScan { get; set; } = 2000;

    /// <summary>Maximum number of FileIngestion workflows running at the same time (0 = unlimited).</summary>
    public int MaxInFlight { get; set; } = 0;

    /// <summary>Files claimed but without running workflow for this long are re-dispatched.</summary>
    public int OrphanAfterMinutes { get; set; } = 10;

    public string Pipeline { get; set; } = PipelineNames.FileIngestion;
}

public static class PipelineNames
{
    public const string FileIngestion = "file-ingestion";
    public const string SensorProcessing = "sensor-processing";
}
