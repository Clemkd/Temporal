using TemporalPoc.Core.Pipelines;

namespace TemporalPoc.Core.Workflows;

// ---------- Ingestion ----------

public sealed record FileIngestionInput(
    string RelativeKey,
    string PipelineName = Configuration.PipelineNames.FileIngestion,
    int? PipelineVersion = null,
    // Optional explicit steps (overrides the stored pipeline).
    List<StepDefinition>? StepsOverride = null);

/// <summary>Context passed from step to step (claim-check: only keys, never file content).</summary>
public sealed record FileContext
{
    public required string RelativeKey { get; init; }
    public required string WorkingKey { get; init; }
    public string? Format { get; init; }
    public string? Checksum { get; init; }
    public long SizeBytes { get; init; }
    public string? ConvertedKey { get; init; }
    public int RowCount { get; init; }
    public int InvalidRowCount { get; init; }
    public int StoredCount { get; init; }
    /// <summary>The file was already archived/quarantined by a previous run: nothing to do.</summary>
    public bool AlreadyFinalized { get; init; }
    public string? FinalizedAs { get; init; }
    public List<string> CompletedSteps { get; init; } = [];
}

public sealed record StepInvocation(FileContext Context, Dictionary<string, string> Parameters);

public sealed record FinalizeRequest(
    FileContext Context,
    string Pipeline,
    int PipelineVersion,
    string? FailedStep = null,
    string? ErrorType = null,
    string? Error = null);

public sealed record FileIngestionResult(string RelativeKey, string Status, int StoredCount, string? FailedStep, string? Error);

public sealed record FileIngestionStatus(string RelativeKey, string Phase, string? CurrentStep, int PipelineVersion, IReadOnlyList<string> CompletedSteps, string? Error);

// ---------- Watcher ----------

public sealed record WatcherConfig(
    int ScanIntervalSeconds,
    int MaxFilesPerScan,
    int MaxInFlight,
    int OrphanAfterMinutes,
    string Pipeline);

public sealed record WatcherState
{
    public required WatcherConfig Config { get; init; }
    public bool Paused { get; init; }
    public long TotalScans { get; init; }
    public long TotalDispatched { get; init; }
    public long TotalReDispatched { get; init; }
    public DateTime? LastScanAt { get; init; }
    public int Generation { get; init; }
}

public sealed record GetPipelineRequest(string Name, int? Version);

public sealed record ScanRequest(string Pipeline, int MaxFiles, int MaxInFlight);

public sealed record ScanResult(int Listed, int Dispatched, int AlreadyRunning, int Throttled, bool HasMore);

public sealed record ReconcileRequest(string Pipeline, int OrphanAfterMinutes);

public sealed record ReconcileResult(int Checked, int ReDispatched);

// ---------- Processing ----------

public sealed record ProcessingJobInput
{
    public required string JobId { get; init; }
    public required string SensorType { get; init; }
    public int BatchSize { get; init; } = 500;
    public string PipelineName { get; init; } = Configuration.PipelineNames.SensorProcessing;
    public int? PipelineVersion { get; init; }
    /// <summary>Only process measurements that were never categorized.</summary>
    public bool OnlyUncategorized { get; init; } = true;
    /// <summary>Stop after N batches (0 = until no data is left).</summary>
    public int MaxBatches { get; init; }
    /// <summary>Continue-as-new every N batches to keep the history small.</summary>
    public int BatchesPerRun { get; init; } = 20;
    /// <summary>State carried over continue-as-new. Null on the first run.</summary>
    public ProcessingProgress? Progress { get; init; }
    /// <summary>Steps carried over continue-as-new (resolved on first run).</summary>
    public PipelineDefinition? ResolvedPipeline { get; init; }
    /// <summary>Pause flag carried over continue-as-new.</summary>
    public bool Paused { get; init; }
}

public sealed record ProcessingProgress
{
    public long Cursor { get; init; }
    public int Batches { get; init; }
    public long Processed { get; init; }
    public int FailedBatches { get; init; }
    public int Runs { get; init; } = 1;
    public Dictionary<string, long> Categories { get; init; } = [];
    public DateTime StartedAt { get; init; }
}

public sealed record BatchRef(int BatchNumber, long FirstId, long LastId, int Count);

public sealed record FetchBatchRequest(string JobId, string SensorType, long AfterId, int BatchSize, int BatchNumber, bool OnlyUncategorized);

public sealed record BatchContext(string JobId, string SensorType, BatchRef Batch, bool OnlyUncategorized, Dictionary<string, string> Parameters);

public sealed record BatchStepResult(int Affected, Dictionary<string, long>? Categories = null);

public sealed record JobUpdate(string JobId, string SensorType, string Status, string Pipeline, int PipelineVersion, long Processed, int Batches, int FailedBatches, string? Error = null);

public sealed record ProcessingStatus(
    string JobId,
    string SensorType,
    string Phase,
    bool Paused,
    int BatchSize,
    string? CurrentStep,
    ProcessingProgress Progress,
    string Pipeline,
    int PipelineVersion,
    IReadOnlyList<string> Steps);
