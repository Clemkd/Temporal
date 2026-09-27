namespace TemporalPoc.Core.Data;

public static class FileStatus
{
    public const string Processing = "Processing";
    public const string Stored = "Stored";
    public const string Invalid = "Invalid";
}

public sealed class IngestedFile
{
    /// <summary>Path relative to the life-cycle prefix (see FileLayout).</summary>
    public string Key { get; set; } = "";
    public string Status { get; set; } = FileStatus.Processing;
    public string? WorkflowId { get; set; }
    public string? RunId { get; set; }
    public string? Pipeline { get; set; }
    public int? PipelineVersion { get; set; }
    public string? Format { get; set; }
    public string? Checksum { get; set; }
    public long SizeBytes { get; set; }
    public int RowCount { get; set; }
    public int InvalidRowCount { get; set; }
    public int StoredCount { get; set; }
    public string? FailedStep { get; set; }
    public string? Error { get; set; }
    public int Attempts { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
}

public sealed class Measurement
{
    public long Id { get; set; }
    public string FileKey { get; set; } = "";
    public int LineNumber { get; set; }
    public string SensorId { get; set; } = "";
    public string SensorType { get; set; } = "";
    public DateTimeOffset Timestamp { get; set; }
    public double Value { get; set; }
    public string Unit { get; set; } = "";
    public string? Category { get; set; }
    public DateTimeOffset? CategorizedAt { get; set; }
    public string? CategorizedByJob { get; set; }
}

/// <summary>Claim-check table between the "categorize" and "update" steps of a processing batch.</summary>
public sealed class CategorizationStaging
{
    public string JobId { get; set; } = "";
    public long MeasurementId { get; set; }
    public int BatchNumber { get; set; }
    public string Category { get; set; } = "";
}

public sealed class ProcessingResult
{
    public long Id { get; set; }
    public string JobId { get; set; } = "";
    public int BatchNumber { get; set; }
    public string SensorId { get; set; } = "";
    public string SensorType { get; set; } = "";
    public int Count { get; set; }
    public double Min { get; set; }
    public double Max { get; set; }
    public double Avg { get; set; }
    public int LowCount { get; set; }
    public int NormalCount { get; set; }
    public int HighCount { get; set; }
    public int CriticalCount { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}

public static class JobStatus
{
    public const string Running = "Running";
    public const string Completed = "Completed";
    public const string Failed = "Failed";
}

public sealed class ProcessingJob
{
    public string JobId { get; set; } = "";
    public string SensorType { get; set; } = "";
    public string Status { get; set; } = JobStatus.Running;
    public string? Pipeline { get; set; }
    public int? PipelineVersion { get; set; }
    public long Processed { get; set; }
    public int Batches { get; set; }
    public int FailedBatches { get; set; }
    public string? Error { get; set; }
    public DateTimeOffset StartedAt { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
}

public sealed class PipelineDefinitionRecord
{
    public string Name { get; set; } = "";
    public int Version { get; set; }
    public string DefinitionJson { get; set; } = "";
    public string? Comment { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}

public static class VehicleDayStatus
{
    public const string Succeeded = "Succeeded";
    public const string Failed = "Failed";
}

/// <summary>Last processing status of a (vehicle, day). One row per pair, overwritten on each processing.</summary>
public sealed class VehicleDayRun
{
    public string VehicleId { get; set; } = "";
    public DateOnly Day { get; set; }
    public string Status { get; set; } = VehicleDayStatus.Succeeded;
    public int Attempts { get; set; }
    public int Runs { get; set; }
    public string? RequestIds { get; set; }
    public string? ErrorType { get; set; }
    public string? Error { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
    public DateTimeOffset? SucceededAt { get; set; }
}

/// <summary>Result of the processing of a (vehicle, day): replaced on every reprocessing (idempotent).</summary>
public sealed class VehicleDayResult
{
    public string VehicleId { get; set; } = "";
    public DateOnly Day { get; set; }
    public int MeasurementCount { get; set; }
    public double? Min { get; set; }
    public double? Max { get; set; }
    public double? Avg { get; set; }
    public DateTimeOffset ComputedAt { get; set; }
}
