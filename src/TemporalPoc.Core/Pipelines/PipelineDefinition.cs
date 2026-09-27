namespace TemporalPoc.Core.Pipelines;

/// <summary>
/// A step of a pipeline. It maps to a Temporal activity (by name) and carries its own
/// retry/timeout policy and parameters, all editable at runtime without redeploying.
/// </summary>
public sealed record StepDefinition
{
    /// <summary>Activity name, see <see cref="StepCatalog"/>.</summary>
    public required string Activity { get; init; }

    public bool Enabled { get; init; } = true;

    /// <summary>
    /// Max attempts before the step is considered failed (0 = unlimited). Invalid content is reported with
    /// non retryable errors, so this limit only applies to technical errors: keep it high enough that
    /// transient failures (and crashed attempts) cannot make a valid file end up invalid.
    /// </summary>
    public int MaxAttempts { get; init; } = 10;

    public int InitialRetrySeconds { get; init; } = 1;
    public int MaxRetrySeconds { get; init; } = 30;

    /// <summary>Start-to-close timeout of one attempt.</summary>
    public int TimeoutSeconds { get; init; } = 120;

    /// <summary>
    /// A crashed worker is detected after this delay (instead of waiting for the full timeout).
    /// Activities heartbeat regularly.
    /// </summary>
    public int HeartbeatTimeoutSeconds { get; init; } = 20;

    /// <summary>Processing pipelines only: skip the batch instead of failing the whole job.</summary>
    public bool ContinueOnError { get; init; }

    public Dictionary<string, string> Parameters { get; init; } = [];
}

public sealed record PipelineDefinition
{
    public required string Name { get; init; }
    public int Version { get; init; }
    public string? Comment { get; init; }
    public required List<StepDefinition> Steps { get; init; }

    public IEnumerable<StepDefinition> EnabledSteps => Steps.Where(s => s.Enabled);
}

/// <summary>Catalog of the activities that can be used as configurable steps.</summary>
public static class StepCatalog
{
    public static class Ingestion
    {
        public const string Fetch = "ingest.fetch";
        public const string Validate = "ingest.validate";
        public const string Convert = "ingest.convert";
        public const string Store = "ingest.store";
        public const string Delay = "ingest.delay";
        public const string NotifyVehicles = "ingest.notify-vehicles";
    }

    public static class Processing
    {
        public const string Categorize = "processing.categorize";
        public const string UpdateMeasurements = "processing.update-measurements";
        public const string InsertResults = "processing.insert-results";
        public const string Cleanup = "processing.cleanup";
        public const string Delay = "processing.delay";
    }

    public static readonly IReadOnlyDictionary<string, string[]> ByPipelineKind = new Dictionary<string, string[]>
    {
        ["ingestion"] = [Ingestion.Fetch, Ingestion.Validate, Ingestion.Convert, Ingestion.Store, Ingestion.Delay, Ingestion.NotifyVehicles],
        ["processing"] = [Processing.Categorize, Processing.UpdateMeasurements, Processing.InsertResults, Processing.Cleanup, Processing.Delay],
    };

    public static string KindOf(string pipelineName) =>
        pipelineName.StartsWith("sensor-processing", StringComparison.Ordinal) || pipelineName.StartsWith("processing", StringComparison.Ordinal)
            ? "processing"
            : "ingestion";

    public static IReadOnlyList<string> Validate(PipelineDefinition definition)
    {
        var errors = new List<string>();
        var allowed = ByPipelineKind[KindOf(definition.Name)];
        if (definition.Steps.Count == 0)
        {
            errors.Add("A pipeline needs at least one step");
        }
        foreach (var step in definition.Steps)
        {
            if (!allowed.Contains(step.Activity))
            {
                errors.Add($"Unknown step '{step.Activity}'. Allowed: {string.Join(", ", allowed)}");
            }
            if (step.MaxAttempts < 0 || step.TimeoutSeconds <= 0 || step.HeartbeatTimeoutSeconds < 0)
            {
                errors.Add($"Step '{step.Activity}': invalid retry/timeout values");
            }
        }
        return errors;
    }
}

public static class DefaultPipelines
{
    public static PipelineDefinition FileIngestion => new()
    {
        Name = Configuration.PipelineNames.FileIngestion,
        Version = 1,
        Comment = "Default: fetch from S3, validate, convert to canonical units, store in database",
        Steps =
        [
            new() { Activity = StepCatalog.Ingestion.Fetch, MaxAttempts = 10 },
            new() { Activity = StepCatalog.Ingestion.Validate, MaxAttempts = 10, Parameters = new() { ["maxInvalidRowRatio"] = "0.0", ["maxRows"] = "100000" } },
            new() { Activity = StepCatalog.Ingestion.Convert, MaxAttempts = 6 },
            new() { Activity = StepCatalog.Ingestion.Store, MaxAttempts = 10, TimeoutSeconds = 300 },
        ],
    };

    public static PipelineDefinition SensorProcessing => new()
    {
        Name = Configuration.PipelineNames.SensorProcessing,
        Version = 1,
        Comment = "Default: categorize measurements, update them, insert per-sensor results",
        Steps =
        [
            new()
            {
                Activity = StepCatalog.Processing.Categorize,
                Parameters = new()
                {
                    // low;high;critical in canonical unit - overrides CategoryThresholds.Defaults
                    ["thresholds.temperature"] = "5;30;45",
                    ["thresholds.humidity"] = "20;70;90",
                },
            },
            new() { Activity = StepCatalog.Processing.UpdateMeasurements, MaxAttempts = 10 },
            new() { Activity = StepCatalog.Processing.InsertResults, MaxAttempts = 10 },
            new() { Activity = StepCatalog.Processing.Cleanup, MaxAttempts = 0, ContinueOnError = true },
        ],
    };

    public static IEnumerable<PipelineDefinition> All => [FileIngestion, SensorProcessing];
}
