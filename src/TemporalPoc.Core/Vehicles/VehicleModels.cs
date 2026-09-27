namespace TemporalPoc.Core.Vehicles;

/// <summary>A processing request for one vehicle over an inclusive range of days.</summary>
/// <param name="RequestId">Idempotency key: the same request delivered twice is merged once.</param>
/// <param name="Priority">Higher first (0 = bulk / campaign, 10 = urgent).</param>
public sealed record ProcessingRequest(
    string RequestId,
    string VehicleId,
    DateOnly From,
    DateOnly To,
    int Priority = 0,
    string? Reason = null);

/// <summary>Result of a submission, returned by the update (Update-With-Start).</summary>
public sealed record SubmitAck(
    string RequestId,
    bool Duplicate,
    int Added,
    int Merged,
    int RequeuedInProgress,
    int PendingTotal);

public enum DayOrder
{
    /// <summary>Most recent days first (within a priority level).</summary>
    MostRecentFirst,
    OldestFirst,
}

/// <summary>Tuning of the vehicle workflow. Carried over continue-as-new.</summary>
public sealed record VehicleProcessingSettings
{
    /// <summary>The workflow completes after this idle period (restarted by the next request).</summary>
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromHours(1);

    /// <summary>Continue-as-new after this many days to keep the history small.</summary>
    public int MaxDaysPerRun { get; init; } = 500;

    /// <summary>Bounds the workflow state (pending days are carried over continue-as-new).</summary>
    public int MaxPendingDays { get; init; } = 5_000;

    /// <summary>Maximum length of one request.</summary>
    public int MaxRequestDays { get; init; } = 732;

    public DayOrder Order { get; init; } = DayOrder.MostRecentFirst;

    public TimeSpan DayTimeout { get; init; } = TimeSpan.FromMinutes(10);
    public TimeSpan HeartbeatTimeout { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>Retries after the first attempt (transient failures only).</summary>
    public int MaxRetries { get; init; } = 5;
    public TimeSpan RetryInitialInterval { get; init; } = TimeSpan.FromSeconds(2);
    public TimeSpan RetryMaxInterval { get; init; } = TimeSpan.FromMinutes(2);
    public double RetryBackoffCoefficient { get; init; } = 2.0;
}

/// <summary>A day waiting to be processed (possibly requested by several requests).</summary>
public sealed record PendingDay(DateOnly Day, int Priority, DateTime FirstRequestedAt, List<string> RequestIds);

/// <summary>Failed day kept in the status (bounded list).</summary>
public sealed record FailedDay(DateOnly Day, string ErrorType, string Message, int Attempts, DateTime FailedAt);

/// <summary>State carried from one run to the next (continue-as-new).</summary>
public sealed record VehicleWorkflowState
{
    public List<PendingDay> Pending { get; init; } = [];
    public List<string> RecentRequestIds { get; init; } = [];
    public List<FailedDay> RecentFailures { get; init; } = [];
    public long Succeeded { get; init; }
    public long Failed { get; init; }
    public int Runs { get; init; } = 1;
}

public sealed record VehicleWorkflowInput(
    string VehicleId,
    VehicleProcessingSettings? Settings = null,
    VehicleWorkflowState? State = null);

/// <summary>Input of the day activity. Retry tuning is passed so the jitter matches the retry policy.</summary>
public sealed record VehicleDayInput(
    string VehicleId,
    DateOnly Day,
    IReadOnlyList<string> RequestIds,
    TimeSpan RetryInitialInterval,
    TimeSpan RetryMaxInterval,
    double RetryBackoffCoefficient);

public sealed record DayOutcome(string VehicleId, DateOnly Day, int Measurements, int Attempt);

public sealed record VehicleDayFailure(string VehicleId, DateOnly Day, IReadOnlyList<string> RequestIds, string ErrorType, string Message, int Attempts);

public sealed record VehicleStatus(
    string VehicleId,
    DateOnly? InProgress,
    int PendingCount,
    IReadOnlyList<PendingDay> NextDays,
    long Succeeded,
    long Failed,
    IReadOnlyList<FailedDay> RecentFailures,
    int Runs);

/// <summary>Business / functional error: never retried (the data or the configuration must change first).</summary>
public sealed class VehicleBusinessException(string message) : Exception(message);

public static class VehicleErrorTypes
{
    public const string Transient = "Transient";
    public const string Business = "BusinessError";
    public const string Application = "ApplicationError";
}
