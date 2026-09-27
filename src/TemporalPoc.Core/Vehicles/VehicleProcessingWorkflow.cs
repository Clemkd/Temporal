using Microsoft.Extensions.Logging;
using Temporalio.Api.Enums.V1;
using Temporalio.Common;
using Temporalio.Exceptions;
using Temporalio.Workflows;

namespace TemporalPoc.Core.Vehicles;

/// <summary>
/// One long-lived workflow per vehicle (id <c>vehicle:&lt;vehicleId&gt;</c>) acting as the vehicle's work queue.
///
/// - Requests (a range of days) arrive at any time through an Update (with an acknowledgement) or a Signal,
///   both usually sent "with start": the workflow is started if it is not running.
/// - The workflow keeps a set of pending DAYS, not a list of requests: a day requested several times and
///   not processed yet is processed once. A day requested again while it is being processed is processed
///   again afterwards (the new request may come from a change made during the processing). Already
///   processed days requested again are processed again: a new request means "recompute".
/// - One day at a time per vehicle (no concurrent writes on a vehicle), chosen by priority then date.
/// - Each day is one activity: transient failures retried with exponential backoff + jitter (max 5 retries),
///   business / application errors not retried. A failed day is recorded and the vehicle moves on.
/// - Continue-as-new keeps the history bounded (pending days and counters are carried over); the workflow
///   completes after an idle period and is started again by the next request.
/// </summary>
[Workflow("VehicleProcessing")]
public sealed class VehicleProcessingWorkflow
{
    public const string TaskQueue = Configuration.TaskQueues.VehicleProcessing;
    private const int MaxRecentRequestIds = 500;
    private const int MaxRecentFailures = 50;
    private const int MaxRequestIdsPerDay = 10;

    private readonly Dictionary<DateOnly, PendingDay> _pending = new();
    private readonly List<string> _recentRequestIds = new();
    private readonly HashSet<string> _seenRequestIds = new();
    private readonly List<FailedDay> _recentFailures = new();
    private string _vehicleId = "";
    private VehicleProcessingSettings _settings = new();
    private DateOnly? _inProgress;
    private PendingDay? _inProgressDay;
    private bool _rerunInProgress;
    private long _succeeded;
    private long _failed;
    private int _runs = 1;

    public static string WorkflowId(string vehicleId) => $"vehicle:{vehicleId}";

    /// <summary>
    /// Runs before any signal / update handler: with Update-With-Start the request handler can run before
    /// RunAsync, so the state it relies on must be initialized here.
    /// </summary>
    [WorkflowInit]
    public VehicleProcessingWorkflow(VehicleWorkflowInput input)
    {
        _vehicleId = input.VehicleId;
        _settings = input.Settings ?? new VehicleProcessingSettings();
        Restore(input.State);
    }

    [WorkflowRun]
    public async Task<VehicleStatus> RunAsync(VehicleWorkflowInput input)
    {

        var daysThisRun = 0;
        while (true)
        {
            // Wait for work. Nothing for IdleTimeout: complete (the next request starts a new run).
            var hasWork = await Workflow.WaitConditionAsync(() => _pending.Count > 0, _settings.IdleTimeout);
            if (!hasWork)
            {
                // Never complete with a handler still running or a request just merged.
                await Workflow.WaitConditionAsync(() => Workflow.AllHandlersFinished);
                if (_pending.Count == 0)
                {
                    return Status;
                }
                continue;
            }

            var day = PickNext();
            _pending.Remove(day.Day);
            _inProgress = day.Day;
            _inProgressDay = day;
            _rerunInProgress = false;

            await ProcessDayAsync(day);

            if (_rerunInProgress)
            {
                // Requested again while it was being processed: process it once more.
                Merge(day.Day, _inProgressDay.Priority, _inProgressDay.RequestIds);
            }
            _inProgress = null;
            _inProgressDay = null;
            daysThisRun++;

            if (daysThisRun >= _settings.MaxDaysPerRun || Workflow.ContinueAsNewSuggested)
            {
                await Workflow.WaitConditionAsync(() => Workflow.AllHandlersFinished);
                var next = new VehicleWorkflowInput(_vehicleId, _settings, Snapshot());
                throw Workflow.CreateContinueAsNewException((VehicleProcessingWorkflow wf) => wf.RunAsync(next));
            }
        }
    }

    private async Task ProcessDayAsync(PendingDay day)
    {
        var input = new VehicleDayInput(_vehicleId, day.Day, day.RequestIds,
            _settings.RetryInitialInterval, _settings.RetryMaxInterval, _settings.RetryBackoffCoefficient);
        try
        {
            await Workflow.ExecuteActivityAsync((VehicleDayActivities a) => a.ProcessDayAsync(input), DayActivityOptions());
            _succeeded++;
        }
        catch (ActivityFailureException e) when (!TemporalException.IsCanceledException(e))
        {
            // Retries exhausted, or non retryable (business / application) error: record it and move on.
            var (type, message, attempts) = Describe(e);
            _failed++;
            _recentFailures.Add(new FailedDay(day.Day, type, message, attempts, Workflow.UtcNow));
            if (_recentFailures.Count > MaxRecentFailures)
            {
                _recentFailures.RemoveAt(0);
            }
            Workflow.Logger.LogWarning("Vehicle {Vehicle} day {Day} failed ({Type}, {Attempts} attempts): {Message}",
                _vehicleId, day.Day, type, attempts, message);

            var failure = new VehicleDayFailure(_vehicleId, day.Day, day.RequestIds, type, message, attempts);
            await Workflow.ExecuteActivityAsync((VehicleDayActivities a) => a.RecordFailureAsync(failure), RecordActivityOptions());
        }
    }

    private ActivityOptions DayActivityOptions() => new()
    {
        StartToCloseTimeout = _settings.DayTimeout,
        HeartbeatTimeout = _settings.HeartbeatTimeout,
        RetryPolicy = new RetryPolicy
        {
            // The delays below apply to timeouts; for failures thrown by the activity, the activity sets
            // its own delay (exponential + jitter) through nextRetryDelay.
            InitialInterval = _settings.RetryInitialInterval,
            BackoffCoefficient = (float)_settings.RetryBackoffCoefficient,
            MaximumInterval = _settings.RetryMaxInterval,
            MaximumAttempts = 1 + _settings.MaxRetries,
            // Defense in depth: the activity already marks these errors as non retryable.
            NonRetryableErrorTypes = [VehicleErrorTypes.Business, VehicleErrorTypes.Application],
        },
        Summary = $"{_vehicleId} {_inProgress:yyyy-MM-dd}",
    };

    /// <summary>Bookkeeping: retried until it succeeds (a failure must never be lost).</summary>
    private static ActivityOptions RecordActivityOptions() => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(30),
        RetryPolicy = new RetryPolicy { InitialInterval = TimeSpan.FromSeconds(1), MaximumInterval = TimeSpan.FromMinutes(1), MaximumAttempts = 0 },
    };

    private (string Type, string Message, int Attempts) Describe(ActivityFailureException e)
    {
        var exhausted = e.RetryState == RetryState.MaximumAttemptsReached;
        return e.InnerException switch
        {
            ApplicationFailureException app => (app.ErrorType ?? VehicleErrorTypes.Application, app.Message, exhausted ? 1 + _settings.MaxRetries : AttemptFrom(app)),
            TimeoutFailureException timeout => ($"Timeout.{timeout.TimeoutType}", timeout.Message, exhausted ? 1 + _settings.MaxRetries : 1),
            { } inner => (inner.GetType().Name, inner.Message, exhausted ? 1 + _settings.MaxRetries : 1),
            null => ("ActivityFailure", e.Message, 1),
        };
    }

    private static int AttemptFrom(ApplicationFailureException app)
    {
        try
        {
            return app.Details.Count > 0 ? app.Details.ElementAt<int>(0) : 1;
        }
        catch (Exception)
        {
            return 1;
        }
    }

    // ------------------------------------------------------------------ requests

    /// <summary>Submits a request and returns what was merged. Sent with Update-With-Start by the API.</summary>
    [WorkflowUpdate("Submit")]
    public Task<SubmitAck> SubmitAsync(ProcessingRequest request) => Task.FromResult(Accept(request));

    [WorkflowUpdateValidator(nameof(SubmitAsync))]
    public void ValidateSubmit(ProcessingRequest request)
    {
        // Validators run before the update is recorded in history: an invalid request leaves no trace.
        if (string.IsNullOrWhiteSpace(request.RequestId))
        {
            throw new ArgumentException("RequestId is required");
        }
        if (request.VehicleId != _vehicleId)
        {
            throw new ArgumentException($"Request for {request.VehicleId} sent to the workflow of {_vehicleId}");
        }
        if (request.To < request.From)
        {
            throw new ArgumentException("To must be on or after From");
        }
        var days = request.To.DayNumber - request.From.DayNumber + 1;
        if (days > _settings.MaxRequestDays)
        {
            throw new ArgumentException($"A request covers at most {_settings.MaxRequestDays} days ({days} requested)");
        }
        if (_pending.Count + days > _settings.MaxPendingDays)
        {
            throw new ArgumentException($"Too many pending days for {_vehicleId} ({_pending.Count} + {days} > {_settings.MaxPendingDays}), retry later");
        }
    }

    /// <summary>Fire-and-forget variant (bulk submissions with Signal-With-Start). Invalid requests are ignored and logged.</summary>
    [WorkflowSignal("SubmitRequest")]
    public Task SubmitSignalAsync(ProcessingRequest request)
    {
        try
        {
            ValidateSubmit(request);
            Accept(request);
        }
        catch (ArgumentException e)
        {
            Workflow.Logger.LogWarning("Request {RequestId} for {Vehicle} rejected: {Message}", request.RequestId, _vehicleId, e.Message);
        }
        return Task.CompletedTask;
    }

    [WorkflowQuery("Status")]
    public VehicleStatus Status => new(
        _vehicleId,
        _inProgress,
        _pending.Count,
        Ordered(_pending.Values).Take(20).ToList(),
        _succeeded,
        _failed,
        _recentFailures.ToList(),
        _runs);

    private SubmitAck Accept(ProcessingRequest request)
    {
        if (!_seenRequestIds.Add(request.RequestId))
        {
            // Same request delivered twice (client retry): nothing to do.
            return new SubmitAck(request.RequestId, true, 0, 0, 0, _pending.Count);
        }
        _recentRequestIds.Add(request.RequestId);
        if (_recentRequestIds.Count > MaxRecentRequestIds)
        {
            _seenRequestIds.Remove(_recentRequestIds[0]);
            _recentRequestIds.RemoveAt(0);
        }

        int added = 0, merged = 0, requeued = 0;
        for (var day = request.From; day <= request.To; day = day.AddDays(1))
        {
            if (day == _inProgress)
            {
                _rerunInProgress = true;
                _inProgressDay = _inProgressDay! with { Priority = Math.Max(_inProgressDay.Priority, request.Priority) };
                AddRequestId(_inProgressDay.RequestIds, request.RequestId);
                requeued++;
            }
            else if (Merge(day, request.Priority, [request.RequestId]))
            {
                merged++;
            }
            else
            {
                added++;
            }
        }
        return new SubmitAck(request.RequestId, false, added, merged, requeued, _pending.Count);
    }

    /// <summary>Adds a day or merges it with the pending one. Returns true when it was already pending.</summary>
    private bool Merge(DateOnly day, int priority, IEnumerable<string> requestIds)
    {
        if (_pending.TryGetValue(day, out var existing))
        {
            var ids = existing.RequestIds;
            foreach (var id in requestIds)
            {
                AddRequestId(ids, id);
            }
            _pending[day] = existing with { Priority = Math.Max(existing.Priority, priority) };
            return true;
        }
        var list = new List<string>();
        foreach (var id in requestIds)
        {
            AddRequestId(list, id);
        }
        _pending[day] = new PendingDay(day, priority, Workflow.UtcNow, list);
        return false;
    }

    private static void AddRequestId(List<string> ids, string id)
    {
        if (!ids.Contains(id) && ids.Count < MaxRequestIdsPerDay)
        {
            ids.Add(id);
        }
    }

    // ------------------------------------------------------------------ ordering & state

    private PendingDay PickNext() => Ordered(_pending.Values).First();

    /// <summary>Deterministic order: priority, then date (per settings), then age of the request.</summary>
    private IOrderedEnumerable<PendingDay> Ordered(IEnumerable<PendingDay> days)
    {
        var byPriority = days.OrderByDescending(d => d.Priority);
        var byDate = _settings.Order == DayOrder.MostRecentFirst
            ? byPriority.ThenByDescending(d => d.Day)
            : byPriority.ThenBy(d => d.Day);
        return byDate.ThenBy(d => d.FirstRequestedAt);
    }

    private void Restore(VehicleWorkflowState? state)
    {
        if (state is null)
        {
            return;
        }
        foreach (var day in state.Pending)
        {
            _pending[day.Day] = day;
        }
        foreach (var id in state.RecentRequestIds)
        {
            _seenRequestIds.Add(id);
            _recentRequestIds.Add(id);
        }
        _recentFailures.AddRange(state.RecentFailures);
        _succeeded = state.Succeeded;
        _failed = state.Failed;
        _runs = state.Runs + 1;
    }

    private VehicleWorkflowState Snapshot() => new()
    {
        Pending = _pending.Values.OrderBy(d => d.Day).ToList(),
        RecentRequestIds = _recentRequestIds.ToList(),
        RecentFailures = _recentFailures.ToList(),
        Succeeded = _succeeded,
        Failed = _failed,
        Runs = _runs,
    };
}
