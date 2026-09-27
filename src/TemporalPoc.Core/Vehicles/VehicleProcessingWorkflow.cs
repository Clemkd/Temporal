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
/// - Rate limits per (vehicle, day), whatever the source of the request (file, daily, mass): the current
///   operating day is processed at most once per RealtimeMinInterval (1 min), any other day at most once per
///   DailyMinInterval (1 h). Requests arriving meanwhile are merged and run when the interval has elapsed:
///   nothing is dropped, every request is followed by a processing that starts after it.
/// - Each day is one activity: transient failures retried with exponential backoff + jitter (max 5 retries),
///   business / application errors not retried. A failed day is recorded and the vehicle moves on.
/// - Continue-as-new keeps the history bounded (pending days and counters are carried over); the workflow
///   completes after an idle period and is started again by the next request.
/// </summary>
[Workflow("VehicleProcessing")]
public sealed class VehicleProcessingWorkflow
{
    public const string TaskQueue = Configuration.TaskQueues.VehicleProcessing;

    // Bounds of the state kept in the workflow: everything is carried over continue-as-new and serialized in
    // the history, so nothing may grow without limit (a payload is limited to 2 MB).
    private const int MaxRecentRequestIds = 500;
    private const int MaxRecentFailures = 50;
    private const int MaxRequestIdsPerDay = 10;

    // The vehicle's queue: one entry per DAY (the key), which is what merges duplicated requests.
    private readonly Dictionary<DateOnly, PendingDay> _pending = new();

    // Last request ids received (FIFO list + set for O(1) lookups): a request delivered twice is ignored.
    private readonly List<string> _recentRequestIds = new();
    private readonly HashSet<string> _seenRequestIds = new();

    private readonly List<FailedDay> _recentFailures = new();

    // Start time of the last processing of each recently processed day: drives the minimum intervals.
    private readonly Dictionary<DateOnly, DateTime> _lastStarts = new();

    private TimeZoneInfo _timeZone = TimeZoneInfo.Utc;

    // Incremented by every accepted request: lets the main loop wake up from a timer when something new arrives.
    private long _changes;

    private string _vehicleId = "";
    private VehicleProcessingSettings _settings = new();

    // Day being processed (activity running) and its queue entry; _rerunInProgress = requested again meanwhile.
    private DateOnly? _inProgress;
    private PendingDay? _inProgressDay;
    private bool _rerunInProgress;

    private long _succeeded;
    private long _failed;
    private int _runs = 1;   // number of runs (1 + number of continue-as-new), for diagnostics

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
        // Reads the OS time zone database: all workers must share the same tzdata, otherwise a replay on another
        // worker could compute another operating day (non determinism).
        _timeZone = TimeZoneInfo.FindSystemTimeZoneById(_settings.OperatingTimeZone);
        // State carried over by a previous run (continue-as-new): pending days, dedup ids, counters.
        Restore(input.State);
    }

    [WorkflowRun]
    public async Task<VehicleStatus> RunAsync(VehicleWorkflowInput input)
    {
        // `input` was already consumed by the [WorkflowInit] constructor: Temporal passes it to both.
        var daysThisRun = 0;
        while (true)
        {
            if (_pending.Count == 0)
            {
                // Nothing to do. Nothing for IdleTimeout: complete (the next request starts a new run).
                // WaitConditionAsync is a durable timer + condition: it returns true as soon as a request is merged
                // (the condition is re-evaluated after each signal/update), false when the timeout fires.
                var hasWork = await Workflow.WaitConditionAsync(() => _pending.Count > 0, _settings.IdleTimeout);
                if (!hasWork)
                {
                    // Never complete with a handler still running or a request just merged: an update accepted
                    // in the same workflow task would otherwise be lost with the completed run.
                    await Workflow.WaitConditionAsync(() => Workflow.AllHandlersFinished);
                    if (_pending.Count == 0)
                    {
                        return Status;
                    }
                }
                continue;   // re-evaluate from the top (a request may have arrived during the checks)
            }

            // Workflow.UtcNow, never DateTime.UtcNow: it is the time of the current workflow task, recorded in
            // the history, so a replay computes exactly the same decisions.
            var now = Workflow.UtcNow;
            PruneLastStarts(now);
            // Days whose minimum interval since their last processing has elapsed (or never processed).
            var eligible = _pending.Values.Where(d => EligibleAt(d.Day, now) <= now).ToList();
            if (eligible.Count == 0)
            {
                // Every pending day is within its minimum interval: sleep until the first one becomes
                // eligible, or until a new request arrives (it may concern another, eligible, day).
                var wakeAt = _pending.Values.Min(d => EligibleAt(d.Day, now));
                var seen = _changes;   // snapshot: the condition becomes true when a new request is accepted
                var delay = wakeAt - now;
                // A timer needs a strictly positive duration.
                await Workflow.WaitConditionAsync(() => _changes != seen, delay > TimeSpan.FromMilliseconds(1) ? delay : TimeSpan.FromMilliseconds(1));
                continue;
            }

            // Highest priority first, then date, then oldest request (see Ordered).
            var day = Ordered(eligible).First();
            _pending.Remove(day.Day);
            _inProgress = day.Day;
            _inProgressDay = day;
            _rerunInProgress = false;
            // The interval is counted from this scheduling decision (not from the end of the processing).
            _lastStarts[day.Day] = now;

            await ProcessDayAsync(day);

            if (_rerunInProgress)
            {
                // Requested again while it was being processed: process it once more, after its interval.
                // The new request may come from a change made during the processing, whose result could be stale.
                // (_inProgressDay may have been replaced by a handler meanwhile, with the higher priority/new ids.)
                Merge(day.Day, _inProgressDay!.Priority, _inProgressDay.RequestIds);
            }
            _inProgress = null;
            _inProgressDay = null;
            daysThisRun++;

            // Continue-as-new: same workflow id, fresh (empty) history, state passed as input. Keeps the history
            // (and the replay cost after a worker restart) bounded however long the vehicle keeps receiving work.
            // ContinueAsNewSuggested is raised by the server when the history gets large.
            if (daysThisRun >= _settings.MaxDaysPerRun || Workflow.ContinueAsNewSuggested)
            {
                await Workflow.WaitConditionAsync(() => Workflow.AllHandlersFinished);   // do not drop an in-flight update
                var next = new VehicleWorkflowInput(_vehicleId, _settings, Snapshot());
                throw Workflow.CreateContinueAsNewException((VehicleProcessingWorkflow wf) => wf.RunAsync(next));
            }
        }
    }

    // ------------------------------------------------------------------ rate limits

    /// <summary>Operating day of an instant (time zone + start hour), e.g. 03:00 with a 04:00 start = previous day.</summary>
    public static DateOnly OperatingDayOf(DateTime utc, TimeZoneInfo zone, int startHour) =>
        // SpecifyKind: ConvertTimeFromUtc rejects a DateTime whose Kind is Local. Shifting by -startHour maps
        // e.g. 03:00 to the previous calendar day when the operating day starts at 04:00.
        DateOnly.FromDateTime(TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(utc, DateTimeKind.Utc), zone).AddHours(-startHour));

    private DateOnly OperatingDay(DateTime utc) => OperatingDayOf(utc, _timeZone, _settings.OperatingDayStartHour);

    // Evaluated at "now": after the operating day ends, yesterday switches from the 1 min to the 1 h interval.
    private TimeSpan MinInterval(DateOnly day, DateTime now) =>
        day == OperatingDay(now) ? _settings.RealtimeMinInterval : _settings.DailyMinInterval;

    // DateTime.MinValue = never processed recently = eligible immediately.
    private DateTime EligibleAt(DateOnly day, DateTime now) =>
        _lastStarts.TryGetValue(day, out var last) ? last + MinInterval(day, now) : DateTime.MinValue;

    /// <summary>A start older than the longest interval no longer limits anything: forget it (bounded state).</summary>
    private void PruneLastStarts(DateTime now)
    {
        var horizon = now - (_settings.DailyMinInterval > _settings.RealtimeMinInterval ? _settings.DailyMinInterval : _settings.RealtimeMinInterval);
        // ToList(): materialize before removing (a dictionary cannot be modified while enumerated).
        foreach (var day in _lastStarts.Where(kv => kv.Value < horizon && kv.Key != _inProgress).Select(kv => kv.Key).ToList())
        {
            _lastStarts.Remove(day);
        }
    }

    private async Task ProcessDayAsync(PendingDay day)
    {
        // The retry tuning travels with the input so that the jitter computed by the activity matches the policy.
        var input = new VehicleDayInput(_vehicleId, day.Day, day.RequestIds,
            _settings.RetryInitialInterval, _settings.RetryMaxInterval, _settings.RetryBackoffCoefficient);
        try
        {
            await Workflow.ExecuteActivityAsync((VehicleDayActivities a) => a.ProcessDayAsync(input), DayActivityOptions(day.Day));
            _succeeded++;
        }
        // The retries happen inside ExecuteActivityAsync (server side): we only get here once they are over.
        // A cancellation (workflow cancelled) is NOT caught: it must propagate and cancel the workflow.
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

            // Persisted by an activity (a workflow never does I/O itself); retried without limit, see RecordActivityOptions.
            var failure = new VehicleDayFailure(_vehicleId, day.Day, day.RequestIds, type, message, attempts);
            await Workflow.ExecuteActivityAsync((VehicleDayActivities a) => a.RecordFailureAsync(failure), RecordActivityOptions());
        }
    }

    private ActivityOptions DayActivityOptions(DateOnly day) => new()
    {
        // Current operating day: dedicated activity queue (its own worker slots), so real time processing is not
        // queued behind the activities of a mass reprocessing. null = the workflow's own task queue.
        TaskQueue = day == OperatingDay(Workflow.UtcNow) ? _settings.RealtimeTaskQueue : null,
        // Maximum duration of ONE attempt.
        StartToCloseTimeout = _settings.DayTimeout,
        // A worker that dies stops heartbeating: the attempt is retried elsewhere after this delay,
        // instead of waiting for the whole StartToCloseTimeout.
        HeartbeatTimeout = _settings.HeartbeatTimeout,
        RetryPolicy = new RetryPolicy
        {
            // The delays below apply to timeouts; for failures thrown by the activity, the activity sets
            // its own delay (exponential + jitter) through nextRetryDelay.
            InitialInterval = _settings.RetryInitialInterval,
            BackoffCoefficient = (float)_settings.RetryBackoffCoefficient,
            MaximumInterval = _settings.RetryMaxInterval,
            MaximumAttempts = 1 + _settings.MaxRetries,   // attempts = first execution + retries
            // Defense in depth: the activity already marks these errors as non retryable.
            NonRetryableErrorTypes = [VehicleErrorTypes.Business, VehicleErrorTypes.Application],
        },
        Summary = $"{_vehicleId} {_inProgress:yyyy-MM-dd}",   // label shown in the Temporal UI
    };

    /// <summary>Bookkeeping: retried until it succeeds (a failure must never be lost).</summary>
    private static ActivityOptions RecordActivityOptions() => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(30),
        // MaximumAttempts = 0 means unlimited (bounded by the backoff: at most one attempt per minute).
        RetryPolicy = new RetryPolicy { InitialInterval = TimeSpan.FromSeconds(1), MaximumInterval = TimeSpan.FromMinutes(1), MaximumAttempts = 0 },
    };

    /// <summary>
    /// Error type, message and number of attempts of a failed day. ActivityFailureException wraps the real cause
    /// (InnerException); RetryState tells why the server stopped retrying.
    /// </summary>
    private (string Type, string Message, int Attempts) Describe(ActivityFailureException e)
    {
        var exhausted = e.RetryState == RetryState.MaximumAttemptsReached;   // transient failure, retries used up
        return e.InnerException switch
        {
            ApplicationFailureException app => (app.ErrorType ?? VehicleErrorTypes.Application, app.Message, exhausted ? 1 + _settings.MaxRetries : AttemptFrom(app)),
            TimeoutFailureException timeout => ($"Timeout.{timeout.TimeoutType}", timeout.Message, exhausted ? 1 + _settings.MaxRetries : 1),
            { } inner => (inner.GetType().Name, inner.Message, exhausted ? 1 + _settings.MaxRetries : 1),
            null => ("ActivityFailure", e.Message, 1),
        };
    }

    /// <summary>Attempt number sent by the activity in the failure details (see VehicleDayActivities).</summary>
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

    // Throwing here rejects the update: the caller gets WorkflowUpdateFailedException with the message.
    // A validator must not modify the workflow state (it is read-only by contract).
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
        var days = request.To.DayNumber - request.From.DayNumber + 1;   // inclusive range
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
        // A signal cannot be rejected (it is already in the history when the handler runs): the same
        // validation is applied here, and an invalid request is only logged.
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

    /// <summary>
    /// Real time event (file received). Current operating day: high priority, at most once per minute.
    /// Late file (another day): processed as a daily request (at most once per hour for that day).
    /// </summary>
    [WorkflowSignal("FileReceived")]
    public Task FileReceivedAsync(FileReceivedEvent e)
    {
        var current = e.Day == OperatingDay(Workflow.UtcNow);
        // The event id becomes the request id: the same file notified twice is merged once (dedup of Accept).
        var request = new ProcessingRequest(e.EventId, e.VehicleId, e.Day, e.Day,
            current ? _settings.RealtimePriority : _settings.LateFilePriority, current ? "file" : "late-file");
        return SubmitSignalAsync(request);
    }

    // Queries are read-only and not recorded in the history: they can be called at any time, even very often.
    [WorkflowQuery("Status")]
    public VehicleStatus Status => new(
        _vehicleId,
        OperatingDay(Workflow.UtcNow),
        _inProgress,
        _pending.Count,
        Ordered(_pending.Values).Take(20)
            .Select(d => new PendingDayView(d.Day, d.Priority, EligibleAt(d.Day, Workflow.UtcNow), d.Day == OperatingDay(Workflow.UtcNow), d.RequestIds))
            .ToList(),
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
        if (_recentRequestIds.Count > MaxRecentRequestIds)   // forget the oldest id (bounded state)
        {
            _seenRequestIds.Remove(_recentRequestIds[0]);
            _recentRequestIds.RemoveAt(0);
        }

        _changes++;   // wakes up the main loop if it is sleeping until a day becomes eligible
        int added = 0, merged = 0, requeued = 0;
        for (var day = request.From; day <= request.To; day = day.AddDays(1))
        {
            if (day == _inProgress)
            {
                // Being processed right now: not added to _pending (it would be picked in parallel);
                // flagged to be re-queued when the current processing ends.
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
            // Records are immutable: `with` creates a copy with the new priority (the list is shared, updated above).
            _pending[day] = existing with { Priority = Math.Max(existing.Priority, priority) };
            return true;
        }
        var list = new List<string>();
        foreach (var id in requestIds)
        {
            AddRequestId(list, id);
        }
        _pending[day] = new PendingDay(day, priority, Workflow.UtcNow, list);   // Workflow.UtcNow: deterministic
        return false;
    }

    // Keeps at most MaxRequestIdsPerDay ids per day: enough for diagnostics, bounded for a day requested very often.
    private static void AddRequestId(List<string> ids, string id)
    {
        if (!ids.Contains(id) && ids.Count < MaxRequestIdsPerDay)
        {
            ids.Add(id);
        }
    }

    // ------------------------------------------------------------------ ordering & state

    /// <summary>Deterministic order: priority, then date (per settings), then age of the request.</summary>
    // The complete sort key (priority, date, time of the request) makes the choice independent of the Dictionary's
    // enumeration order: the same history always gives the same order on replay.
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
        foreach (var start in state.LastStarts)
        {
            _lastStarts[start.Day] = start.StartedAt;
        }
        _succeeded = state.Succeeded;
        _failed = state.Failed;
        _runs = state.Runs + 1;   // this run is the next one
    }

    // State passed to the next run by continue-as-new. Sorted so that the serialized input does not depend
    // on dictionary order.
    private VehicleWorkflowState Snapshot() => new()
    {
        Pending = _pending.Values.OrderBy(d => d.Day).ToList(),
        RecentRequestIds = _recentRequestIds.ToList(),
        RecentFailures = _recentFailures.ToList(),
        LastStarts = _lastStarts.OrderBy(kv => kv.Key).Select(kv => new DayStart(kv.Key, kv.Value)).ToList(),
        Succeeded = _succeeded,
        Failed = _failed,
        Runs = _runs,
    };
}
