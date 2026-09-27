using Microsoft.Extensions.Logging;
using Temporalio.Activities;
using Temporalio.Exceptions;

namespace TemporalPoc.Core.Vehicles;

/// <summary>
/// Activities of the vehicle workflow. Failures are translated for Temporal:
///  - transient (network, timeout, DB unavailable...): retryable, with an exponential delay + jitter;
///  - business (<see cref="VehicleBusinessException"/>): non retryable, the day is marked failed at once;
///  - anything else (bug, unexpected state): non retryable as well, retrying would fail the same way.
/// </summary>
public sealed class VehicleDayActivities(IVehicleDayProcessor processor, IVehicleDayRunStore store, ILogger<VehicleDayActivities> logger)
{
    [Activity("vehicle.process-day")]
    public async Task<DayOutcome> ProcessDayAsync(VehicleDayInput input)
    {
        var ctx = ActivityExecutionContext.Current;
        var attempt = ctx.Info.Attempt;   // 1 = first execution, 2 = first retry...
        // Heartbeat: tells Temporal the worker is alive. For a long processing, call it regularly (the processor
        // can do so): without heartbeat within HeartbeatTimeout, the attempt is considered lost and retried.
        ctx.Heartbeat();
        try
        {
            var count = await processor.ProcessAsync(input.VehicleId, input.Day, input.RequestIds, attempt, ctx.CancellationToken);
            return new DayOutcome(input.VehicleId, input.Day, count, attempt);
        }
        // Only OUR cancellation token: an OperationCanceledException raised by an HTTP client timeout, for
        // instance, is a transient failure and goes to the branches below.
        catch (OperationCanceledException) when (ctx.CancellationToken.IsCancellationRequested)
        {
            throw;   // cancellation (workflow cancelled, worker shutting down): let Temporal handle it
        }
        catch (VehicleBusinessException e)
        {
            // nonRetryable: the server stops at once. details: the attempt number, read back by the workflow.
            throw new ApplicationFailureException(e.Message, errorType: VehicleErrorTypes.Business, nonRetryable: true, details: [attempt]);
        }
        catch (Exception e) when (FailureClassifier.IsTransient(e))
        {
            // Random is allowed here: activities are normal code, only workflows must be deterministic.
            var delay = RetryBackoff.Next(attempt, input.RetryInitialInterval, input.RetryMaxInterval, input.RetryBackoffCoefficient, Random.Shared);
            logger.LogWarning("Transient failure on {Vehicle} {Day} (attempt {Attempt}), next try in {Delay}: {Message}",
                input.VehicleId, input.Day, attempt, delay, e.Message);
            // nextRetryDelay replaces the RetryPolicy interval for THIS retry (that is how the jitter is applied);
            // MaximumAttempts of the policy still applies: after the 6th attempt the server stops.
            throw new ApplicationFailureException(
                $"{e.GetType().Name}: {e.Message}", errorType: VehicleErrorTypes.Transient, nonRetryable: false, details: [attempt], nextRetryDelay: delay);
        }
        catch (Exception e)
        {
            // Unknown error (bug, unexpected data): retrying would fail the same way, so it is not retried.
            logger.LogError(e, "Non transient failure on {Vehicle} {Day}", input.VehicleId, input.Day);
            throw new ApplicationFailureException($"{e.GetType().Name}: {e.Message}", errorType: VehicleErrorTypes.Application, nonRetryable: true, details: [attempt]);
        }
    }

    /// <summary>Records the final failure of a day. Infrastructure activity: retried until it succeeds.</summary>
    [Activity("vehicle.record-failure")]
    public Task RecordFailureAsync(VehicleDayFailure failure) =>
        store.RecordFailureAsync(failure, ActivityExecutionContext.Current.CancellationToken);
}
