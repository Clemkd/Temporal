using Temporalio.Common;
using Temporalio.Exceptions;
using Temporalio.Workflows;
using TemporalPoc.Core.Pipelines;

namespace TemporalPoc.Core.Workflows;

internal static class ActivityOptionsFactory
{
    /// <summary>
    /// Options for "infrastructure" activities (claim, finalize, bookkeeping): retried forever with
    /// capped backoff, and not bound to the workflow cancellation so they still run during a cancel.
    /// </summary>
    public static ActivityOptions Infrastructure(int timeoutSeconds = 60) => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(timeoutSeconds),
        HeartbeatTimeout = TimeSpan.FromSeconds(Math.Min(timeoutSeconds, 30)),
        RetryPolicy = new RetryPolicy
        {
            InitialInterval = TimeSpan.FromSeconds(1),
            BackoffCoefficient = 2,
            MaximumInterval = TimeSpan.FromSeconds(30),
            MaximumAttempts = 0,
            NonRetryableErrorTypes = ["PipelineNotFound"],
        },
        CancellationToken = CancellationToken.None,
    };

    /// <summary>Options of a configurable step, built from its runtime definition.</summary>
    public static ActivityOptions ForStep(StepDefinition step) => new()
    {
        StartToCloseTimeout = TimeSpan.FromSeconds(step.TimeoutSeconds),
        HeartbeatTimeout = step.HeartbeatTimeoutSeconds > 0 ? TimeSpan.FromSeconds(step.HeartbeatTimeoutSeconds) : null,
        RetryPolicy = new RetryPolicy
        {
            InitialInterval = TimeSpan.FromSeconds(Math.Max(1, step.InitialRetrySeconds)),
            BackoffCoefficient = 2,
            MaximumInterval = TimeSpan.FromSeconds(Math.Max(step.InitialRetrySeconds, step.MaxRetrySeconds)),
            MaximumAttempts = step.MaxAttempts,
        },
        Summary = step.Activity,
    };

    /// <summary>Extracts a (type, message) pair from the failure of an activity after its retries.</summary>
    public static (string Type, string Message) Describe(ActivityFailureException failure)
    {
        return failure.InnerException switch
        {
            ApplicationFailureException app => (app.ErrorType ?? "ApplicationFailure", app.Message),
            TimeoutFailureException timeout => ($"Timeout.{timeout.TimeoutType}", timeout.Message),
            CanceledFailureException canceled => ("Cancelled", canceled.Message),
            { } inner => (inner.GetType().Name, inner.Message),
            null => ("ActivityFailure", failure.Message),
        };
    }
}
