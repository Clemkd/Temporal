using Temporalio.Exceptions;
using Temporalio.Workflows;
using TemporalPoc.Core.Activities;
using TemporalPoc.Core.Pipelines;

namespace TemporalPoc.Core.Workflows;

/// <summary>
/// Integration of ONE file: claim (S3 move incoming -> processing), then the configurable steps of the
/// pipeline (fetch, validate, convert, store...), then archive (processed/) or quarantine (invalid/).
///
/// Guarantees:
///  - one workflow per file (workflow id derived from the key);
///  - every file ends up in processed/ or invalid/ (finalization is retried forever and survives cancellation);
///  - a worker crash at any point resumes exactly where it stopped (event sourced state + idempotent activities).
/// </summary>
[Workflow("FileIngestion")]
public sealed class FileIngestionWorkflow
{
    private string _relativeKey = "";
    private string _phase = "Starting";
    private string? _currentStep;
    private int _pipelineVersion;
    private FileContext? _context;
    private string? _error;

    [WorkflowRun]
    public async Task<FileIngestionResult> RunAsync(FileIngestionInput input)
    {
        _relativeKey = input.RelativeKey;

        _phase = "Claiming";
        var context = await Workflow.ExecuteActivityAsync(
            (IngestionActivities a) => a.ClaimAsync(input.RelativeKey),
            ActivityOptionsFactory.Infrastructure());
        _context = context;

        if (context.AlreadyFinalized)
        {
            _phase = "Skipped";
            return new FileIngestionResult(input.RelativeKey, $"AlreadyFinalized:{context.FinalizedAs}", context.StoredCount, null, null);
        }

        PipelineDefinition? pipeline = null;
        string? failedStep = null, errorType = null, error = null;
        try
        {
            _phase = "LoadingPipeline";
            _currentStep = "pipeline.get";
            pipeline = input.StepsOverride is { Count: > 0 }
                ? new PipelineDefinition { Name = input.PipelineName + "+override", Version = 0, Steps = input.StepsOverride }
                : await Workflow.ExecuteActivityAsync(
                    (PipelineActivities a) => a.GetAsync(new GetPipelineRequest(input.PipelineName, input.PipelineVersion)),
                    ActivityOptionsFactory.Infrastructure());
            _pipelineVersion = pipeline.Version;

            _phase = "Running";
            foreach (var step in pipeline.EnabledSteps)
            {
                _currentStep = step.Activity;
                context = await Workflow.ExecuteActivityAsync<FileContext>(
                    step.Activity,
                    [new StepInvocation(context, step.Parameters)],
                    ActivityOptionsFactory.ForStep(step));
                context = context with { CompletedSteps = [.. context.CompletedSteps, step.Activity] };
                _context = context;
            }
            _currentStep = null;
        }
        catch (ActivityFailureException e)
        {
            // Retries exhausted, non retryable error (invalid content, unknown pipeline) or workflow cancelled.
            failedStep = _currentStep;
            (errorType, error) = ActivityOptionsFactory.Describe(e);
            _error = $"{errorType}: {error}";
            Workflow.Logger.LogFileFailed(input.RelativeKey, failedStep, _error);
        }

        _phase = "Finalizing";
        var request = new FinalizeRequest(context, pipeline?.Name ?? input.PipelineName, pipeline?.Version ?? 0, failedStep, errorType, error);
        if (failedStep is null)
        {
            await Workflow.ExecuteActivityAsync((IngestionActivities a) => a.ArchiveAsync(request), ActivityOptionsFactory.Infrastructure());
            _phase = "Stored";
            return new FileIngestionResult(input.RelativeKey, "Stored", context.StoredCount, null, null);
        }

        await Workflow.ExecuteActivityAsync((IngestionActivities a) => a.QuarantineAsync(request), ActivityOptionsFactory.Infrastructure());
        _phase = "Invalid";
        return new FileIngestionResult(input.RelativeKey, "Invalid", 0, failedStep, _error);
    }

    [WorkflowQuery]
    public FileIngestionStatus Status => new(
        _relativeKey, _phase, _currentStep, _pipelineVersion, _context?.CompletedSteps ?? [], _error);
}

internal static partial class WorkflowLog
{
    [Microsoft.Extensions.Logging.LoggerMessage(Level = Microsoft.Extensions.Logging.LogLevel.Warning,
        Message = "File {Key} failed at step {Step}: {Error}")]
    public static partial void LogFileFailed(this Microsoft.Extensions.Logging.ILogger logger, string key, string? step, string error);

    [Microsoft.Extensions.Logging.LoggerMessage(Level = Microsoft.Extensions.Logging.LogLevel.Information,
        Message = "Job {JobId} reloaded pipeline, now v{Version}")]
    public static partial void LogPipelineReloaded(this Microsoft.Extensions.Logging.ILogger logger, string jobId, int version);

    [Microsoft.Extensions.Logging.LoggerMessage(Level = Microsoft.Extensions.Logging.LogLevel.Warning,
        Message = "Job {JobId} batch {Batch}: step {Step} failed and was skipped: {Error}")]
    public static partial void LogBatchStepSkipped(this Microsoft.Extensions.Logging.ILogger logger, string jobId, int batch, string step, string error);
}
