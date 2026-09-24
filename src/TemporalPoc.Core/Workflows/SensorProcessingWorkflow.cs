using Temporalio.Exceptions;
using Temporalio.Workflows;
using TemporalPoc.Core.Activities;
using TemporalPoc.Core.Data;
using TemporalPoc.Core.Pipelines;

namespace TemporalPoc.Core.Workflows;

/// <summary>
/// Long running batch job over stored measurements of one sensor type:
/// for each batch (fetched by sensor type with a keyset cursor) it runs the configurable steps
/// (categorize, update measurements, insert results, ...).
///
///  - Pause / resume (signals), batch size change (validated update), pipeline reload (signal): all at runtime.
///  - Progress is queryable and persisted in the processing_jobs table.
///  - Continue-as-new every N batches: the job can run for days with a bounded history.
///  - A crash of a worker resumes on the current batch; every step is idempotent per (job, batch).
/// </summary>
[Workflow("SensorProcessing")]
public sealed class SensorProcessingWorkflow
{
    private ProcessingJobInput _input = null!;
    private ProcessingProgress _progress = new();
    private PipelineDefinition? _pipeline;
    private bool _paused;
    private bool _reloadRequested;
    private int _batchSize;
    private string _phase = "Starting";
    private string? _currentStep;

    [WorkflowRun]
    public async Task<ProcessingProgress> RunAsync(ProcessingJobInput input)
    {
        // Empty JobId (e.g. started by a Temporal Schedule): the workflow id, unique per scheduled run, is the job id.
        if (string.IsNullOrEmpty(input.JobId))
        {
            input = input with { JobId = Workflow.Info.WorkflowId };
        }
        _input = input;
        _batchSize = input.BatchSize;
        _paused = input.Paused;
        _progress = input.Progress ?? new ProcessingProgress { StartedAt = Workflow.UtcNow };
        _pipeline = input.ResolvedPipeline ?? await LoadPipelineAsync(input.PipelineVersion);

        if (input.Progress is null)
        {
            await RecordAsync(JobStatus.Running);
        }

        try
        {
            var batchesThisRun = 0;
            while (true)
            {
                _phase = _paused ? "Paused" : "Running";
                await Workflow.WaitConditionAsync(() => !_paused);
                _phase = "Running";

                if (_reloadRequested)
                {
                    _reloadRequested = false;
                    _pipeline = await LoadPipelineAsync(null);
                    Workflow.Logger.LogPipelineReloaded(input.JobId, _pipeline.Version);
                }

                if (input.MaxBatches > 0 && _progress.Batches >= input.MaxBatches)
                {
                    break;
                }

                _currentStep = "fetch";
                var batch = await Workflow.ExecuteActivityAsync(
                    (ProcessingActivities a) => a.FetchBatchAsync(new FetchBatchRequest(
                        input.JobId, input.SensorType, _progress.Cursor, _batchSize, _progress.Batches + 1, input.OnlyUncategorized)),
                    ActivityOptionsFactory.Infrastructure());
                if (batch.Count == 0)
                {
                    break;
                }

                var batchFailed = await RunBatchStepsAsync(batch);

                _progress = _progress with
                {
                    Cursor = batch.LastId,
                    Batches = _progress.Batches + 1,
                    Processed = _progress.Processed + batch.Count,
                    FailedBatches = _progress.FailedBatches + (batchFailed ? 1 : 0),
                };
                batchesThisRun++;

                if (batchesThisRun % 5 == 0)
                {
                    await RecordAsync(JobStatus.Running);
                }

                if (batchesThisRun >= input.BatchesPerRun || Workflow.ContinueAsNewSuggested)
                {
                    await RecordAsync(JobStatus.Running);
                    await Workflow.WaitConditionAsync(() => Workflow.AllHandlersFinished);
                    var next = input with
                    {
                        BatchSize = _batchSize,
                        Paused = _paused,
                        Progress = _progress with { Runs = _progress.Runs + 1 },
                        ResolvedPipeline = _reloadRequested ? null : _pipeline,
                    };
                    throw Workflow.CreateContinueAsNewException((SensorProcessingWorkflow wf) => wf.RunAsync(next));
                }
            }
        }
        catch (Exception e) when (e is ActivityFailureException or ApplicationFailureException || TemporalException.IsCanceledException(e))
        {
            _phase = "Failed";
            await RecordAsync(JobStatus.Failed, e.InnerException?.Message ?? e.Message);
            throw;
        }

        _phase = "Completed";
        _currentStep = null;
        await RecordAsync(JobStatus.Completed);
        return _progress;
    }

    /// <summary>Runs the configured steps on one batch. Returns true when a "continue on error" step failed.</summary>
    private async Task<bool> RunBatchStepsAsync(BatchRef batch)
    {
        var failed = false;
        foreach (var step in _pipeline!.EnabledSteps)
        {
            _currentStep = step.Activity;
            try
            {
                var result = await Workflow.ExecuteActivityAsync<BatchStepResult>(
                    step.Activity,
                    [new BatchContext(_input.JobId, _input.SensorType, batch, _input.OnlyUncategorized, step.Parameters)],
                    ActivityOptionsFactory.ForStep(step));

                if (result.Categories is { } categories)
                {
                    var merged = new Dictionary<string, long>(_progress.Categories);
                    foreach (var (category, count) in categories)
                    {
                        merged[category] = merged.GetValueOrDefault(category) + count;
                    }
                    _progress = _progress with { Categories = merged };
                }
            }
            catch (ActivityFailureException e) when (step.ContinueOnError && !TemporalException.IsCanceledException(e))
            {
                var (type, message) = ActivityOptionsFactory.Describe(e);
                Workflow.Logger.LogBatchStepSkipped(_input.JobId, batch.BatchNumber, step.Activity, $"{type}: {message}");
                failed = true;
            }
            catch (ActivityFailureException e) when (!TemporalException.IsCanceledException(e))
            {
                var (type, message) = ActivityOptionsFactory.Describe(e);
                throw new ApplicationFailureException(
                    $"Step {step.Activity} failed on batch {batch.BatchNumber}: {type}: {message}", "ProcessingStepFailed", nonRetryable: true);
            }
        }
        return failed;
    }

    private Task<PipelineDefinition> LoadPipelineAsync(int? version) =>
        Workflow.ExecuteActivityAsync(
            (PipelineActivities a) => a.GetAsync(new GetPipelineRequest(_input.PipelineName, version)),
            ActivityOptionsFactory.Infrastructure());

    private Task RecordAsync(string status, string? error = null)
    {
        var update = new JobUpdate(
            _input.JobId, _input.SensorType, status, _pipeline?.Name ?? _input.PipelineName, _pipeline?.Version ?? 0,
            _progress.Processed, _progress.Batches, _progress.FailedBatches, error);
        return Workflow.ExecuteActivityAsync((ProcessingActivities a) => a.RecordJobAsync(update), ActivityOptionsFactory.Infrastructure());
    }

    [WorkflowSignal]
    public Task PauseAsync()
    {
        _paused = true;
        return Task.CompletedTask;
    }

    [WorkflowSignal]
    public Task ResumeAsync()
    {
        _paused = false;
        return Task.CompletedTask;
    }

    /// <summary>Reloads the latest version of the pipeline before the next batch.</summary>
    [WorkflowSignal]
    public Task ReloadPipelineAsync()
    {
        _reloadRequested = true;
        return Task.CompletedTask;
    }

    [WorkflowUpdate]
    public Task<int> SetBatchSizeAsync(int batchSize)
    {
        var previous = _batchSize;
        _batchSize = batchSize;
        return Task.FromResult(previous);
    }

    [WorkflowUpdateValidator(nameof(SetBatchSizeAsync))]
    public void ValidateBatchSize(int batchSize)
    {
        if (batchSize is < 1 or > 10_000)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be in [1, 10000]");
        }
    }

    [WorkflowQuery]
    public ProcessingStatus Status => new(
        _input.JobId,
        _input.SensorType,
        _phase,
        _paused,
        _batchSize,
        _currentStep,
        _progress,
        _pipeline?.Name ?? _input.PipelineName,
        _pipeline?.Version ?? 0,
        _pipeline?.EnabledSteps.Select(s => s.Activity).ToList() ?? []);
}
