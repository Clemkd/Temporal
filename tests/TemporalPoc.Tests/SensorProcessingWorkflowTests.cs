using System.Collections.Concurrent;
using Temporalio.Activities;
using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using Temporalio.Exceptions;
using Temporalio.Worker;
using TemporalPoc.Core.Pipelines;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Tests;

[Collection(WorkflowCollection.Name)]
public class SensorProcessingWorkflowTests(WorkflowEnvironmentFixture fixture)
{
    private sealed class FakeActivities(long totalRows)
    {
        public ConcurrentQueue<(string Step, int Batch)> Calls { get; } = new();
        public ConcurrentQueue<JobUpdate> Records { get; } = new();
        public TaskCompletionSource FirstBatch { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int DelayMs { get; set; }

        [Activity("pipeline.get")]
        public PipelineDefinition GetPipeline(GetPipelineRequest r) => DefaultPipelines.SensorProcessing;

        [Activity("processing.fetch-batch")]
        public async Task<BatchRef> Fetch(FetchBatchRequest r)
        {
            await Task.Delay(DelayMs);
            var first = r.AfterId + 1;
            var last = Math.Min(totalRows, r.AfterId + r.BatchSize);
            return last < first ? new BatchRef(r.BatchNumber, r.AfterId, r.AfterId, 0) : new BatchRef(r.BatchNumber, first, last, (int)(last - first + 1));
        }

        private BatchStepResult Step(string name, BatchContext c, Dictionary<string, long>? categories = null)
        {
            Calls.Enqueue((name, c.Batch.BatchNumber));
            FirstBatch.TrySetResult();
            return new BatchStepResult(c.Batch.Count, categories);
        }

        [Activity("processing.categorize")]
        public BatchStepResult Categorize(BatchContext c) => Step("categorize", c, new() { ["NORMAL"] = c.Batch.Count });

        [Activity("processing.update-measurements")] public BatchStepResult Update(BatchContext c) => Step("update", c);
        [Activity("processing.insert-results")] public BatchStepResult Insert(BatchContext c) => Step("insert", c);
        [Activity("processing.cleanup")] public BatchStepResult Cleanup(BatchContext c) => Step("cleanup", c);

        [Activity("processing.record-job")]
        public void Record(JobUpdate u) => Records.Enqueue(u);
    }

    private static TemporalWorker Worker(WorkflowEnvironmentFixture fixture, string queue, FakeActivities fakes) =>
        new(fixture.Env.Client, new TemporalWorkerOptions(queue).AddWorkflow<SensorProcessingWorkflow>().AddAllActivities(fakes));

    [Fact]
    public async Task Processes_every_batch_across_continue_as_new()
    {
        var fakes = new FakeActivities(totalRows: 1050);
        var queue = $"test-{Guid.NewGuid()}";
        using var worker = Worker(fixture, queue, fakes);

        var progress = await worker.ExecuteAsync(() => fixture.Env.Client.ExecuteWorkflowAsync(
            (SensorProcessingWorkflow wf) => wf.RunAsync(new ProcessingJobInput
            {
                JobId = "job-1", SensorType = "temperature", BatchSize = 100, BatchesPerRun = 3,
            }),
            new WorkflowOptions($"processing-test-{Guid.NewGuid()}", queue)));

        Assert.Equal(11, progress.Batches);
        Assert.Equal(1050, progress.Processed);
        Assert.Equal(4, progress.Runs); // 3 + 3 + 3 + 2 batches
        Assert.Equal(1050, progress.Categories["NORMAL"]);
        // each batch went through the 4 configured steps, in order
        Assert.Equal(44, fakes.Calls.Count);
        Assert.Equal(["categorize", "update", "insert", "cleanup"], fakes.Calls.Where(c => c.Batch == 5).Select(c => c.Step));
        Assert.Equal("Completed", fakes.Records.Last().Status);
    }

    [Fact]
    public async Task Can_be_paused_resumed_and_reconfigured_while_running()
    {
        var fakes = new FakeActivities(totalRows: 5000) { DelayMs = 50 };
        var queue = $"test-{Guid.NewGuid()}";
        using var worker = Worker(fixture, queue, fakes);

        await worker.ExecuteAsync(async () =>
        {
            var handle = await fixture.Env.Client.StartWorkflowAsync(
                (SensorProcessingWorkflow wf) => wf.RunAsync(new ProcessingJobInput { JobId = "job-2", SensorType = "co2", BatchSize = 100, BatchesPerRun = 100 }),
                new WorkflowOptions($"processing-test-{Guid.NewGuid()}", queue));

            await fakes.FirstBatch.Task;
            await handle.SignalAsync(wf => wf.PauseAsync());

            // invalid update rejected by the validator, nothing recorded in history
            await Assert.ThrowsAsync<WorkflowUpdateFailedException>(() => handle.ExecuteUpdateAsync(wf => wf.SetBatchSizeAsync(0)));
            Assert.Equal(100, await handle.ExecuteUpdateAsync(wf => wf.SetBatchSizeAsync(1000)));

            await Task.Delay(500);
            var paused = await handle.QueryAsync(wf => wf.Status);
            Assert.True(paused.Paused);
            Assert.Equal(1000, paused.BatchSize);
            var processedWhilePaused = paused.Progress.Processed;
            await Task.Delay(500);
            Assert.Equal(processedWhilePaused, (await handle.QueryAsync(wf => wf.Status)).Progress.Processed);

            await handle.SignalAsync(wf => wf.ResumeAsync());
            var result = await handle.GetResultAsync();
            Assert.Equal(5000, result.Processed);
            Assert.True(result.Batches < 50, "batch size change applied");
        });
    }

    [Fact]
    public async Task Cancellation_records_the_job_as_failed()
    {
        var fakes = new FakeActivities(totalRows: 100_000) { DelayMs = 100 };
        var queue = $"test-{Guid.NewGuid()}";
        using var worker = Worker(fixture, queue, fakes);

        await worker.ExecuteAsync(async () =>
        {
            var handle = await fixture.Env.Client.StartWorkflowAsync(
                (SensorProcessingWorkflow wf) => wf.RunAsync(new ProcessingJobInput { JobId = "job-3", SensorType = "co2", BatchSize = 10 }),
                new WorkflowOptions($"processing-test-{Guid.NewGuid()}", queue));
            await fakes.FirstBatch.Task;
            await handle.CancelAsync();

            var e = await Assert.ThrowsAsync<WorkflowFailedException>(() => handle.GetResultAsync());
            Assert.True(TemporalException.IsCanceledException(e.InnerException!));
            Assert.Equal(WorkflowExecutionStatus.Canceled, (await handle.DescribeAsync()).Status);
            Assert.Equal("Failed", fakes.Records.Last().Status);
        });
    }
}
