using System.Collections.Concurrent;
using Temporalio.Activities;
using Temporalio.Client;
using Temporalio.Exceptions;
using Temporalio.Worker;
using TemporalPoc.Core.Pipelines;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Tests;

[Collection(WorkflowCollection.Name)]
public class FileIngestionWorkflowTests(WorkflowEnvironmentFixture fixture)
{
    /// <summary>Fake activities registered under the real activity names.</summary>
    private sealed class FakeActivities
    {
        public ConcurrentQueue<string> Calls { get; } = new();
        public ConcurrentDictionary<string, int> Attempts { get; } = new();
        public Func<string, int, Exception?> FailWith { get; set; } = (_, _) => null;
        public FinalizeRequest? Finalized { get; private set; }
        public string? FinalizedAs { get; private set; }
        public PipelineDefinition Pipeline { get; set; } = DefaultPipelines.FileIngestion;

        private FileContext Step(string name, StepInvocation step)
        {
            var attempt = Attempts.AddOrUpdate(name, 1, (_, a) => a + 1);
            Calls.Enqueue(name);
            if (FailWith(name, attempt) is { } e)
            {
                throw e;
            }
            return name == "ingest.store" ? step.Context with { StoredCount = 42 } : step.Context;
        }

        [Activity("ingest.claim")]
        public FileContext Claim(string key) => new() { RelativeKey = key, WorkingKey = "processing/" + key };

        [Activity("pipeline.get")]
        public PipelineDefinition GetPipeline(GetPipelineRequest request) => request.Name == "missing"
            ? throw new ApplicationFailureException("Pipeline 'missing' not found", "PipelineNotFound", nonRetryable: true)
            : Pipeline;

        public TaskCompletionSource SlowStepStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        [Activity("ingest.fetch")] public FileContext Fetch(StepInvocation s) => Step("ingest.fetch", s);
        [Activity("ingest.validate")] public FileContext Validate(StepInvocation s) => Step("ingest.validate", s);
        [Activity("ingest.convert")] public FileContext Convert(StepInvocation s) => Step("ingest.convert", s);
        [Activity("ingest.store")] public FileContext Store(StepInvocation s) => Step("ingest.store", s);
        [Activity("ingest.delay")]
        public async Task<FileContext> Delay(StepInvocation s)
        {
            Step("ingest.delay", s);
            SlowStepStarted.TrySetResult();
            var ctx = ActivityExecutionContext.Current;
            while (s.Parameters.ContainsKey("forever"))
            {
                ctx.Heartbeat();
                await Task.Delay(100, ctx.CancellationToken);
            }
            return s.Context;
        }

        [Activity("ingest.archive")]
        public void Archive(FinalizeRequest r) { Finalized = r; FinalizedAs = "processed"; }

        [Activity("ingest.quarantine")]
        public void Quarantine(FinalizeRequest r) { Finalized = r; FinalizedAs = "invalid"; }
    }

    private async Task<FileIngestionResult> RunAsync(FakeActivities fakes, FileIngestionInput? input = null)
    {
        var queue = $"test-{Guid.NewGuid()}";
        using var worker = new TemporalWorker(fixture.Env.Client,
            new TemporalWorkerOptions(queue).AddWorkflow<FileIngestionWorkflow>().AddAllActivities(fakes));
        return await worker.ExecuteAsync(() => fixture.Env.Client.ExecuteWorkflowAsync(
            (FileIngestionWorkflow wf) => wf.RunAsync(input ?? new FileIngestionInput("a/file.csv")),
            new WorkflowOptions($"ingest-test-{Guid.NewGuid()}", queue)));
    }

    private static PipelineDefinition FastRetries(PipelineDefinition p) => p with
    {
        Steps = p.Steps.Select(s => s with { InitialRetrySeconds = 1, MaxRetrySeconds = 1 }).ToList(),
    };

    [Fact]
    public async Task Runs_all_steps_in_order_then_archives()
    {
        var fakes = new FakeActivities();
        var result = await RunAsync(fakes);

        Assert.Equal("Stored", result.Status);
        Assert.Equal(42, result.StoredCount);
        Assert.Equal(["ingest.fetch", "ingest.validate", "ingest.convert", "ingest.store"], fakes.Calls);
        Assert.Equal("processed", fakes.FinalizedAs);
    }

    [Fact]
    public async Task Invalid_content_is_not_retried_and_the_file_is_quarantined()
    {
        var fakes = new FakeActivities
        {
            FailWith = (name, _) => name == "ingest.validate" ? new ApplicationFailureException("bad header", "InvalidFile", nonRetryable: true) : null,
        };
        var result = await RunAsync(fakes);

        Assert.Equal("Invalid", result.Status);
        Assert.Equal("ingest.validate", result.FailedStep);
        Assert.Equal(1, fakes.Attempts["ingest.validate"]);
        Assert.DoesNotContain("ingest.convert", fakes.Calls);
        Assert.Equal("invalid", fakes.FinalizedAs);
        Assert.Equal("InvalidFile", fakes.Finalized!.ErrorType);
    }

    [Fact]
    public async Task Transient_errors_are_retried_then_succeed()
    {
        var fakes = new FakeActivities
        {
            Pipeline = FastRetries(DefaultPipelines.FileIngestion),
            FailWith = (name, attempt) => name == "ingest.store" && attempt < 3 ? new IOException("db down") : null,
        };
        var result = await RunAsync(fakes);

        Assert.Equal("Stored", result.Status);
        Assert.Equal(3, fakes.Attempts["ingest.store"]);
    }

    [Fact]
    public async Task Persistent_errors_quarantine_the_file_after_max_attempts()
    {
        var fakes = new FakeActivities
        {
            Pipeline = FastRetries(DefaultPipelines.FileIngestion),
            FailWith = (name, _) => name == "ingest.convert" ? new IOException("always broken") : null,
        };
        var result = await RunAsync(fakes);

        Assert.Equal("Invalid", result.Status);
        Assert.Equal(DefaultPipelines.FileIngestion.Steps.Single(s => s.Activity == "ingest.convert").MaxAttempts, fakes.Attempts["ingest.convert"]);
        Assert.Contains("always broken", result.Error);
        Assert.Equal("invalid", fakes.FinalizedAs);
    }

    [Fact]
    public async Task Steps_come_from_the_runtime_definition()
    {
        var fakes = new FakeActivities
        {
            Pipeline = new PipelineDefinition
            {
                Name = "custom",
                Version = 7,
                Steps =
                [
                    new() { Activity = "ingest.fetch" },
                    new() { Activity = "ingest.validate", Enabled = false },
                    new() { Activity = "ingest.delay" },
                    new() { Activity = "ingest.store" },
                ],
            },
        };
        var result = await RunAsync(fakes);

        Assert.Equal("Stored", result.Status);
        Assert.Equal(["ingest.fetch", "ingest.delay", "ingest.store"], fakes.Calls);
        Assert.Equal(7, fakes.Finalized!.PipelineVersion);
    }

    [Fact]
    public async Task Unknown_pipeline_quarantines_the_file_instead_of_leaving_it_behind()
    {
        var fakes = new FakeActivities();
        var result = await RunAsync(fakes, new FileIngestionInput("a/file.csv", "missing"));

        Assert.Equal("Invalid", result.Status);
        Assert.Equal("pipeline.get", result.FailedStep);
        Assert.Equal("PipelineNotFound", fakes.Finalized!.ErrorType);
        Assert.Empty(fakes.Calls);
    }

    [Fact]
    public async Task Cancelling_a_file_workflow_still_quarantines_the_file()
    {
        var fakes = new FakeActivities
        {
            Pipeline = new PipelineDefinition
            {
                Name = "slow",
                Steps = [new() { Activity = "ingest.fetch" }, new() { Activity = "ingest.delay", HeartbeatTimeoutSeconds = 5, Parameters = new() { ["forever"] = "1" } }],
            },
        };
        var queue = $"test-{Guid.NewGuid()}";
        using var worker = new TemporalWorker(fixture.Env.Client,
            new TemporalWorkerOptions(queue).AddWorkflow<FileIngestionWorkflow>().AddAllActivities(fakes));
        await worker.ExecuteAsync(async () =>
        {
            var handle = await fixture.Env.Client.StartWorkflowAsync(
                (FileIngestionWorkflow wf) => wf.RunAsync(new FileIngestionInput("a/slow.csv")),
                new WorkflowOptions($"ingest-test-{Guid.NewGuid()}", queue));
            await fakes.SlowStepStarted.Task;
            await handle.CancelAsync();

            var result = await handle.GetResultAsync();
            Assert.Equal("Invalid", result.Status);
            Assert.Equal("ingest.delay", result.FailedStep);
            Assert.Equal("Cancelled", fakes.Finalized!.ErrorType);
            Assert.Equal("invalid", fakes.FinalizedAs);
        });
    }
}
