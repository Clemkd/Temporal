using Temporalio.Activities;
using Temporalio.Exceptions;
using TemporalPoc.Core.Pipelines;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Core.Activities;

/// <summary>
/// Loads a pipeline definition. Executed as an activity so the definition read at runtime is recorded
/// in the workflow history: replays stay deterministic even if the definition changes afterwards.
/// </summary>
public sealed class PipelineActivities(PipelineRepository repository)
{
    [Activity("pipeline.get")]
    public async Task<PipelineDefinition> GetAsync(GetPipelineRequest request)
    {
        var ct = ActivityExecutionContext.Current.CancellationToken;
        return await repository.GetAsync(request.Name, request.Version, ct)
            ?? throw new ApplicationFailureException(
                $"Pipeline '{request.Name}' v{request.Version?.ToString() ?? "latest"} not found", "PipelineNotFound", nonRetryable: true);
    }
}
