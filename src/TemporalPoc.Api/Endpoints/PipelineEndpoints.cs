using TemporalPoc.Core.Pipelines;

namespace TemporalPoc.Api.Endpoints;

public sealed record PipelineUpdateRequest(string? Comment, List<StepDefinition> Steps);

public static class PipelineEndpoints
{
    public static void MapPipelineEndpoints(this IEndpointRouteBuilder app)
    {
        var pipelines = app.MapGroup("/api/pipelines").WithTags("Pipelines");

        pipelines.MapGet("/", async (PipelineRepository repo, CancellationToken ct) => await repo.ListLatestAsync(ct));

        pipelines.MapGet("/catalog", () => StepCatalog.ByPipelineKind).WithSummary("Steps (activities) usable in pipelines");

        pipelines.MapGet("/{name}", async (string name, int? version, PipelineRepository repo, CancellationToken ct) =>
            await repo.GetAsync(name, version, ct) is { } p ? Results.Ok(p) : Results.NotFound());

        pipelines.MapGet("/{name}/history", async (string name, PipelineRepository repo, CancellationToken ct) =>
            await repo.HistoryAsync(name, ct));

        pipelines.MapPut("/{name}", async (string name, PipelineUpdateRequest request, PipelineRepository repo, CancellationToken ct) =>
        {
            var definition = new PipelineDefinition { Name = name, Comment = request.Comment, Steps = request.Steps };
            var errors = StepCatalog.Validate(definition);
            if (errors.Count > 0)
            {
                return Results.BadRequest(new { errors });
            }
            var saved = await repo.SaveNewVersionAsync(definition, ct);
            return Results.Ok(saved);
        }).WithSummary("Saves a new version. New workflows use it immediately; running ones keep their version (or reload on signal).");

        pipelines.MapPost("/{name}/reset", async (string name, PipelineRepository repo, CancellationToken ct) =>
            DefaultPipelines.All.FirstOrDefault(p => p.Name == name) is { } defaults
                ? Results.Ok(await repo.SaveNewVersionAsync(defaults with { Comment = "reset to defaults" }, ct))
                : Results.NotFound())
            .WithSummary("Saves the built-in default definition as a new version");
    }
}
