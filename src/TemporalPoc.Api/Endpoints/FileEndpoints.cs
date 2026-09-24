using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Temporalio.Client;
using Temporalio.Exceptions;
using TemporalPoc.Core.Data;
using TemporalPoc.Core.Storage;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Api.Endpoints;

public static class FileEndpoints
{
    public static void MapFileEndpoints(this IEndpointRouteBuilder app)
    {
        var files = app.MapGroup("/api/files").WithTags("Files");

        files.MapPost("/", async (IFormFileCollection upload, IObjectStore store, ITemporalClient client, CancellationToken ct) =>
        {
            var keys = new List<string>();
            foreach (var file in upload)
            {
                var key = FileLayout.IncomingKey(Path.GetFileName(file.FileName));
                await using var stream = file.OpenReadStream();
                await store.PutAsync(key, stream, file.ContentType, null, ct);
                keys.Add(key);
            }
            await WatcherEndpoints.TryPokeAsync(client);
            return Results.Accepted(value: new { uploaded = keys });
        }).DisableAntiforgery().WithSummary("Upload one or more files into incoming/");

        files.MapGet("/", async (PocDbContext db, string? status, int take = 100, CancellationToken ct = default) =>
        {
            var query = db.Files.AsNoTracking();
            if (!string.IsNullOrEmpty(status))
            {
                query = query.Where(f => f.Status == status);
            }
            return await query.OrderByDescending(f => f.UpdatedAt).Take(Math.Clamp(take, 1, 1000)).ToListAsync(ct);
        }).WithSummary("Files known by the database (status: Processing, Stored, Invalid)");

        files.MapGet("/status", async ([FromQuery] string key, PocDbContext db, ITemporalClient client, IObjectStore store, CancellationToken ct) =>
        {
            var relative = FileLayout.Relative(key);
            var record = await db.Files.AsNoTracking().FirstOrDefaultAsync(f => f.Key == relative, ct);
            FileIngestionStatus? workflow = null;
            try
            {
                workflow = await client.GetWorkflowHandle<FileIngestionWorkflow>(FileLayout.IngestionWorkflowId(relative)).QueryAsync(wf => wf.Status);
            }
            catch (RpcException e) when (e.Code == RpcException.StatusCode.NotFound)
            {
            }

            var locations = new Dictionary<string, IReadOnlyDictionary<string, string>>();
            foreach (var prefix in new[] { FileLayout.Incoming, FileLayout.Processing, FileLayout.Processed, FileLayout.Invalid })
            {
                if (await store.StatAsync(prefix + relative, ct) is not null)
                {
                    locations[prefix + relative] = await store.GetTagsAsync(prefix + relative, ct);
                }
            }
            return Results.Ok(new { key = relative, record, workflow, objects = locations });
        }).WithSummary("Database record, live workflow state and S3 location/tags of one file");

        files.MapPost("/reprocess", async ([FromQuery] string key, IObjectStore store, ITemporalClient client, CancellationToken ct) =>
        {
            var relative = FileLayout.Relative(key);
            if (!await store.MoveAsync(FileLayout.InvalidKey(relative), FileLayout.IncomingKey(relative), null, ct))
            {
                return Results.NotFound(new { error = $"{FileLayout.InvalidKey(relative)} not found" });
            }
            await store.DeleteAsync(FileLayout.ErrorReportKey(relative), ct);
            await WatcherEndpoints.TryPokeAsync(client);
            return Results.Accepted(value: new { requeued = FileLayout.IncomingKey(relative) });
        }).WithSummary("Moves an invalid file back to incoming/ (e.g. after fixing the pipeline)");

        app.MapPost("/api/simulation/files", async (SimulationRequest request, IObjectStore store, ITemporalClient client, CancellationToken ct) =>
        {
            var (prefix, counts) = await FileSimulator.GenerateAsync(store, request, ct);
            await WatcherEndpoints.TryPokeAsync(client);
            return Results.Accepted(value: new { prefix, generated = request.Count, kinds = counts });
        }).WithTags("Simulation").WithSummary("Generates files (valid, invalid, poison, slow) directly in incoming/");
    }
}
