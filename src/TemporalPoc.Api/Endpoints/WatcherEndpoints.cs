using Temporalio.Client;
using Temporalio.Exceptions;
using TemporalPoc.Core.Configuration;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Api.Endpoints;

public static class WatcherEndpoints
{
    public static void MapWatcherEndpoints(this IEndpointRouteBuilder app)
    {
        var watcher = app.MapGroup("/api/watcher").WithTags("Inbox watcher");

        watcher.MapGet("/", async (ITemporalClient client) =>
            Results.Ok(await Handle(client).QueryAsync(wf => wf.State)));

        watcher.MapPost("/start", async (ITemporalClient client, WatcherSettings settings) =>
        {
            await Bootstrap.EnsureWatcherAsync(client, settings);
            return Results.Accepted();
        }).WithSummary("Starts the watcher if it is not running");

        watcher.MapPost("/poke", async (ITemporalClient client) =>
        {
            await Handle(client).SignalAsync(wf => wf.PokeAsync());
            return Results.Accepted();
        });

        watcher.MapPost("/pause", async (ITemporalClient client) =>
        {
            await Handle(client).SignalAsync(wf => wf.PauseAsync());
            return Results.Accepted();
        });

        watcher.MapPost("/resume", async (ITemporalClient client) =>
        {
            await Handle(client).SignalAsync(wf => wf.ResumeAsync());
            return Results.Accepted();
        });

        watcher.MapPut("/config", async (WatcherConfig config, ITemporalClient client) =>
        {
            try
            {
                return Results.Ok(await Handle(client).ExecuteUpdateAsync(wf => wf.ConfigureAsync(config)));
            }
            catch (WorkflowUpdateFailedException e)
            {
                return Results.BadRequest(new { error = e.InnerException?.Message ?? e.Message });
            }
        }).WithSummary("Changes the configuration of the running watcher (validated update)");
    }

    public static async Task TryPokeAsync(ITemporalClient client)
    {
        try
        {
            await Handle(client).SignalAsync(wf => wf.PokeAsync());
        }
        catch (RpcException)
        {
            // Watcher not started yet: it will pick the files up on its first scan.
        }
    }

    private static WorkflowHandle<InboxWatcherWorkflow> Handle(ITemporalClient client) =>
        client.GetWorkflowHandle<InboxWatcherWorkflow>(InboxWatcherWorkflow.WorkflowId);
}
