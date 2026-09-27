using Microsoft.EntityFrameworkCore;
using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using Temporalio.Exceptions;
using TemporalPoc.Core.Data;
using TemporalPoc.Core.Vehicles;

namespace TemporalPoc.Api.Endpoints;

public sealed record VehicleRequestBody(DateOnly From, DateOnly To, int Priority = 0, string? RequestId = null, string? Reason = null);

public sealed record BulkVehicleRequestBody(List<string>? VehicleIds, DateOnly From, DateOnly To, int Priority = 0, string? RequestId = null, string? Reason = null, bool AllKnownVehicles = false, int VehiclesPerSecond = 10);

public sealed record FileReceivedBody(DateOnly Day, string? FileKey = null, string? EventId = null);

public static class VehicleEndpoints
{
    public static void MapVehicleEndpoints(this IEndpointRouteBuilder app)
    {
        var vehicles = app.MapGroup("/api/vehicles").WithTags("Vehicles");

        vehicles.MapPost("/{vehicleId}/requests", async (string vehicleId, VehicleRequestBody body, ITemporalClient client) =>
        {
            // The caller may pass its own RequestId to make its retries idempotent.
            var request = new ProcessingRequest(body.RequestId ?? Guid.NewGuid().ToString("N"), vehicleId, body.From, body.To, body.Priority, body.Reason);
            try
            {
                return Results.Ok(await VehicleWorkflowClient.SubmitWithAckAsync(client, request));
            }
            catch (WorkflowUpdateFailedException e)
            {
                return Results.BadRequest(new { error = e.InnerException?.Message ?? e.Message });
            }
        }).WithSummary("Adds a range of days to the vehicle's queue (starts its workflow if needed) and returns what was merged");

        vehicles.MapPost("/{vehicleId}/files", async (string vehicleId, FileReceivedBody body, ITemporalClient client) =>
        {
            // Real time: fire-and-forget signal, the vehicle workflow applies the "1 per minute" limit.
            var eventId = body.EventId ?? body.FileKey ?? Guid.NewGuid().ToString("N");
            await VehicleWorkflowClient.SendFileReceivedAsync(client, new FileReceivedEvent(eventId, vehicleId, body.Day, body.FileKey));
            return Results.Accepted(value: new { vehicleId, eventId, day = body.Day });
        }).WithSummary("A file was received for the vehicle: current operating day at most once per minute, other days as daily requests");

        vehicles.MapPost("/requests", async (BulkVehicleRequestBody body, ITemporalClient client) =>
        {
            if (!body.AllKnownVehicles && (body.VehicleIds is null || body.VehicleIds.Count == 0))
            {
                return Results.BadRequest(new { error = "No vehicle: give vehicleIds or allKnownVehicles=true" });
            }
            var requestId = body.RequestId ?? Guid.NewGuid().ToString("N");
            // Durable, paced fan-out (a workflow): survives a crash of this API, never sends twice.
            var workflowId = await VehicleWorkflowClient.StartFleetRequestAsync(client, new FleetRequestInput(
                requestId, body.From, body.To, body.Priority, body.Reason, body.VehicleIds, body.AllKnownVehicles, body.VehiclesPerSecond));
            return Results.Accepted(value: new { requestId, workflowId });
        }).WithSummary("Same request for many vehicles or the whole fleet (e.g. after a configuration change); same per-day limits");

        vehicles.MapGet("/requests/{requestId}", async (string requestId, ITemporalClient client) =>
        {
            var handle = client.GetWorkflowHandle<FleetRequestWorkflow>(FleetRequestWorkflow.WorkflowId(requestId));
            return Results.Ok(new { status = (await handle.DescribeAsync()).Status.ToString(), progress = await handle.QueryAsync(wf => wf.Progress) });
        }).WithSummary("Progress of a fleet request (vehicles submitted so far)");

        vehicles.MapGet("/{vehicleId}", async (string vehicleId, ITemporalClient client, PocDbContext db, CancellationToken ct) =>
        {
            VehicleStatus? live = null;
            try
            {
                var handle = client.GetWorkflowHandle<VehicleProcessingWorkflow>(VehicleProcessingWorkflow.WorkflowId(vehicleId));
                if ((await handle.DescribeAsync()).Status == WorkflowExecutionStatus.Running)
                {
                    live = await handle.QueryAsync(wf => wf.Status);
                }
            }
            catch (RpcException e) when (e.Code == RpcException.StatusCode.NotFound)
            {
            }
            var days = await db.VehicleDayRuns.AsNoTracking().Where(r => r.VehicleId == vehicleId)
                .GroupBy(r => r.Status).Select(g => new { g.Key, Count = g.Count() }).ToDictionaryAsync(x => x.Key, x => x.Count, ct);
            return Results.Ok(new { vehicleId, running = live is not null, live, days });
        }).WithSummary("Live queue of the vehicle (pending days, day in progress, failures) and counts per status");

        vehicles.MapGet("/{vehicleId}/days", async (string vehicleId, DateOnly? from, DateOnly? to, PocDbContext db, CancellationToken ct) =>
        {
            var query = db.VehicleDayRuns.AsNoTracking().Where(r => r.VehicleId == vehicleId);
            if (from is { } f) query = query.Where(r => r.Day >= f);
            if (to is { } t) query = query.Where(r => r.Day <= t);
            return await query.OrderBy(r => r.Day).Take(1000).ToListAsync(ct);
        }).WithSummary("Status of each processed day");
    }
}
