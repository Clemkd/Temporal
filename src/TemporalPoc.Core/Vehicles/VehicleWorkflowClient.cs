using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using Temporalio.Exceptions;

namespace TemporalPoc.Core.Vehicles;

/// <summary>Client side of the vehicle workflow: every call starts the workflow if it is not running.</summary>
public static class VehicleWorkflowClient
{
    /// <summary>Update-With-Start: returns the acknowledgement (added / merged / duplicate).</summary>
    public static async Task<SubmitAck> SubmitWithAckAsync(ITemporalClient client, ProcessingRequest request)
    {
        for (var attempt = 1; ; attempt++)
        {
            var start = WithStartWorkflowOperation.Create(
                (VehicleProcessingWorkflow wf) => wf.RunAsync(new VehicleWorkflowInput(request.VehicleId, null, null)),
                StartOptions(request.VehicleId));
            try
            {
                return await client.ExecuteUpdateWithStartWorkflowAsync(
                    (VehicleProcessingWorkflow wf) => wf.SubmitAsync(request),
                    new WorkflowUpdateWithStartOptions { StartWorkflowOperation = start });
            }
            catch (Exception e) when (attempt < 3 && e is WorkflowUpdateRpcTimeoutOrCanceledException or RpcException { Code: RpcException.StatusCode.NotFound })
            {
                // The workflow was completing (idle) at that moment: the same RequestId makes the retry safe.
                await Task.Delay(200 * attempt);
            }
        }
    }

    /// <summary>Signal-With-Start of a request (bulk, fire-and-forget).</summary>
    public static Task SubmitSignalAsync(ITemporalClient client, ProcessingRequest request, string? taskQueue = null) =>
        SignalWithStartAsync(client, request.VehicleId, "SubmitRequest", request, taskQueue);

    /// <summary>Signal-With-Start of a real time file event.</summary>
    public static Task SendFileReceivedAsync(ITemporalClient client, FileReceivedEvent e) =>
        SignalWithStartAsync(client, e.VehicleId, "FileReceived", e);

    private static Task SignalWithStartAsync(ITemporalClient client, string vehicleId, string signal, object arg, string? taskQueue = null)
    {
        var options = StartOptions(vehicleId, taskQueue);
        options.StartSignal = signal;
        options.StartSignalArgs = [arg];
        return client.StartWorkflowAsync((VehicleProcessingWorkflow wf) => wf.RunAsync(new VehicleWorkflowInput(vehicleId, null, null)), options);
    }

    public static WorkflowOptions StartOptions(string vehicleId, string? taskQueue = null) => new(VehicleProcessingWorkflow.WorkflowId(vehicleId), taskQueue ?? VehicleProcessingWorkflow.TaskQueue)
    {
        IdConflictPolicy = WorkflowIdConflictPolicy.UseExisting,   // running: attach to it
        IdReusePolicy = WorkflowIdReusePolicy.AllowDuplicate,      // completed (idle): start a new run
        StaticSummary = $"Processing queue of vehicle {vehicleId}",
    };

    /// <summary>Starts (idempotently) the durable fan-out of a request to many vehicles.</summary>
    public static async Task<string> StartFleetRequestAsync(ITemporalClient client, FleetRequestInput input, string? taskQueue = null)
    {
        try
        {
            var handle = await client.StartWorkflowAsync(
            (FleetRequestWorkflow wf) => wf.RunAsync(input),
            new WorkflowOptions(FleetRequestWorkflow.WorkflowId(input.RequestId), taskQueue ?? Configuration.TaskQueues.Control)
            {
                IdConflictPolicy = WorkflowIdConflictPolicy.UseExisting,        // same RequestId while running: one fan-out
                IdReusePolicy = WorkflowIdReusePolicy.RejectDuplicate,           // ... and after it completed
                StaticSummary = $"Request {input.RequestId} for {(input.AllKnownVehicles ? "the whole fleet" : $"{input.VehicleIds?.Count} vehicles")}",
            });
            return handle.Id;
        }
        catch (WorkflowAlreadyStartedException e)
        {
            return e.WorkflowId;   // already sent: idempotent
        }
    }
}
