using Microsoft.Extensions.Logging;
using Temporalio.Activities;
using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using Temporalio.Exceptions;
using TemporalPoc.Core.Configuration;
using TemporalPoc.Core.Storage;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Core.Activities;

/// <summary>Activities of the inbox watcher: turn files into workflows.</summary>
public sealed class DispatchActivities(IObjectStore store, ITemporalClient client, ILogger<DispatchActivities> logger)
{
    private const int StartParallelism = 16;

    /// <summary>
    /// Lists incoming/ and starts one FileIngestion workflow per file. Idempotent thanks to the
    /// deterministic workflow id: a file already being processed is simply skipped.
    /// </summary>
    [Activity("dispatch.scan-inbox")]
    public async Task<ScanResult> ScanInboxAsync(ScanRequest request)
    {
        var ctx = ActivityExecutionContext.Current;
        var budget = request.MaxFiles;
        var throttled = 0;
        if (request.MaxInFlight > 0)
        {
            var running = await client.CountWorkflowsAsync("WorkflowType = 'FileIngestion' AND ExecutionStatus = 'Running'");
            budget = (int)Math.Clamp(request.MaxInFlight - running.Count, 0, request.MaxFiles);
        }

        var keys = new List<string>();
        string? token = null;
        var truncated = false;
        do
        {
            var page = await store.ListAsync(FileLayout.Incoming, 1000, token, ctx.CancellationToken);
            keys.AddRange(page.Objects.Select(o => o.Key));
            token = page.ContinuationToken;
            ctx.Heartbeat(keys.Count);
        }
        while (token is not null && keys.Count < request.MaxFiles);
        truncated = token is not null || keys.Count > request.MaxFiles;

        int dispatched = 0, alreadyRunning = 0;
        var candidates = keys.Take(request.MaxFiles).ToList();
        await Parallel.ForEachAsync(candidates, new ParallelOptions { MaxDegreeOfParallelism = StartParallelism, CancellationToken = ctx.CancellationToken },
            async (key, _) =>
            {
                if (Volatile.Read(ref dispatched) >= budget)
                {
                    Interlocked.Increment(ref throttled);
                    return;
                }
                if (await TryStartAsync(FileLayout.Relative(key), request.Pipeline))
                {
                    Interlocked.Increment(ref dispatched);
                }
                else
                {
                    Interlocked.Increment(ref alreadyRunning);
                }
                ctx.Heartbeat(dispatched + alreadyRunning);
            });

        if (dispatched > 0 || throttled > 0)
        {
            logger.LogInformation("Inbox scan: {Listed} listed, {Dispatched} dispatched, {Running} already running, {Throttled} throttled",
                candidates.Count, dispatched, alreadyRunning, throttled);
        }
        // Loop immediately only when there is more work we are allowed to start.
        var hasMore = truncated && dispatched > 0 && throttled == 0;
        return new ScanResult(candidates.Count, dispatched, alreadyRunning, throttled, hasMore);
    }

    /// <summary>
    /// Safety net: files in processing/ for too long without a running workflow (e.g. workflow terminated
    /// by an operator) are dispatched again so that no file is ever left behind.
    /// </summary>
    [Activity("dispatch.reconcile")]
    public async Task<ReconcileResult> ReconcileAsync(ReconcileRequest request)
    {
        var ctx = ActivityExecutionContext.Current;
        var threshold = DateTimeOffset.UtcNow.AddMinutes(-request.OrphanAfterMinutes);
        int checkedCount = 0, reDispatched = 0;
        string? token = null;
        do
        {
            var page = await store.ListAsync(FileLayout.Processing, 1000, token, ctx.CancellationToken);
            foreach (var obj in page.Objects.Where(o => o.LastModified < threshold))
            {
                checkedCount++;
                var relative = FileLayout.Relative(obj.Key);
                if (!await IsRunningAsync(FileLayout.IngestionWorkflowId(relative)) && await TryStartAsync(relative, request.Pipeline))
                {
                    reDispatched++;
                    logger.LogWarning("Re-dispatched orphan file {Key}", relative);
                }
                ctx.Heartbeat(checkedCount);
            }
            token = page.ContinuationToken;
        }
        while (token is not null);
        return new ReconcileResult(checkedCount, reDispatched);
    }

    private async Task<bool> TryStartAsync(string relativeKey, string pipeline)
    {
        try
        {
            await client.StartWorkflowAsync(
                (FileIngestionWorkflow wf) => wf.RunAsync(new FileIngestionInput(relativeKey, pipeline, null, null)),
                new WorkflowOptions(FileLayout.IngestionWorkflowId(relativeKey), TaskQueues.Ingestion)
                {
                    IdReusePolicy = WorkflowIdReusePolicy.AllowDuplicate,
                    IdConflictPolicy = WorkflowIdConflictPolicy.Fail,
                    StaticSummary = $"Ingestion of {relativeKey}",
                });
            return true;
        }
        catch (WorkflowAlreadyStartedException)
        {
            return false;
        }
    }

    private async Task<bool> IsRunningAsync(string workflowId)
    {
        try
        {
            var description = await client.GetWorkflowHandle(workflowId).DescribeAsync();
            return description.Status == WorkflowExecutionStatus.Running;
        }
        catch (RpcException e) when (e.Code == RpcException.StatusCode.NotFound)
        {
            return false;
        }
    }
}
