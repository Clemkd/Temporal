// Vehicle / day benchmark: one parent workflow per vehicle, one child workflow per day.
//
// Runs the worker AND the driver in this process:
//  - starts N "Vehicle" workflows (1500 by default), each processing D days (365 by default);
//  - a Vehicle workflow starts one "Day" child workflow per day, at most --window at a time
//    (0 = all 365 at once = full fan-out, 1 = one day after the other);
//  - a Day workflow runs one activity (the day's processing, simulated) and completes.
// Writes one CSV line per second: elapsed, vehicles started / done, days done, days/s, errors.
//
//   dotnet run -c Release -- --vehicles 1500 --days 365 --window 0 --max-seconds 600 --out fanout.csv
//   --from 2025-01-01 --days 7: period processed by each vehicle (one child per date, e.g. id veh-0001-2025-01-03).
//   --day-as-activity: alternative design, the day is an activity of the vehicle workflow (no child workflow).
using System.Diagnostics;
using System.Globalization;
using Temporalio.Activities;
using Temporalio.Client;
using Temporalio.Worker;
using Temporalio.Workflows;

var opts = Args.Parse(args);
var client = await TemporalClient.ConnectAsync(new(opts.Address) { Namespace = "default" });
var taskQueue = $"vehicles-{opts.Label}";

using var worker = new TemporalWorker(client, new TemporalWorkerOptions(taskQueue)
{
    MaxConcurrentActivities = 200,
    MaxConcurrentWorkflowTasks = 200,
    // Every running vehicle workflow stays cached (otherwise its growing history is replayed).
    MaxCachedWorkflows = 5000,
}
    .AddWorkflow<VehicleWorkflow>()
    .AddWorkflow<DayWorkflow>()
    .AddAllActivities(new DayActivities()));

using var workerCts = new CancellationTokenSource();
var workerTask = worker.ExecuteAsync(workerCts.Token);

var sw = Stopwatch.StartNew();
long vehiclesStarted = 0, vehiclesDone = 0, errors = 0;
using var csv = new StreamWriter(opts.Out) { AutoFlush = true };
csv.WriteLine("elapsed_s,vehicles_started,vehicles_done,days_done,days_per_s,errors_total");

var reporterCts = new CancellationTokenSource();
var reporter = Task.Run(async () =>
{
    long previous = 0;
    var next = 1.0;
    while (!reporterCts.IsCancellationRequested)
    {
        var wait = next - sw.Elapsed.TotalSeconds;
        if (wait > 0) { try { await Task.Delay(TimeSpan.FromSeconds(wait), reporterCts.Token); } catch { } }
        var days = Interlocked.Read(ref DayActivities.Done);
        csv.WriteLine(string.Join(',', F(sw.Elapsed.TotalSeconds), Interlocked.Read(ref vehiclesStarted),
            Interlocked.Read(ref vehiclesDone), days, days - previous, Interlocked.Read(ref errors)));
        if (days / 20_000 != previous / 20_000)
        {
            Console.WriteLine($"{DateTime.Now:HH:mm:ss} {opts.Label}: {days:N0} days done, {Interlocked.Read(ref vehiclesDone)}/{opts.Vehicles} vehicles ({days - previous}/s)");
        }
        previous = days;
        next += 1;
    }
});

// Vehicles done: counted through visibility every 5 s (1500 long polls would be heavier than the test).
var doneCts = new CancellationTokenSource();
var doneCounter = Task.Run(async () =>
{
    while (!doneCts.IsCancellationRequested)
    {
        try
        {
            var count = await client.CountWorkflowsAsync($"WorkflowType = 'VehicleWorkflow' AND TaskQueue = '{taskQueue}' AND ExecutionStatus = 'Completed'");
            Interlocked.Exchange(ref vehiclesDone, count.Count);
        }
        catch { }
        try { await Task.Delay(5000, doneCts.Token); } catch { }
    }
});

// Start the vehicles, 100 at a time.
for (var offset = 0; offset < opts.Vehicles; offset += 100)
{
    var tasks = Enumerable.Range(offset, Math.Min(100, opts.Vehicles - offset)).Select(async i =>
    {
        var input = new VehicleInput($"veh-{opts.Label}-{i:D4}", opts.Days, opts.Window, opts.DayAsActivity, opts.From);
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await client.StartWorkflowAsync((VehicleWorkflow wf) => wf.RunAsync(input), new WorkflowOptions(input.VehicleId, taskQueue));
                break;
            }
            catch (Temporalio.Exceptions.WorkflowAlreadyStartedException) when (attempt > 1) { break; }
            catch (Exception) when (attempt < 5) { Interlocked.Increment(ref errors); await Task.Delay(200 * attempt); }
        }
        Interlocked.Increment(ref vehiclesStarted);
    });
    await Task.WhenAll(tasks);
}
Console.WriteLine($"{DateTime.Now:HH:mm:ss} {opts.Label}: {opts.Vehicles} vehicles started in {sw.Elapsed.TotalSeconds:F1}s");

var totalDays = (long)opts.Vehicles * opts.Days;
while (Interlocked.Read(ref DayActivities.Done) < totalDays && sw.Elapsed.TotalSeconds < opts.MaxSeconds)
{
    await Task.Delay(500);
}
var elapsed = sw.Elapsed.TotalSeconds;
var daysDone = Interlocked.Read(ref DayActivities.Done);
var complete = daysDone >= totalDays;
if (complete)
{
    // Let the last vehicle workflows complete so the "vehicles done" counter is final.
    for (var i = 0; i < 20 && Interlocked.Read(ref vehiclesDone) < opts.Vehicles; i++) { await Task.Delay(1000); }
}
reporterCts.Cancel();
doneCts.Cancel();
await reporter;
await doneCounter;
Console.WriteLine($"DONE {opts.Label}: {daysDone:N0}/{totalDays:N0} days in {elapsed:F1}s = {daysDone / elapsed:F0} days/s, complete={complete}");
File.WriteAllText(Path.ChangeExtension(opts.Out, ".summary.json"),
    $"{{\"label\":\"{opts.Label}\",\"vehicles\":{opts.Vehicles},\"days\":{opts.Days},\"window\":{opts.Window},\"days_done\":{daysDone}," +
    $"\"vehicles_done\":{Interlocked.Read(ref vehiclesDone)},\"elapsed_s\":{F(elapsed)},\"rate\":{F(daysDone / elapsed)},\"complete\":{(complete ? "true" : "false")},\"errors\":{errors}}}");
workerCts.Cancel();
try { await workerTask; } catch (OperationCanceledException) { }

static string F(double v) => v.ToString("0.###", CultureInfo.InvariantCulture);

/// <summary>Period to process: From..From+Days-1 (dates as ISO strings: yyyy-MM-dd).</summary>
public record VehicleInput(string VehicleId, int Days, int Window, bool DayAsActivity = false, string From = "2025-01-01");

public record DayInput(string VehicleId, int Day, string Date = "");

public record DaySummary(string VehicleId, int Day, int Files, int Measurements);

/// <summary>One workflow per vehicle: runs one child workflow per day, at most Window at a time (0 = no limit).</summary>
[Workflow("VehicleWorkflow")]
public class VehicleWorkflow
{
    private int _completed;

    [WorkflowRun]
    public async Task<int> RunAsync(VehicleInput input)
    {
        var running = new List<Task<DaySummary>>();
        for (var day = 1; day <= input.Days; day++)
        {
            if (input.Window > 0 && running.Count >= input.Window)
            {
                var finished = await Workflow.WhenAnyAsync(running);
                running.Remove(finished);
                await finished;
                _completed++;
            }
            var date = DateOnly.ParseExact(input.From, "yyyy-MM-dd", CultureInfo.InvariantCulture).AddDays(day - 1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            var dayInput = new DayInput(input.VehicleId, day, date);
            running.Add(input.DayAsActivity
                // Alternative design: the day is an activity of the vehicle workflow (no child workflow).
                ? Workflow.ExecuteActivityAsync(
                    (DayActivities a) => a.ProcessDayAsync(dayInput),
                    new ActivityOptions { StartToCloseTimeout = TimeSpan.FromMinutes(5) })
                : Workflow.ExecuteChildWorkflowAsync(
                    (DayWorkflow wf) => wf.RunAsync(dayInput),
                    new ChildWorkflowOptions { Id = $"{input.VehicleId}-{date}" }));
        }
        await Task.WhenAll(running);
        _completed += running.Count;
        return _completed;
    }

    [WorkflowQuery]
    public int Completed => _completed;
}

/// <summary>One workflow per (vehicle, day): the day's processing as a single activity.</summary>
[Workflow("DayWorkflow")]
public class DayWorkflow
{
    [WorkflowRun]
    public Task<DaySummary> RunAsync(DayInput input) =>
        Workflow.ExecuteActivityAsync(
            (DayActivities a) => a.ProcessDayAsync(input),
            new ActivityOptions { StartToCloseTimeout = TimeSpan.FromMinutes(5) });
}

public class DayActivities
{
    public static long Done;

    /// <summary>Simulated day processing (no I/O): the benchmark measures Temporal's own cost.</summary>
    [Activity("ProcessDay")]
    public Task<DaySummary> ProcessDayAsync(DayInput input)
    {
        Interlocked.Increment(ref Done);
        return Task.FromResult(new DaySummary(input.VehicleId, input.Day, 24, 24 * 100));
    }
}

record Args(string Address, int Vehicles, int Days, int Window, string Label, string Out, double MaxSeconds, bool DayAsActivity, string From)
{
    public static Args Parse(string[] a)
    {
        string Get(string name, string fallback) { var i = Array.IndexOf(a, name); return i >= 0 && i + 1 < a.Length ? a[i + 1] : fallback; }
        var window = int.Parse(Get("--window", "0"));
        var asActivity = a.Contains("--day-as-activity");
        var label = Get("--label", (asActivity ? "act" : "") + (window == 0 ? "fanout" : $"w{window}"));
        return new(Get("--address", "localhost:17233"), int.Parse(Get("--vehicles", "1500")), int.Parse(Get("--days", "365")), window,
            label, Get("--out", $"{label}.csv"), double.Parse(Get("--max-seconds", "1e12"), CultureInfo.InvariantCulture), asActivity,
            Get("--from", "2025-01-01"));
    }
}
