// Workflow start benchmark: starts N workflows (no worker is polling, so every start ends up as a
// pending workflow task in the task queue) either one by one or in concurrent batches.
// Writes one CSV line per second: elapsed, total started, starts/s, client latency p50/p99/max, errors.
//
//   dotnet run -c Release -- --address localhost:17233 --count 1000000 --batch 100 --out run.csv
//   (--batch 1 = strictly sequential: each start awaits the previous one)
//   --max-seconds 600 stops starting new workflows after 10 min (the in-flight batch completes).
using System.Diagnostics;
using System.Globalization;
using Temporalio.Client;

var opts = Args.Parse(args);
var client = await TemporalClient.ConnectAsync(new(opts.Address) { Namespace = "default" });
var taskQueue = $"bench-{opts.Label}";
var payload = new BenchInput("incoming/2026/09/26/sensor-file-000000.csv", 12_345, "file-ingestion"); // ~100 bytes, like a real file ref

var latencies = new List<double>(capacity: 200_000);
var lockObj = new object();
long started = 0, errors = 0, duplicates = 0;
var sw = Stopwatch.StartNew();
using var csv = new StreamWriter(opts.Out) { AutoFlush = true };
csv.WriteLine("elapsed_s,started_total,starts_per_s,lat_p50_ms,lat_p99_ms,lat_max_ms,errors_total");

// Reporter: one line per second.
var reporterCts = new CancellationTokenSource();
var reporter = Task.Run(async () =>
{
    long previous = 0;
    var next = 1.0;
    while (!reporterCts.IsCancellationRequested)
    {
        var wait = next - sw.Elapsed.TotalSeconds;
        if (wait > 0) { try { await Task.Delay(TimeSpan.FromSeconds(wait), reporterCts.Token); } catch { } }
        double[] snapshot;
        lock (lockObj) { snapshot = latencies.ToArray(); latencies.Clear(); }
        Array.Sort(snapshot);
        var total = Interlocked.Read(ref started);
        csv.WriteLine(string.Join(',', F(sw.Elapsed.TotalSeconds), total, total - previous,
            F(Pct(snapshot, 0.50)), F(Pct(snapshot, 0.99)), F(snapshot.Length > 0 ? snapshot[^1] : 0), Interlocked.Read(ref errors)));
        if (total / 50_000 != previous / 50_000)
        {
            Console.WriteLine($"{DateTime.Now:HH:mm:ss} {opts.Label}: {total:N0}/{opts.Count:N0} ({total - previous}/s)");
        }
        previous = total;
        next += 1;
    }
});

async Task StartOneAsync(long i)
{
    var t = Stopwatch.GetTimestamp();
    for (var attempt = 1; ; attempt++)
    {
        try
        {
            await client.StartWorkflowAsync("BenchWorkflow", [payload with { Index = i }],
                new WorkflowOptions($"bench-{opts.Label}-{i}", taskQueue));
            break;
        }
        catch (Temporalio.Exceptions.WorkflowAlreadyStartedException) when (attempt > 1)
        {
            // A previous attempt timed out on the client but the server did create the workflow:
            // the deterministic workflow id makes the retry idempotent.
            Interlocked.Increment(ref duplicates);
            break;
        }
        catch (Exception) when (attempt < 5)
        {
            Interlocked.Increment(ref errors);
            await Task.Delay(100 * attempt);
        }
    }
    var ms = Stopwatch.GetElapsedTime(t).TotalMilliseconds;
    lock (lockObj) { latencies.Add(ms); }
    Interlocked.Increment(ref started);
}

if (opts.Batch <= 1)
{
    for (long i = 0; i < opts.Count && sw.Elapsed.TotalSeconds < opts.MaxSeconds; i++)
    {
        await StartOneAsync(i);
    }
}
else
{
    // "Batch" = N concurrent StartWorkflow calls, the next batch starts when the whole batch is done.
    // (Temporal has no bulk-start RPC: this is the only way to batch from a client.)
    for (long offset = 0; offset < opts.Count && sw.Elapsed.TotalSeconds < opts.MaxSeconds; offset += opts.Batch)
    {
        var size = (int)Math.Min(opts.Batch, opts.Count - offset);
        var tasks = new Task[size];
        for (var j = 0; j < size; j++)
        {
            tasks[j] = StartOneAsync(offset + j);
        }
        await Task.WhenAll(tasks);
    }
}

var elapsed = sw.Elapsed.TotalSeconds;
reporterCts.Cancel();
await reporter;
var done = Interlocked.Read(ref started);
Console.WriteLine($"DONE {opts.Label}: {done:N0} starts in {elapsed:F1}s = {done / elapsed:F0}/s, retried errors: {errors}, retries already started server-side: {duplicates}");
File.WriteAllText(Path.ChangeExtension(opts.Out, ".summary.json"),
    $"{{\"label\":\"{opts.Label}\",\"count\":{done},\"batch\":{opts.Batch},\"elapsed_s\":{F(elapsed)},\"rate\":{F(done / elapsed)},\"errors\":{errors},\"duplicates\":{duplicates}}}");

static double Pct(double[] sorted, double p) => sorted.Length == 0 ? 0 : sorted[Math.Min(sorted.Length - 1, (int)(p * sorted.Length))];
static string F(double v) => v.ToString("0.###", CultureInfo.InvariantCulture);

record BenchInput(string Key, long Index, string Pipeline);

record Args(string Address, long Count, int Batch, string Label, string Out, double MaxSeconds)
{
    public static Args Parse(string[] a)
    {
        string Get(string name, string fallback) { var i = Array.IndexOf(a, name); return i >= 0 && i + 1 < a.Length ? a[i + 1] : fallback; }
        var batch = int.Parse(Get("--batch", "1"));
        var label = Get("--label", batch <= 1 ? "seq" : $"b{batch}");
        return new(Get("--address", "localhost:17233"), long.Parse(Get("--count", "10000")), batch, label, Get("--out", $"{label}.csv"),
            double.Parse(Get("--max-seconds", "1e12"), CultureInfo.InvariantCulture));
    }
}
