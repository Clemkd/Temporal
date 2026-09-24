using TemporalPoc.Core.Domain;
using TemporalPoc.Core.Storage;

namespace TemporalPoc.Api.Endpoints;

public sealed record SimulationRequest(
    int Count = 100,
    int RowsPerFile = 200,
    double InvalidRatio = 0.05,
    double PoisonRatio = 0.01,
    double SlowRatio = 0.0,
    double JsonRatio = 0.3,
    string? Prefix = null,
    int? Seed = null);

/// <summary>Writes generated sensor files (valid, invalid, poison, slow) into incoming/.</summary>
public static class FileSimulator
{
    public static async Task<(string Prefix, Dictionary<string, int> Kinds)> GenerateAsync(
        IObjectStore store, SimulationRequest request, CancellationToken ct)
    {
        var random = request.Seed is { } seed ? new Random(seed) : new Random();
        var prefix = request.Prefix ?? $"sim-{DateTime.UtcNow:yyyyMMdd-HHmmss}";
        var plan = Enumerable.Range(0, request.Count).Select(i =>
        {
            var roll = random.NextDouble();
            var kind = roll < request.InvalidRatio ? GeneratedFileKind.Invalid
                : roll < request.InvalidRatio + request.PoisonRatio ? GeneratedFileKind.Poison
                : roll < request.InvalidRatio + request.PoisonRatio + request.SlowRatio ? GeneratedFileKind.Slow
                : GeneratedFileKind.Valid;
            return (Index: i, Kind: kind, Json: random.NextDouble() < request.JsonRatio, Seed: random.Next());
        }).ToList();

        await Parallel.ForEachAsync(plan, new ParallelOptions { MaxDegreeOfParallelism = 16, CancellationToken = ct }, async (p, token) =>
        {
            var (name, content) = SensorFileGenerator.Generate($"{prefix}/file-{p.Index:D6}", p.Kind, request.RowsPerFile, p.Json, new Random(p.Seed));
            await store.PutBytesAsync(FileLayout.IncomingKey(name), content, p.Json ? "application/json" : "text/csv", null, token);
        });

        return (prefix, plan.GroupBy(p => p.Kind).ToDictionary(g => g.Key.ToString(), g => g.Count()));
    }
}
