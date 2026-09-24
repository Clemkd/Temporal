using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using TemporalPoc.Core.Data;

namespace TemporalPoc.Core.Pipelines;

/// <summary>Versioned pipeline definitions stored in the database (append only).</summary>
public sealed class PipelineRepository(PocDbContext db)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public async Task<PipelineDefinition?> GetAsync(string name, int? version = null, CancellationToken ct = default)
    {
        var query = db.Pipelines.AsNoTracking().Where(p => p.Name == name);
        var record = version is { } v
            ? await query.FirstOrDefaultAsync(p => p.Version == v, ct)
            : await query.OrderByDescending(p => p.Version).FirstOrDefaultAsync(ct);
        return record is null ? null : ToDefinition(record);
    }

    public async Task<IReadOnlyList<PipelineDefinition>> ListLatestAsync(CancellationToken ct = default)
    {
        var records = await db.Pipelines.AsNoTracking().ToListAsync(ct);
        return records.GroupBy(r => r.Name)
            .Select(g => ToDefinition(g.MaxBy(r => r.Version)!))
            .OrderBy(p => p.Name)
            .ToList();
    }

    public async Task<IReadOnlyList<PipelineDefinition>> HistoryAsync(string name, CancellationToken ct = default)
    {
        var records = await db.Pipelines.AsNoTracking().Where(p => p.Name == name).OrderByDescending(p => p.Version).ToListAsync(ct);
        return records.Select(ToDefinition).ToList();
    }

    /// <summary>Saves a new version of the pipeline. Running workflows keep the version they started with.</summary>
    public async Task<PipelineDefinition> SaveNewVersionAsync(PipelineDefinition definition, CancellationToken ct = default)
    {
        var current = await db.Pipelines.Where(p => p.Name == definition.Name).MaxAsync(p => (int?)p.Version, ct) ?? 0;
        var saved = definition with { Version = current + 1 };
        db.Pipelines.Add(new PipelineDefinitionRecord
        {
            Name = saved.Name,
            Version = saved.Version,
            Comment = saved.Comment,
            DefinitionJson = JsonSerializer.Serialize(saved, Json),
            CreatedAt = DateTimeOffset.UtcNow,
        });
        await db.SaveChangesAsync(ct);
        return saved;
    }

    private static PipelineDefinition ToDefinition(PipelineDefinitionRecord record) =>
        JsonSerializer.Deserialize<PipelineDefinition>(record.DefinitionJson, Json)! with { Version = record.Version, Name = record.Name };

    internal static PipelineDefinitionRecord ToRecord(PipelineDefinition definition) => new()
    {
        Name = definition.Name,
        Version = definition.Version,
        Comment = definition.Comment,
        DefinitionJson = JsonSerializer.Serialize(definition, Json),
        CreatedAt = DateTimeOffset.UtcNow,
    };
}
