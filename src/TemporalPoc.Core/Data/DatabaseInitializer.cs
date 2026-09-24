using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using TemporalPoc.Core.Pipelines;

namespace TemporalPoc.Core.Data;

public static class DatabaseInitializer
{
    /// <summary>
    /// Creates the schema and seeds default pipelines. Several processes (API + N workers) may start
    /// at the same time, so the initialization is serialized with a Postgres advisory lock.
    /// </summary>
    public static async Task InitializeAsync(PocDbContext db, ILogger logger, CancellationToken ct = default)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await db.Database.OpenConnectionAsync(ct);
                break;
            }
            catch (Exception e) when (attempt < 30 && !ct.IsCancellationRequested)
            {
                logger.LogWarning("Database not reachable yet ({Message}), retrying...", e.Message);
                await Task.Delay(TimeSpan.FromSeconds(2), ct);
            }
        }

        try
        {
            await db.Database.ExecuteSqlRawAsync("SELECT pg_advisory_lock(424242)", ct);
            await db.Database.EnsureCreatedAsync(ct);

            foreach (var pipeline in DefaultPipelines.All)
            {
                if (!await db.Pipelines.AnyAsync(p => p.Name == pipeline.Name, ct))
                {
                    db.Pipelines.Add(PipelineRepository.ToRecord(pipeline));
                    logger.LogInformation("Seeded pipeline {Pipeline} v{Version}", pipeline.Name, pipeline.Version);
                }
            }
            await db.SaveChangesAsync(ct);
        }
        finally
        {
            await db.Database.ExecuteSqlRawAsync("SELECT pg_advisory_unlock(424242)", CancellationToken.None);
            await db.Database.CloseConnectionAsync();
        }
    }
}
