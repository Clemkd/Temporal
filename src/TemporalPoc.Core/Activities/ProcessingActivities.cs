using Microsoft.EntityFrameworkCore;
using Temporalio.Activities;
using TemporalPoc.Core.Chaos;
using TemporalPoc.Core.Data;
using TemporalPoc.Core.Domain;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Core.Activities;

/// <summary>
/// Steps of the sensor processing pipeline. Data never travels through Temporal: steps exchange a
/// batch reference (id range) and a staging table (claim-check pattern). Each step is idempotent per (job, batch).
/// </summary>
public sealed class ProcessingActivities(PocDbContext db, ChaosMonkey chaos)
{
    private static CancellationToken Ct => ActivityExecutionContext.Current.CancellationToken;

    /// <summary>Retrieval by sensor type: next batch after the cursor (keyset pagination).</summary>
    [Activity("processing.fetch-batch")]
    public async Task<BatchRef> FetchBatchAsync(FetchBatchRequest request)
    {
        chaos.MaybeFail("processing.fetch-batch");
        var query = db.Measurements.AsNoTracking()
            .Where(m => m.SensorType == request.SensorType && m.Id > request.AfterId);
        if (request.OnlyUncategorized)
        {
            query = query.Where(m => m.Category == null || m.CategorizedByJob == request.JobId);
        }

        var ids = await query.OrderBy(m => m.Id).Select(m => m.Id).Take(request.BatchSize).ToListAsync(Ct);
        return ids.Count == 0
            ? new BatchRef(request.BatchNumber, request.AfterId, request.AfterId, 0)
            : new BatchRef(request.BatchNumber, ids[0], ids[^1], ids.Count);
    }

    /// <summary>Categorizes the measurements of the batch into the staging table.</summary>
    [Activity("processing.categorize")]
    public async Task<BatchStepResult> CategorizeAsync(BatchContext batch)
    {
        chaos.MaybeFail("processing.categorize");
        var thresholds = ResolveThresholds(batch.SensorType, batch.Parameters);

        var rows = await BatchQuery(batch).Select(m => new { m.Id, m.Value }).ToListAsync(Ct);
        var staging = rows.Select(r => new CategorizationStaging
        {
            JobId = batch.JobId,
            BatchNumber = batch.Batch.BatchNumber,
            MeasurementId = r.Id,
            Category = thresholds.Categorize(r.Value),
        }).ToList();

        await using var tx = await db.Database.BeginTransactionAsync(Ct);
        await db.CategorizationStaging
            .Where(s => s.JobId == batch.JobId && s.BatchNumber == batch.Batch.BatchNumber)
            .ExecuteDeleteAsync(Ct);
        db.CategorizationStaging.AddRange(staging);
        await db.SaveChangesAsync(Ct);
        await tx.CommitAsync(Ct);

        var categories = staging.GroupBy(s => s.Category).ToDictionary(g => g.Key, g => (long)g.Count());
        return new BatchStepResult(staging.Count, categories);
    }

    /// <summary>Applies the categories of the staging table to the measurements.</summary>
    [Activity("processing.update-measurements")]
    public async Task<BatchStepResult> UpdateMeasurementsAsync(BatchContext batch)
    {
        chaos.MaybeFail("processing.update-measurements");
        var affected = await db.Database.ExecuteSqlInterpolatedAsync($"""
            UPDATE measurements AS m
               SET "Category" = s."Category", "CategorizedAt" = now(), "CategorizedByJob" = s."JobId"
              FROM categorization_staging AS s
             WHERE s."JobId" = {batch.JobId} AND s."BatchNumber" = {batch.Batch.BatchNumber}
               AND m."Id" = s."MeasurementId"
            """, Ct);
        return new BatchStepResult(affected);
    }

    /// <summary>Inserts per-sensor aggregates of the batch (replaces previous results of the same batch).</summary>
    [Activity("processing.insert-results")]
    public async Task<BatchStepResult> InsertResultsAsync(BatchContext batch)
    {
        chaos.MaybeFail("processing.insert-results");
        var rows = await (
            from s in db.CategorizationStaging
            where s.JobId == batch.JobId && s.BatchNumber == batch.Batch.BatchNumber
            join m in db.Measurements on s.MeasurementId equals m.Id
            select new { m.SensorId, m.Value, s.Category }).ToListAsync(Ct);

        var now = DateTimeOffset.UtcNow;
        var results = rows.GroupBy(r => r.SensorId).Select(g => new ProcessingResult
        {
            JobId = batch.JobId,
            BatchNumber = batch.Batch.BatchNumber,
            SensorId = g.Key,
            SensorType = batch.SensorType,
            Count = g.Count(),
            Min = g.Min(r => r.Value),
            Max = g.Max(r => r.Value),
            Avg = Math.Round(g.Average(r => r.Value), 4),
            LowCount = g.Count(r => r.Category == Categories.Low),
            NormalCount = g.Count(r => r.Category == Categories.Normal),
            HighCount = g.Count(r => r.Category == Categories.High),
            CriticalCount = g.Count(r => r.Category == Categories.Critical),
            CreatedAt = now,
        }).ToList();

        await using var tx = await db.Database.BeginTransactionAsync(Ct);
        await db.ProcessingResults
            .Where(r => r.JobId == batch.JobId && r.BatchNumber == batch.Batch.BatchNumber)
            .ExecuteDeleteAsync(Ct);
        db.ProcessingResults.AddRange(results);
        await db.SaveChangesAsync(Ct);
        await tx.CommitAsync(Ct);
        return new BatchStepResult(results.Count);
    }

    [Activity("processing.cleanup")]
    public async Task<BatchStepResult> CleanupAsync(BatchContext batch)
    {
        var deleted = await db.CategorizationStaging
            .Where(s => s.JobId == batch.JobId && s.BatchNumber == batch.Batch.BatchNumber)
            .ExecuteDeleteAsync(Ct);
        return new BatchStepResult(deleted);
    }

    /// <summary>Artificial long step (parameter "seconds") with heartbeat-based resume.</summary>
    [Activity("processing.delay")]
    public async Task<BatchStepResult> DelayAsync(BatchContext batch)
    {
        var seconds = batch.Parameters.TryGetValue("seconds", out var v) && int.TryParse(v, out var s) ? s : 5;
        await IngestionActivities.HeartbeatingDelayAsync(TimeSpan.FromSeconds(seconds));
        return new BatchStepResult(0);
    }

    [Activity("processing.record-job")]
    public async Task RecordJobAsync(JobUpdate update)
    {
        var now = DateTimeOffset.UtcNow;
        var job = await db.ProcessingJobs.FindAsync([update.JobId], Ct);
        if (job is null)
        {
            job = new ProcessingJob { JobId = update.JobId, StartedAt = now };
            db.ProcessingJobs.Add(job);
        }
        job.SensorType = update.SensorType;
        job.Status = update.Status;
        job.Pipeline = update.Pipeline;
        job.PipelineVersion = update.PipelineVersion;
        job.Processed = update.Processed;
        job.Batches = update.Batches;
        job.FailedBatches = update.FailedBatches;
        job.Error = update.Error;
        job.UpdatedAt = now;
        job.CompletedAt = update.Status == JobStatus.Running ? null : now;
        await db.SaveChangesAsync(Ct);

        if (update.Status != JobStatus.Running)
        {
            // A cancelled or failed job may stop between "categorize" and "cleanup".
            await db.CategorizationStaging.Where(s => s.JobId == update.JobId).ExecuteDeleteAsync(Ct);
        }
    }

    private IQueryable<Measurement> BatchQuery(BatchContext batch)
    {
        var query = db.Measurements.AsNoTracking().Where(m =>
            m.SensorType == batch.SensorType && m.Id >= batch.Batch.FirstId && m.Id <= batch.Batch.LastId);
        return batch.OnlyUncategorized
            ? query.Where(m => m.Category == null || m.CategorizedByJob == batch.JobId)
            : query;
    }

    internal static CategoryThresholds ResolveThresholds(string sensorType, IReadOnlyDictionary<string, string> parameters)
    {
        if (parameters.TryGetValue($"thresholds.{sensorType}", out var configured))
        {
            return CategoryThresholds.Parse(configured);
        }
        return CategoryThresholds.Defaults.TryGetValue(sensorType, out var fallback)
            ? fallback
            : new CategoryThresholds(double.MinValue, double.MaxValue, double.MaxValue);
    }
}
