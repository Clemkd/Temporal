using Microsoft.EntityFrameworkCore;
using TemporalPoc.Core.Chaos;
using TemporalPoc.Core.Data;

namespace TemporalPoc.Core.Vehicles;

/// <summary>The business processing of one (vehicle, day). Must be idempotent: it can run more than once.</summary>
public interface IVehicleDayProcessor
{
    /// <returns>Number of measurements processed.</returns>
    Task<int> ProcessAsync(string vehicleId, DateOnly day, IReadOnlyList<string> requestIds, int attempt, CancellationToken ct);
}

/// <summary>Records the final failure of a day (after retries, or non retryable error).</summary>
public interface IVehicleDayRunStore
{
    Task RecordFailureAsync(VehicleDayFailure failure, CancellationToken ct);
}

/// <summary>
/// Demo processing: daily aggregates of the measurements whose sensor id is the vehicle id.
/// Result and status are written in ONE transaction (delete + insert): replaying the day after a crash
/// gives the same result, never a duplicate.
/// </summary>
public sealed class MeasurementVehicleDayProcessor(PocDbContext db, ChaosMonkey chaos) : IVehicleDayProcessor
{
    public async Task<int> ProcessAsync(string vehicleId, DateOnly day, IReadOnlyList<string> requestIds, int attempt, CancellationToken ct)
    {
        chaos.MaybeFail("vehicle.process-day");
        if (vehicleId.StartsWith("invalid", StringComparison.OrdinalIgnoreCase))
        {
            throw new VehicleBusinessException($"Vehicle {vehicleId} has no valid configuration");
        }

        // Demo: the day is taken in UTC. With an operating day in another time zone / start hour, compute the
        // bounds with the same rule as VehicleProcessingWorkflow.OperatingDayOf.
        var start = new DateTimeOffset(day.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
        var end = start.AddDays(1);
        var values = await db.Measurements.AsNoTracking()
            .Where(m => m.SensorId == vehicleId && m.Timestamp >= start && m.Timestamp < end)
            .Select(m => m.Value)
            .ToListAsync(ct);

        var now = DateTimeOffset.UtcNow;
        // One transaction for result + status: either both are written or none (a crash in between is rolled back
        // by Postgres, and the retried attempt starts from a clean state).
        await using var tx = await db.Database.BeginTransactionAsync(ct);
        // Delete + insert = replace: processing the same day again never duplicates its result (idempotent).
        await db.VehicleDayResults.Where(r => r.VehicleId == vehicleId && r.Day == day).ExecuteDeleteAsync(ct);
        db.VehicleDayResults.Add(new VehicleDayResult
        {
            VehicleId = vehicleId,
            Day = day,
            MeasurementCount = values.Count,
            Min = values.Count > 0 ? values.Min() : null,
            Max = values.Count > 0 ? values.Max() : null,
            Avg = values.Count > 0 ? Math.Round(values.Average(), 4) : null,
            ComputedAt = now,
        });
        var run = await db.VehicleDayRuns.FindAsync([vehicleId, day], ct);
        if (run is null)
        {
            run = new VehicleDayRun { VehicleId = vehicleId, Day = day };
            db.VehicleDayRuns.Add(run);
        }
        run.Status = VehicleDayStatus.Succeeded;
        run.Attempts = attempt;   // attempts of the LAST processing (1 = succeeded the first time)
        run.Runs++;               // how many times this day has been processed in total
        run.RequestIds = string.Join(',', requestIds);
        run.ErrorType = null;
        run.Error = null;
        run.UpdatedAt = now;
        run.SucceededAt = now;
        await db.SaveChangesAsync(ct);
        await tx.CommitAsync(ct);
        return values.Count;
    }
}

public sealed class EfVehicleDayRunStore(PocDbContext db) : IVehicleDayRunStore
{
    public async Task RecordFailureAsync(VehicleDayFailure failure, CancellationToken ct)
    {
        var run = await db.VehicleDayRuns.FindAsync([failure.VehicleId, failure.Day], ct);
        if (run is null)
        {
            run = new VehicleDayRun { VehicleId = failure.VehicleId, Day = failure.Day };
            db.VehicleDayRuns.Add(run);
        }
        run.Status = VehicleDayStatus.Failed;
        run.Attempts = failure.Attempts;
        run.Runs++;
        run.RequestIds = string.Join(',', failure.RequestIds);
        run.ErrorType = failure.ErrorType;
        run.Error = failure.Message.Length > 2000 ? failure.Message[..2000] : failure.Message;
        run.UpdatedAt = DateTimeOffset.UtcNow;
        await db.SaveChangesAsync(ct);
    }
}
