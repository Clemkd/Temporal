using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using Temporalio.Activities;
using Temporalio.Exceptions;
using TemporalPoc.Core.Chaos;
using TemporalPoc.Core.Data;
using TemporalPoc.Core.Domain;
using TemporalPoc.Core.Storage;
using TemporalPoc.Core.Workflows;

namespace TemporalPoc.Core.Activities;

/// <summary>
/// Activities of the file ingestion pipeline. All of them are idempotent: they can be executed again
/// after a crash at any point (after the side effect but before Temporal recorded the completion).
/// </summary>
public sealed class IngestionActivities(IObjectStore store, PocDbContext db, ChaosMonkey chaos, ILogger<IngestionActivities> logger, Temporalio.Client.ITemporalClient client)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private static ActivityExecutionContext Ctx => ActivityExecutionContext.Current;
    private static CancellationToken Ct => Ctx.CancellationToken;

    // ------------------------------------------------------------------ fixed steps

    /// <summary>Moves the file from incoming/ to processing/ so it is no longer visible as a new file.</summary>
    [Activity("ingest.claim")]
    public async Task<FileContext> ClaimAsync(string relativeKey)
    {
        chaos.MaybeFail("ingest.claim");
        var info = Ctx.Info;
        var working = FileLayout.ProcessingKey(relativeKey);
        var claimed = await store.MoveAsync(FileLayout.IncomingKey(relativeKey), working,
            new Dictionary<string, string> { ["status"] = "processing", ["workflow"] = info.WorkflowId ?? "" }, Ct);

        if (!claimed)
        {
            // Not in incoming/ nor processing/: a previous run already finalized it (or it was deleted).
            var finalizedAs =
                await store.StatAsync(FileLayout.ProcessedKey(relativeKey), Ct) is not null ? "processed"
                : await store.StatAsync(FileLayout.InvalidKey(relativeKey), Ct) is not null ? "invalid"
                : "missing";
            logger.LogWarning("File {Key} not found in incoming/ or processing/ ({State})", relativeKey, finalizedAs);
            return new FileContext { RelativeKey = relativeKey, WorkingKey = working, AlreadyFinalized = true, FinalizedAs = finalizedAs };
        }

        var now = DateTimeOffset.UtcNow;
        var file = await db.Files.FindAsync([relativeKey], Ct);
        if (file is null)
        {
            file = new IngestedFile { Key = relativeKey, CreatedAt = now };
            db.Files.Add(file);
        }
        if (file.RunId != info.WorkflowRunId)
        {
            file.Attempts++;
        }
        file.Status = FileStatus.Processing;
        file.WorkflowId = info.WorkflowId;
        file.RunId = info.WorkflowRunId;
        file.FailedStep = null;
        file.Error = null;
        file.CompletedAt = null;
        file.UpdatedAt = now;
        await SaveIgnoringDuplicateAsync();

        return new FileContext { RelativeKey = relativeKey, WorkingKey = working };
    }

    /// <summary>Success: moves the file to processed/ with tags.</summary>
    [Activity("ingest.archive")]
    public async Task ArchiveAsync(FinalizeRequest request)
    {
        chaos.MaybeFail("ingest.archive");
        var ctx = request.Context;
        await store.MoveAsync(ctx.WorkingKey, FileLayout.ProcessedKey(ctx.RelativeKey), new Dictionary<string, string>
        {
            ["status"] = "processed",
            ["pipeline"] = $"{request.Pipeline}:v{request.PipelineVersion}",
            ["rows"] = ctx.StoredCount.ToString(),
            ["checksum"] = ctx.Checksum ?? "",
            ["workflow"] = Ctx.Info.WorkflowId ?? "",
        }, Ct);

        await UpdateFileAsync(ctx.RelativeKey, f =>
        {
            f.Status = FileStatus.Stored;
            f.Pipeline = request.Pipeline;
            f.PipelineVersion = request.PipelineVersion;
            f.StoredCount = ctx.StoredCount;
            f.CompletedAt = DateTimeOffset.UtcNow;
        });
    }

    /// <summary>
    /// Failure (even after retries): compensates partial effects and moves the file to invalid/,
    /// tagged with the reason, plus an error report next to it.
    /// </summary>
    [Activity("ingest.quarantine")]
    public async Task QuarantineAsync(FinalizeRequest request)
    {
        chaos.MaybeFail("ingest.quarantine");
        var ctx = request.Context;

        // Compensation: measurements of an invalid file must not stay in the database.
        await db.Measurements.Where(m => m.FileKey == ctx.RelativeKey).ExecuteDeleteAsync(Ct);
        if (ctx.ConvertedKey is not null)
        {
            await store.DeleteAsync(ctx.ConvertedKey, Ct);
        }

        var report = JsonSerializer.SerializeToUtf8Bytes(new
        {
            file = ctx.RelativeKey,
            workflowId = Ctx.Info.WorkflowId,
            pipeline = $"{request.Pipeline}:v{request.PipelineVersion}",
            failedStep = request.FailedStep,
            errorType = request.ErrorType,
            error = request.Error,
            completedSteps = ctx.CompletedSteps,
            at = DateTimeOffset.UtcNow,
        }, Json);
        await store.PutBytesAsync(FileLayout.ErrorReportKey(ctx.RelativeKey), report, "application/json", null, Ct);

        await store.MoveAsync(ctx.WorkingKey, FileLayout.InvalidKey(ctx.RelativeKey), new Dictionary<string, string>
        {
            ["status"] = "invalid",
            ["reason"] = request.ErrorType ?? "unknown",
            ["step"] = request.FailedStep ?? "unknown",
            ["workflow"] = Ctx.Info.WorkflowId ?? "",
        }, Ct);

        await UpdateFileAsync(ctx.RelativeKey, f =>
        {
            f.Status = FileStatus.Invalid;
            f.Pipeline = request.Pipeline;
            f.PipelineVersion = request.PipelineVersion;
            f.FailedStep = request.FailedStep;
            f.Error = Truncate($"{request.ErrorType}: {request.Error}", 2000);
            f.StoredCount = 0;
            f.CompletedAt = DateTimeOffset.UtcNow;
        });
    }

    // ------------------------------------------------------------------ configurable steps

    /// <summary>S3 retrieval: downloads the file, computes its checksum and detects its format.</summary>
    [Activity("ingest.fetch")]
    public async Task<FileContext> FetchAsync(StepInvocation step)
    {
        chaos.MaybeFail("ingest.fetch");
        var ctx = step.Context;
        var maxBytes = GetLong(step.Parameters, "maxBytes", 50L * 1024 * 1024);

        if (chaos.IsSlow(ctx.RelativeKey))
        {
            await HeartbeatingDelayAsync(TimeSpan.FromSeconds(chaos.Settings.SlowSeconds));
        }

        await using var content = await OpenAsync(ctx.WorkingKey);
        if (content.Length > maxBytes)
        {
            throw new ApplicationFailureException($"File is {content.Length} bytes, max {maxBytes}", "FileTooLarge", nonRetryable: true);
        }
        var hash = Convert.ToHexStringLower(await SHA256.HashDataAsync(content, Ct));
        var format = SensorFileParser.DetectFormat(ctx.RelativeKey);

        await UpdateFileAsync(ctx.RelativeKey, f =>
        {
            f.Checksum = hash;
            f.Format = format;
            f.SizeBytes = content.Length;
        });
        return ctx with { Checksum = hash, SizeBytes = content.Length, Format = format };
    }

    /// <summary>Validates structure and content. Invalid content is a non retryable error.</summary>
    [Activity("ingest.validate")]
    public async Task<FileContext> ValidateAsync(StepInvocation step)
    {
        chaos.MaybeFail("ingest.validate");
        var ctx = step.Context;
        var maxInvalidRatio = GetDouble(step.Parameters, "maxInvalidRowRatio", 0);
        var maxRows = (int)GetLong(step.Parameters, "maxRows", 100_000);

        var result = await ParseAsync(ctx, maxRows);
        if (result.FatalError is not null)
        {
            throw new ApplicationFailureException(result.FatalError, "InvalidFile", nonRetryable: true);
        }

        var ratio = (double)result.InvalidRowCount / result.TotalRows;
        if (result.InvalidRowCount > 0 && ratio > maxInvalidRatio)
        {
            var sample = string.Join(" | ", result.Issues.Take(5).Select(i => $"line {i.LineNumber}: {i.Message}"));
            throw new ApplicationFailureException(
                $"{result.InvalidRowCount}/{result.TotalRows} invalid rows (max ratio {maxInvalidRatio}): {sample}",
                "InvalidRows", nonRetryable: true);
        }

        await UpdateFileAsync(ctx.RelativeKey, f =>
        {
            f.RowCount = result.TotalRows;
            f.InvalidRowCount = result.InvalidRowCount;
        });
        return ctx with { Format = result.Format, RowCount = result.TotalRows, InvalidRowCount = result.InvalidRowCount };
    }

    /// <summary>Converts to canonical units and writes a normalized NDJSON file (converted/).</summary>
    [Activity("ingest.convert")]
    public async Task<FileContext> ConvertAsync(StepInvocation step)
    {
        chaos.MaybeFail("ingest.convert");
        var ctx = step.Context;
        if (chaos.IsPoison(ctx.RelativeKey))
        {
            // Retryable error that never goes away: proves that files are quarantined after the last retry.
            throw new IOException($"Simulated persistent failure while converting {ctx.RelativeKey}");
        }

        var result = await ParseAsync(ctx, int.MaxValue);
        if (result.FatalError is not null)
        {
            throw new ApplicationFailureException(result.FatalError, "InvalidFile", nonRetryable: true);
        }

        var ndjson = new StringBuilder();
        foreach (var reading in result.Readings)
        {
            ndjson.AppendLine(JsonSerializer.Serialize(reading, Json));
        }
        var convertedKey = FileLayout.ConvertedKey(ctx.RelativeKey);
        await store.PutBytesAsync(convertedKey, Encoding.UTF8.GetBytes(ndjson.ToString()), "application/x-ndjson", null, Ct);
        return ctx with { ConvertedKey = convertedKey, RowCount = result.TotalRows, InvalidRowCount = result.InvalidRowCount };
    }

    /// <summary>
    /// Stores the measurements. Idempotent: in one transaction, previous rows of the file are deleted
    /// and all rows are bulk inserted (binary COPY).
    /// </summary>
    [Activity("ingest.store")]
    public async Task<FileContext> StoreAsync(StepInvocation step)
    {
        chaos.MaybeFail("ingest.store");
        var ctx = step.Context;
        var readings = ctx.ConvertedKey is not null
            ? await ReadConvertedAsync(ctx.ConvertedKey)
            : (await ParseAsync(ctx, int.MaxValue)).Readings;

        if (readings.Count == 0)
        {
            throw new ApplicationFailureException("No valid measurement to store", "InvalidFile", nonRetryable: true);
        }

        await using var tx = await db.Database.BeginTransactionAsync(Ct);
        await db.Measurements.Where(m => m.FileKey == ctx.RelativeKey).ExecuteDeleteAsync(Ct);

        var connection = (NpgsqlConnection)db.Database.GetDbConnection();
        await using (var writer = await connection.BeginBinaryImportAsync(
            """COPY measurements ("FileKey", "LineNumber", "SensorId", "SensorType", "Timestamp", "Value", "Unit") FROM STDIN (FORMAT BINARY)""", Ct))
        {
            var i = 0;
            foreach (var r in readings)
            {
                await writer.StartRowAsync(Ct);
                await writer.WriteAsync(ctx.RelativeKey, NpgsqlDbType.Text, Ct);
                await writer.WriteAsync(r.LineNumber, NpgsqlDbType.Integer, Ct);
                await writer.WriteAsync(r.SensorId, NpgsqlDbType.Text, Ct);
                await writer.WriteAsync(r.SensorType, NpgsqlDbType.Text, Ct);
                await writer.WriteAsync(r.Timestamp.ToUniversalTime(), NpgsqlDbType.TimestampTz, Ct);
                await writer.WriteAsync(r.Value, NpgsqlDbType.Double, Ct);
                await writer.WriteAsync(r.Unit, NpgsqlDbType.Text, Ct);
                if (++i % 2000 == 0)
                {
                    Ctx.Heartbeat(i);
                }
            }
            await writer.CompleteAsync(Ct);
        }

        await db.Files.Where(f => f.Key == ctx.RelativeKey)
            .ExecuteUpdateAsync(s => s.SetProperty(f => f.StoredCount, readings.Count).SetProperty(f => f.UpdatedAt, DateTimeOffset.UtcNow), Ct);
        await tx.CommitAsync(Ct);

        return ctx with { StoredCount = readings.Count };
    }

    /// <summary>
    /// Artificial long step (parameter "seconds"). Heartbeats its progress: after a crash the retry
    /// resumes from the last heartbeat instead of starting over.
    /// </summary>
    [Activity("ingest.delay")]
    public async Task<FileContext> DelayAsync(StepInvocation step)
    {
        await HeartbeatingDelayAsync(TimeSpan.FromSeconds(GetLong(step.Parameters, "seconds", 5)));
        return step.Context;
    }

    /// <summary>
    /// Optional step (after store): tells each vehicle present in the file that data arrived for a day
    /// (real time processing). Demo mapping: vehicle id = sensor id. Event id = file + vehicle + day, so a
    /// retried step never produces a second event.
    /// </summary>
    [Activity("ingest.notify-vehicles")]
    public async Task<FileContext> NotifyVehiclesAsync(StepInvocation step)
    {
        var ctx = step.Context;
        var readings = ctx.ConvertedKey is not null ? await ReadConvertedAsync(ctx.ConvertedKey) : (await ParseAsync(ctx, int.MaxValue)).Readings;
        var settings = new Vehicles.VehicleProcessingSettings();
        var zone = TimeZoneInfo.FindSystemTimeZoneById(step.Parameters.GetValueOrDefault("timeZone", settings.OperatingTimeZone));
        // One event per (vehicle, operating day) present in the file, not per row: a file of 10 000 rows for one
        // vehicle and one day sends a single signal.
        var events = readings
            .Select(r => (Vehicle: r.SensorId, Day: Vehicles.VehicleProcessingWorkflow.OperatingDayOf(r.Timestamp.UtcDateTime, zone, settings.OperatingDayStartHour)))
            .Distinct()
            .Select(x => new Vehicles.FileReceivedEvent($"{ctx.RelativeKey}|{x.Vehicle}|{x.Day:yyyy-MM-dd}", x.Vehicle, x.Day, ctx.RelativeKey))
            .ToList();
        var sent = 0;
        await Parallel.ForEachAsync(events, new ParallelOptions { MaxDegreeOfParallelism = 16, CancellationToken = Ct }, async (e, _) =>
        {
            await Vehicles.VehicleWorkflowClient.SendFileReceivedAsync(client, e);
            Ctx.Heartbeat(Interlocked.Increment(ref sent));
        });
        return ctx;
    }

    // ------------------------------------------------------------------ helpers

    internal static async Task HeartbeatingDelayAsync(TimeSpan duration)
    {
        var ctx = ActivityExecutionContext.Current;
        var elapsed = ctx.Info.HeartbeatDetails.Count > 0 ? await ctx.Info.HeartbeatDetailAtAsync<double>(0) : 0d;
        while (elapsed < duration.TotalSeconds)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), ctx.CancellationToken);
            elapsed += 1;
            ctx.Heartbeat(elapsed);
        }
    }

    private async Task<Stream> OpenAsync(string key)
    {
        try
        {
            return await store.OpenReadAsync(key, Ct);
        }
        catch (ObjectNotFoundException)
        {
            throw new ApplicationFailureException($"Object {key} disappeared", "ObjectNotFound", nonRetryable: true);
        }
    }

    private async Task<ParseResult> ParseAsync(FileContext ctx, int maxRows)
    {
        await using var content = await OpenAsync(ctx.WorkingKey);
        var format = ctx.Format ?? SensorFileParser.DetectFormat(ctx.RelativeKey);
        return SensorFileParser.Parse(content, format, maxRows);
    }

    private async Task<List<SensorReading>> ReadConvertedAsync(string key)
    {
        await using var content = await OpenAsync(key);
        using var reader = new StreamReader(content);
        var readings = new List<SensorReading>();
        while (await reader.ReadLineAsync(Ct) is { } line)
        {
            if (line.Length > 0)
            {
                readings.Add(JsonSerializer.Deserialize<SensorReading>(line, Json)!);
            }
        }
        return readings;
    }

    private async Task UpdateFileAsync(string key, Action<IngestedFile> update)
    {
        var file = await db.Files.FindAsync([key], Ct);
        if (file is null)
        {
            return;
        }
        update(file);
        file.UpdatedAt = DateTimeOffset.UtcNow;
        await db.SaveChangesAsync(Ct);
    }

    private async Task SaveIgnoringDuplicateAsync()
    {
        try
        {
            await db.SaveChangesAsync(Ct);
        }
        catch (DbUpdateException e) when (e.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation })
        {
            // Concurrent insert by a previous attempt still running on a dead worker: retry the activity.
            throw new ApplicationFailureException("Concurrent claim, retrying", "ConcurrentClaim");
        }
    }

    private static long GetLong(Dictionary<string, string> parameters, string name, long fallback) =>
        parameters.TryGetValue(name, out var v) && long.TryParse(v, out var parsed) ? parsed : fallback;

    private static double GetDouble(Dictionary<string, string> parameters, string name, double fallback) =>
        parameters.TryGetValue(name, out var v) && double.TryParse(v, System.Globalization.CultureInfo.InvariantCulture, out var parsed) ? parsed : fallback;

    private static string Truncate(string value, int max) => value.Length <= max ? value : value[..max];
}
