using System.Globalization;
using System.Text;
using System.Text.Json;

namespace TemporalPoc.Core.Domain;

/// <summary>
/// Parses and validates sensor files. Supported formats:
///  - CSV with header "sensor_id,sensor_type,timestamp,value,unit"
///  - JSON array of { sensorId, sensorType, timestamp, value, unit }
/// Readings are normalized to the canonical unit of their sensor type.
/// </summary>
public static class SensorFileParser
{
    public static readonly string[] CsvHeader = ["sensor_id", "sensor_type", "timestamp", "value", "unit"];
    private const int MaxIssues = 50;

    public static string DetectFormat(string fileName) =>
        Path.GetExtension(fileName).ToLowerInvariant() switch
        {
            ".csv" => "csv",
            ".json" => "json",
            _ => "unknown",
        };

    public static ParseResult Parse(Stream content, string format, int maxRows = 100_000)
    {
        return format switch
        {
            "csv" => ParseCsv(content, maxRows),
            "json" => ParseJson(content, maxRows),
            _ => new ParseResult { Format = format, FatalError = $"Unsupported format '{format}'" },
        };
    }

    private static ParseResult ParseCsv(Stream content, int maxRows)
    {
        var result = new ParseResult { Format = "csv" };
        using var reader = new StreamReader(content, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);

        var header = reader.ReadLine();
        if (string.IsNullOrWhiteSpace(header))
        {
            result.FatalError = "Empty file";
            return result;
        }

        var columns = header.Split(',').Select(c => c.Trim().ToLowerInvariant()).ToArray();
        if (!columns.SequenceEqual(CsvHeader))
        {
            result.FatalError = $"Invalid header '{header}', expected '{string.Join(',', CsvHeader)}'";
            return result;
        }

        var lineNumber = 1;
        while (reader.ReadLine() is { } line)
        {
            lineNumber++;
            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }
            if (result.TotalRows >= maxRows)
            {
                result.FatalError = $"Too many rows (max {maxRows})";
                return result;
            }

            var fields = line.Split(',');
            if (fields.Length != CsvHeader.Length)
            {
                AddIssue(result, lineNumber, $"Expected {CsvHeader.Length} fields, got {fields.Length}");
                continue;
            }
            AddRow(result, lineNumber, fields[0], fields[1], fields[2], fields[3], fields[4]);
        }

        if (result.TotalRows == 0)
        {
            result.FatalError = "File contains no data row";
        }
        return result;
    }

    private static ParseResult ParseJson(Stream content, int maxRows)
    {
        var result = new ParseResult { Format = "json" };
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(content);
        }
        catch (JsonException e)
        {
            result.FatalError = $"Invalid JSON: {e.Message}";
            return result;
        }

        using (document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Array)
            {
                result.FatalError = "JSON root must be an array";
                return result;
            }

            var index = 0;
            foreach (var item in document.RootElement.EnumerateArray())
            {
                index++;
                if (result.TotalRows >= maxRows)
                {
                    result.FatalError = $"Too many rows (max {maxRows})";
                    return result;
                }
                if (item.ValueKind != JsonValueKind.Object)
                {
                    AddIssue(result, index, "Item is not an object");
                    continue;
                }
                AddRow(result, index,
                    Str(item, "sensorId"), Str(item, "sensorType"), Str(item, "timestamp"), Str(item, "value"), Str(item, "unit"));
            }
        }

        if (result.TotalRows == 0)
        {
            result.FatalError = "File contains no data row";
        }
        return result;
    }

    private static string Str(JsonElement item, string name) =>
        item.TryGetProperty(name, out var p)
            ? p.ValueKind switch
            {
                JsonValueKind.String => p.GetString() ?? "",
                JsonValueKind.Number => p.GetRawText(),
                _ => "",
            }
            : "";

    private static void AddRow(ParseResult result, int line, string sensorId, string sensorType, string timestamp, string value, string unit)
    {
        sensorId = sensorId.Trim();
        sensorType = sensorType.Trim();
        unit = unit.Trim();

        if (sensorId.Length is 0 or > 64)
        {
            AddIssue(result, line, "sensor_id is required (max 64 chars)");
            return;
        }
        if (!SensorCatalog.Types.TryGetValue(sensorType, out var type))
        {
            AddIssue(result, line, $"Unknown sensor_type '{sensorType}'");
            return;
        }
        if (!DateTimeOffset.TryParse(timestamp.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts))
        {
            AddIssue(result, line, $"Invalid timestamp '{timestamp}'");
            return;
        }
        if (ts > DateTimeOffset.UtcNow.AddDays(1))
        {
            AddIssue(result, line, $"Timestamp '{timestamp}' is in the future");
            return;
        }
        if (!double.TryParse(value.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var raw) || !double.IsFinite(raw))
        {
            AddIssue(result, line, $"Invalid value '{value}'");
            return;
        }
        if (!type.Converters.TryGetValue(unit, out var toCanonical))
        {
            AddIssue(result, line, $"Unit '{unit}' not supported for {type.Name}");
            return;
        }

        var normalized = Math.Round(toCanonical(raw), 4);
        if (normalized < type.Min || normalized > type.Max)
        {
            AddIssue(result, line, $"Value {normalized}{type.CanonicalUnit} outside [{type.Min}, {type.Max}]");
            return;
        }

        result.Readings.Add(new SensorReading(line, sensorId, type.Name, ts, normalized, type.CanonicalUnit));
    }

    private static void AddIssue(ParseResult result, int line, string message)
    {
        result.InvalidRowCount++;
        if (result.Issues.Count < MaxIssues)
        {
            result.Issues.Add(new ValidationIssue(line, message));
        }
    }
}
