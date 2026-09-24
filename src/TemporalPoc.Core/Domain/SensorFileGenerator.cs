using System.Globalization;
using System.Text;
using System.Text.Json;

namespace TemporalPoc.Core.Domain;

public enum GeneratedFileKind
{
    Valid,
    Invalid,
    Poison,
    Slow,
}

/// <summary>Generates realistic (and deliberately broken) sensor files for load and chaos tests.</summary>
public static class SensorFileGenerator
{
    private static readonly (string Type, string Unit, double Min, double Max)[] Profiles =
    [
        ("temperature", "C", -10, 50),
        ("temperature", "F", 14, 122),
        ("humidity", "%", 5, 95),
        ("pressure", "hPa", 960, 1060),
        ("pressure", "kPa", 96, 106),
        ("vibration", "mm/s", 0, 30),
        ("co2", "ppm", 300, 2500),
    ];

    public static (string FileName, byte[] Content) Generate(string baseName, GeneratedFileKind kind, int rows, bool json, Random random)
    {
        var fileName = kind switch
        {
            GeneratedFileKind.Poison => $"{baseName}-poison",
            GeneratedFileKind.Slow => $"{baseName}-slow",
            _ => baseName,
        } + (json ? ".json" : ".csv");

        var readings = Enumerable.Range(0, rows).Select(i =>
        {
            var p = Profiles[random.Next(Profiles.Length)];
            var value = Math.Round(p.Min + random.NextDouble() * (p.Max - p.Min), 3);
            return (SensorId: $"{p.Type[..3]}-{random.Next(1, 200):D3}", p.Type, Timestamp: DateTimeOffset.UtcNow.AddMinutes(-random.Next(0, 60 * 24 * 30)), Value: value, p.Unit);
        }).ToList();

        if (kind == GeneratedFileKind.Invalid)
        {
            return (fileName, random.Next(4) switch
            {
                0 => Encoding.UTF8.GetBytes(json ? "{ not json" : "id;type;when\n1;2;3\n"), // broken structure
                1 => Encoding.UTF8.GetBytes(""),                                         // empty file
                2 => Render(readings.Select((r, i) => i == rows / 2 ? r with { Type = "radiation" } : r).ToList(), json), // unknown sensor type
                _ => Render(readings.Select((r, i) => i == 0 ? r with { Unit = "C", Type = "temperature", Value = 999 } : r).ToList(), json), // out of range
            });
        }

        return (fileName, Render(readings, json));
    }

    private static byte[] Render(List<(string SensorId, string Type, DateTimeOffset Timestamp, double Value, string Unit)> readings, bool json)
    {
        if (json)
        {
            return JsonSerializer.SerializeToUtf8Bytes(readings.Select(r => new
            {
                sensorId = r.SensorId,
                sensorType = r.Type,
                timestamp = r.Timestamp.ToString("O"),
                value = r.Value,
                unit = r.Unit,
            }));
        }

        var csv = new StringBuilder("sensor_id,sensor_type,timestamp,value,unit\n");
        foreach (var r in readings)
        {
            csv.Append(r.SensorId).Append(',').Append(r.Type).Append(',')
               .Append(r.Timestamp.ToString("O")).Append(',')
               .Append(r.Value.ToString(CultureInfo.InvariantCulture)).Append(',')
               .Append(r.Unit).Append('\n');
        }
        return Encoding.UTF8.GetBytes(csv.ToString());
    }
}
