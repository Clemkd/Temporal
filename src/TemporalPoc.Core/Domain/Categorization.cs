using System.Globalization;

namespace TemporalPoc.Core.Domain;

public static class Categories
{
    public const string Low = "LOW";
    public const string Normal = "NORMAL";
    public const string High = "HIGH";
    public const string Critical = "CRITICAL";

    public static readonly string[] All = [Low, Normal, High, Critical];
}

/// <summary>Thresholds for one sensor type (canonical unit).</summary>
public sealed record CategoryThresholds(double Low, double High, double Critical)
{
    public string Categorize(double value) =>
        value >= Critical ? Categories.Critical
        : value >= High ? Categories.High
        : value < Low ? Categories.Low
        : Categories.Normal;

    /// <summary>Parses "low;high;critical" (e.g. "5;30;45").</summary>
    public static CategoryThresholds Parse(string value)
    {
        var parts = value.Split(';', ',').Select(p => double.Parse(p.Trim(), CultureInfo.InvariantCulture)).ToArray();
        if (parts.Length != 3 || !(parts[0] <= parts[1] && parts[1] <= parts[2]))
        {
            throw new FormatException($"Invalid thresholds '{value}', expected 'low;high;critical' in ascending order");
        }
        return new CategoryThresholds(parts[0], parts[1], parts[2]);
    }

    public static readonly IReadOnlyDictionary<string, CategoryThresholds> Defaults = new Dictionary<string, CategoryThresholds>(StringComparer.OrdinalIgnoreCase)
    {
        ["temperature"] = new(5, 30, 45),
        ["humidity"] = new(20, 70, 90),
        ["pressure"] = new(980, 1030, 1050),
        ["vibration"] = new(0.5, 10, 25),
        ["co2"] = new(350, 1000, 2000),
    };
}
