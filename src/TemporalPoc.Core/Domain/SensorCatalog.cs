namespace TemporalPoc.Core.Domain;

/// <summary>Known sensor types with their canonical unit, accepted units and physical bounds.</summary>
public static class SensorCatalog
{
    public sealed record SensorType(
        string Name,
        string CanonicalUnit,
        IReadOnlyDictionary<string, Func<double, double>> Converters,
        double Min,
        double Max);

    public static readonly IReadOnlyDictionary<string, SensorType> Types = new Dictionary<string, SensorType>(StringComparer.OrdinalIgnoreCase)
    {
        ["temperature"] = new("temperature", "C", Units(
            ("C", v => v), ("F", v => (v - 32) * 5 / 9), ("K", v => v - 273.15)), -90, 150),
        ["humidity"] = new("humidity", "%", Units(
            ("%", v => v), ("ratio", v => v * 100)), 0, 100),
        ["pressure"] = new("pressure", "hPa", Units(
            ("hPa", v => v), ("Pa", v => v / 100), ("kPa", v => v * 10), ("bar", v => v * 1000)), 300, 1100),
        ["vibration"] = new("vibration", "mm/s", Units(
            ("mm/s", v => v), ("m/s", v => v * 1000), ("in/s", v => v * 25.4)), 0, 200),
        ["co2"] = new("co2", "ppm", Units(
            ("ppm", v => v), ("%", v => v * 10_000)), 0, 10_000),
    };

    public static IEnumerable<string> Names => Types.Keys;

    private static IReadOnlyDictionary<string, Func<double, double>> Units(params (string Unit, Func<double, double> ToCanonical)[] units) =>
        units.ToDictionary(u => u.Unit, u => u.ToCanonical, StringComparer.OrdinalIgnoreCase);
}
