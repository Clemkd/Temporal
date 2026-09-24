namespace TemporalPoc.Core.Domain;

/// <summary>A normalized measurement (canonical unit).</summary>
public sealed record SensorReading(
    int LineNumber,
    string SensorId,
    string SensorType,
    DateTimeOffset Timestamp,
    double Value,
    string Unit);

public sealed record ValidationIssue(int LineNumber, string Message);

public sealed class ParseResult
{
    public string Format { get; init; } = "csv";
    public List<SensorReading> Readings { get; } = [];
    public List<ValidationIssue> Issues { get; } = [];

    /// <summary>Error making the whole file unusable (bad header, not parseable...).</summary>
    public string? FatalError { get; set; }

    public int InvalidRowCount { get; set; }
    public int TotalRows => Readings.Count + InvalidRowCount;
}
