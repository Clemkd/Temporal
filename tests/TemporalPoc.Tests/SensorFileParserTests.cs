using System.Text;
using TemporalPoc.Core.Domain;

namespace TemporalPoc.Tests;

public class SensorFileParserTests
{
    private static ParseResult Parse(string content, string format = "csv") =>
        SensorFileParser.Parse(new MemoryStream(Encoding.UTF8.GetBytes(content)), format);

    [Fact]
    public void Csv_is_parsed_and_normalized_to_canonical_units()
    {
        var result = Parse("""
            sensor_id,sensor_type,timestamp,value,unit
            t-1,temperature,2026-01-01T00:00:00Z,68,F
            p-1,pressure,2026-01-01T00:00:00Z,101.3,kPa
            h-1,humidity,2026-01-01T00:00:00Z,0.5,ratio
            """);

        Assert.Null(result.FatalError);
        Assert.Equal(0, result.InvalidRowCount);
        Assert.Collection(result.Readings,
            r => { Assert.Equal(20, r.Value, 3); Assert.Equal("C", r.Unit); },
            r => { Assert.Equal(1013, r.Value, 3); Assert.Equal("hPa", r.Unit); },
            r => { Assert.Equal(50, r.Value, 3); Assert.Equal("%", r.Unit); });
    }

    [Theory]
    [InlineData("", "Empty file")]
    [InlineData("id;type\n1;2", "Invalid header")]
    [InlineData("sensor_id,sensor_type,timestamp,value,unit\n", "no data row")]
    public void Structural_errors_are_fatal(string content, string expected)
    {
        Assert.Contains(expected, Parse(content).FatalError);
    }

    [Fact]
    public void Invalid_rows_are_reported_with_their_line()
    {
        var result = Parse("""
            sensor_id,sensor_type,timestamp,value,unit
            t-1,temperature,2026-01-01T00:00:00Z,20,C
            t-2,radiation,2026-01-01T00:00:00Z,20,Sv
            t-3,temperature,not-a-date,20,C
            t-4,temperature,2026-01-01T00:00:00Z,999,C
            t-5,temperature,2026-01-01T00:00:00Z,20,parsec
            """);

        Assert.Single(result.Readings);
        Assert.Equal(4, result.InvalidRowCount);
        Assert.Equal([3, 4, 5, 6], result.Issues.Select(i => i.LineNumber));
    }

    [Fact]
    public void Json_is_supported()
    {
        var result = Parse("""[{"sensorId":"c-1","sensorType":"co2","timestamp":"2026-01-01T00:00:00Z","value":0.1,"unit":"%"}]""", "json");
        Assert.Null(result.FatalError);
        Assert.Equal(1000, Assert.Single(result.Readings).Value, 3);
    }

    [Fact]
    public void Generated_valid_files_are_valid_and_invalid_files_are_not()
    {
        var random = new Random(7);
        for (var i = 0; i < 50; i++)
        {
            var json = i % 2 == 0;
            var (_, valid) = SensorFileGenerator.Generate($"f{i}", GeneratedFileKind.Valid, 20, json, random);
            var parsed = SensorFileParser.Parse(new MemoryStream(valid), json ? "json" : "csv");
            Assert.Null(parsed.FatalError);
            Assert.Equal(0, parsed.InvalidRowCount);

            var (_, invalid) = SensorFileGenerator.Generate($"f{i}", GeneratedFileKind.Invalid, 20, json, random);
            var bad = SensorFileParser.Parse(new MemoryStream(invalid), json ? "json" : "csv");
            Assert.True(bad.FatalError is not null || bad.InvalidRowCount > 0);
        }
    }
}
