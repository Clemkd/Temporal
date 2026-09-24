using TemporalPoc.Core.Domain;
using TemporalPoc.Core.Pipelines;

namespace TemporalPoc.Tests;

public class CategorizationTests
{
    [Theory]
    [InlineData(-5, Categories.Low)]
    [InlineData(20, Categories.Normal)]
    [InlineData(30, Categories.High)]
    [InlineData(50, Categories.Critical)]
    public void Thresholds_categorize_values(double value, string expected)
    {
        Assert.Equal(expected, CategoryThresholds.Parse("5;30;45").Categorize(value));
    }

    [Fact]
    public void Thresholds_must_be_ascending()
    {
        Assert.Throws<FormatException>(() => CategoryThresholds.Parse("30;5;45"));
    }

    [Fact]
    public void Pipeline_validation_rejects_unknown_steps()
    {
        var definition = DefaultPipelines.FileIngestion with
        {
            Steps = [.. DefaultPipelines.FileIngestion.Steps, new StepDefinition { Activity = "processing.categorize" }],
        };
        var errors = StepCatalog.Validate(definition);
        Assert.Single(errors);
        Assert.Empty(StepCatalog.Validate(DefaultPipelines.FileIngestion));
        Assert.Empty(StepCatalog.Validate(DefaultPipelines.SensorProcessing));
    }
}
