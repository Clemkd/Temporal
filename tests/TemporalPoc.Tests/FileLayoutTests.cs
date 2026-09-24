using TemporalPoc.Core.Storage;

namespace TemporalPoc.Tests;

public class FileLayoutTests
{
    [Theory]
    [InlineData("incoming/a/b.csv", "a/b.csv")]
    [InlineData("processing/b.csv", "b.csv")]
    [InlineData("other/b.csv", "other/b.csv")]
    public void Relative_strips_the_lifecycle_prefix(string key, string expected) =>
        Assert.Equal(expected, FileLayout.Relative(key));

    [Fact]
    public void Workflow_id_is_deterministic() =>
        Assert.Equal(FileLayout.IngestionWorkflowId("a/b.csv"), FileLayout.IngestionWorkflowId(FileLayout.Relative("incoming/a/b.csv")));
}
