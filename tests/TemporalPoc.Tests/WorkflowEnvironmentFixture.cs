using Temporalio.Testing;

namespace TemporalPoc.Tests;

/// <summary>One local Temporal dev server (downloaded or from TEMPORAL_CLI_PATH) shared by the workflow tests.</summary>
public sealed class WorkflowEnvironmentFixture : IAsyncLifetime
{
    public WorkflowEnvironment Env { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var cli = Environment.GetEnvironmentVariable("TEMPORAL_CLI_PATH");
        Env = await WorkflowEnvironment.StartLocalAsync(new()
        {
            DevServerOptions = new() { ExistingPath = string.IsNullOrEmpty(cli) ? null : cli },
        });
    }

    public async Task DisposeAsync() => await Env.ShutdownAsync();
}

[CollectionDefinition(Name)]
public sealed class WorkflowCollection : ICollectionFixture<WorkflowEnvironmentFixture>
{
    public const string Name = "workflows";
}
