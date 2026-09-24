using System.Text.Json.Serialization;
using TemporalPoc.Api;
using TemporalPoc.Api.Endpoints;
using TemporalPoc.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Services.ConfigureHttpJsonOptions(o => o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.AddOpenApi();
builder.Services.AddProblemDetails();
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(30));

// Bootstrap first: the schema must exist before the workers start polling.
builder.Services.AddHostedService<Bootstrap>();
builder.Services.AddTemporalPoc(builder.Configuration);

// Demo mode (docker compose): generates files and processing jobs automatically.
builder.Services.AddSingleton(builder.Configuration.GetSection(DemoSettings.Section).Get<DemoSettings>() ?? new());
builder.Services.AddHostedService<DemoFeeder>();

var app = builder.Build();

app.MapOpenApi();
app.MapGet("/", () => Results.Redirect("/openapi/v1.json")).ExcludeFromDescription();
app.MapGet("/health", () => Results.Ok(new { status = "ok", pid = Environment.ProcessId }));

app.MapFileEndpoints();
app.MapWatcherEndpoints();
app.MapPipelineEndpoints();
app.MapProcessingEndpoints();
app.MapOperationsEndpoints();

app.Run();

public partial class Program;
