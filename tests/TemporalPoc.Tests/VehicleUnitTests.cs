using Microsoft.Extensions.Logging.Abstractions;
using Temporalio.Exceptions;
using Temporalio.Testing;
using TemporalPoc.Core.Vehicles;

namespace TemporalPoc.Tests;

public class VehicleUnitTests
{
    [Fact]
    public void Backoff_is_exponential_capped_and_jittered()
    {
        var random = new Random(1);
        var initial = TimeSpan.FromSeconds(2);
        var max = TimeSpan.FromMinutes(2);
        for (var attempt = 1; attempt <= 10; attempt++)
        {
            var expected = Math.Min(max.TotalMilliseconds, initial.TotalMilliseconds * Math.Pow(2, attempt - 1));
            var samples = Enumerable.Range(0, 200).Select(_ => RetryBackoff.Next(attempt, initial, max, 2, random).TotalMilliseconds).ToList();
            Assert.All(samples, d => Assert.InRange(d, expected / 2, expected));
            Assert.True(samples.Distinct().Count() > 150, "delays are jittered, not all identical");
        }
    }

    [Theory]
    [InlineData(typeof(TimeoutException), true)]
    [InlineData(typeof(IOException), true)]
    [InlineData(typeof(HttpRequestException), true)]
    [InlineData(typeof(VehicleBusinessException), false)]
    [InlineData(typeof(InvalidCastException), false)]
    [InlineData(typeof(NullReferenceException), false)]
    public void Only_transient_errors_are_retryable(Type type, bool transient)
    {
        var e = type == typeof(VehicleBusinessException) ? new VehicleBusinessException("x") : (Exception)Activator.CreateInstance(type)!;
        Assert.Equal(transient, FailureClassifier.IsTransient(e));
    }

    private sealed class ThrowingProcessor(Exception e) : IVehicleDayProcessor
    {
        public Task<int> ProcessAsync(string vehicleId, DateOnly day, IReadOnlyList<string> requestIds, int attempt, CancellationToken ct) => throw e;
    }

    private sealed class NoStore : IVehicleDayRunStore
    {
        public Task RecordFailureAsync(VehicleDayFailure failure, CancellationToken ct) => Task.CompletedTask;
    }

    private static readonly VehicleDayInput Input = new("veh01", new DateOnly(2026, 1, 1), ["r1"],
        TimeSpan.FromSeconds(2), TimeSpan.FromMinutes(2), 2);

    private static Task<ApplicationFailureException> RunAsync(Exception e, int attempt)
    {
        var activities = new VehicleDayActivities(new ThrowingProcessor(e), new NoStore(), NullLogger<VehicleDayActivities>.Instance);
        var env = new ActivityEnvironment { Info = ActivityEnvironment.DefaultInfo with { Attempt = attempt } };
        return Assert.ThrowsAsync<ApplicationFailureException>(() => env.RunAsync(() => activities.ProcessDayAsync(Input)));
    }

    [Fact]
    public async Task Transient_failure_is_retryable_with_a_jittered_delay()
    {
        var failure = await RunAsync(new TimeoutException("db timeout"), attempt: 3);
        Assert.False(failure.NonRetryable);
        Assert.Equal(VehicleErrorTypes.Transient, failure.ErrorType);
        // attempt 3: base = 2 s * 2^2 = 8 s, equal jitter -> [4 s, 8 s]
        Assert.InRange(failure.NextRetryDelay!.Value.TotalSeconds, 4, 8);
    }

    [Fact]
    public async Task Business_failure_is_not_retryable()
    {
        var failure = await RunAsync(new VehicleBusinessException("unknown configuration"), attempt: 1);
        Assert.True(failure.NonRetryable);
        Assert.Equal(VehicleErrorTypes.Business, failure.ErrorType);
    }

    [Fact]
    public async Task Unexpected_application_failure_is_not_retryable()
    {
        var failure = await RunAsync(new InvalidCastException("bug"), attempt: 1);
        Assert.True(failure.NonRetryable);
        Assert.Equal(VehicleErrorTypes.Application, failure.ErrorType);
    }
}
