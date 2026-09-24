namespace TemporalPoc.Core.Chaos;

public sealed class ChaosSettings
{
    public const string Section = "Chaos";

    /// <summary>Probability (0..1) that an activity attempt throws a transient error.</summary>
    public double TransientFailureRate { get; set; }

    /// <summary>Files whose name contains this marker fail on every attempt of the convert step (poison file).</summary>
    public string PoisonMarker { get; set; } = "poison";

    /// <summary>Files whose name contains this marker are slow to fetch (heartbeating long activity).</summary>
    public string SlowMarker { get; set; } = "slow";

    public int SlowSeconds { get; set; } = 30;
}

public sealed class TransientChaosException(string message) : Exception(message);

/// <summary>Fault injection used to prove that retries, timeouts and crash recovery work.</summary>
public sealed class ChaosMonkey(ChaosSettings settings)
{
    public ChaosSettings Settings { get; } = settings;

    public void MaybeFail(string activity)
    {
        var rate = Settings.TransientFailureRate;
        if (rate > 0 && Random.Shared.NextDouble() < rate)
        {
            throw new TransientChaosException($"Chaos: simulated transient failure in {activity}");
        }
    }

    public bool IsPoison(string key) => Contains(key, Settings.PoisonMarker);

    public bool IsSlow(string key) => Contains(key, Settings.SlowMarker);

    private static bool Contains(string key, string marker) =>
        !string.IsNullOrEmpty(marker) && Path.GetFileName(key).Contains(marker, StringComparison.OrdinalIgnoreCase);
}
