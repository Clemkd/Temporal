namespace TemporalPoc.Core.Vehicles;

/// <summary>
/// Exponential backoff with jitter ("equal jitter": half fixed, half random).
/// Temporal's RetryPolicy has no jitter, so the activity computes the delay itself and returns it in
/// ApplicationFailureException.nextRetryDelay; the RetryPolicy still bounds the number of attempts.
/// Jitter spreads the retries of many vehicles hitting the same failing dependency.
/// </summary>
public static class RetryBackoff
{
    /// <param name="attempt">Attempt that just failed (1 = first execution).</param>
    public static TimeSpan Next(int attempt, TimeSpan initial, TimeSpan max, double coefficient, Random random)
    {
        var exponent = Math.Max(0, attempt - 1);
        // initial x coefficient^(attempt-1), capped: 2 s, 4 s, 8 s, 16 s, 32 s... up to max.
        var baseMs = Math.Min(max.TotalMilliseconds, initial.TotalMilliseconds * Math.Pow(coefficient, exponent));
        // Equal jitter: a random delay in [base/2, base]. Keeps a minimum wait (unlike "full jitter" in [0, base])
        // while spreading the retries of many vehicles failing at the same moment.
        var half = baseMs / 2;
        return TimeSpan.FromMilliseconds(half + random.NextDouble() * half);
    }
}
