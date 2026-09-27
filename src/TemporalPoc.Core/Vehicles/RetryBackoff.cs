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
        var baseMs = Math.Min(max.TotalMilliseconds, initial.TotalMilliseconds * Math.Pow(coefficient, exponent));
        var half = baseMs / 2;
        return TimeSpan.FromMilliseconds(half + random.NextDouble() * half);
    }
}
