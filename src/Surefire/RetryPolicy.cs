namespace Surefire;

/// <summary>
///     Configures retry behavior for failed job runs.
/// </summary>
public sealed record RetryPolicy
{
    /// <summary>The maximum number of retries after the initial attempt. Zero means no retries.</summary>
    public int MaxRetries { get; init; } = 0;

    /// <summary>The backoff strategy to use between retries.</summary>
    public BackoffType BackoffType { get; init; }

    /// <summary>The delay before the first retry.</summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>The maximum delay between retries.</summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Whether to apply jitter to retry delays to decorrelate concurrent retries.</summary>
    public bool Jitter { get; init; } = true;

    /// <summary>
    ///     Calculates the delay before the specified retry attempt.
    /// </summary>
    /// <param name="attempt">The retry attempt number (1-based for retries; 0 returns the initial delay).</param>
    /// <returns>The delay to wait before retrying.</returns>
    public TimeSpan GetDelay(int attempt)
    {
        if (InitialDelay <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        if (attempt <= 0)
        {
            return Clamp(InitialDelay);
        }

        if (BackoffType != BackoffType.Exponential)
        {
            return Clamp(InitialDelay);
        }

        var maxShift = 62 - (int)Math.Floor(Math.Log2(Math.Max(InitialDelay.Ticks, 1)));
        var shift = Math.Min(attempt - 1, Math.Max(maxShift, 0));
        var ticks = InitialDelay.Ticks * (1L << shift);
        var delay = TimeSpan.FromTicks(ticks);
        return Clamp(delay);
    }

    private TimeSpan Clamp(TimeSpan delay)
    {
        if (delay > MaxDelay)
        {
            delay = MaxDelay;
        }

        if (Jitter && delay > TimeSpan.Zero)
        {
            // Equal jitter: random in [50%, 100%] of delay to decorrelate retries across nodes
            delay = TimeSpan.FromTicks((long)(delay.Ticks * (0.5 + Random.Shared.NextDouble() * 0.5)));
        }

        return delay;
    }
}