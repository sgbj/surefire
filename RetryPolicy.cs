namespace Surefire;

public enum BackoffType
{
    Fixed,
    Exponential
}

public sealed class RetryPolicy
{
    public int MaxAttempts { get; set; } = 1;
    public BackoffType BackoffType { get; set; } = BackoffType.Fixed;
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(5);
    public bool Jitter { get; set; } = true;

    public TimeSpan GetDelay(int attempt)
    {
        if (InitialDelay <= TimeSpan.Zero) return TimeSpan.Zero;
        if (attempt <= 0) return Clamp(InitialDelay);

        if (BackoffType != BackoffType.Exponential)
            return Clamp(InitialDelay);

        var maxShift = 62 - (int)Math.Floor(Math.Log2(Math.Max(InitialDelay.Ticks, 1)));
        var shift = Math.Min(attempt - 1, Math.Max(maxShift, 0));
        var ticks = InitialDelay.Ticks * (1L << shift);
        var delay = TimeSpan.FromTicks(ticks);
        return Clamp(delay);
    }

    private TimeSpan Clamp(TimeSpan delay)
    {
        if (delay > MaxDelay) delay = MaxDelay;

        if (Jitter && delay > TimeSpan.Zero)
        {
            // Equal jitter: random in [50%, 100%] of delay to decorrelate retries across nodes
            delay = TimeSpan.FromTicks((long)(delay.Ticks * (0.5 + Random.Shared.NextDouble() * 0.5)));
        }

        return delay;
    }
}
