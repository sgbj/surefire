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

    public TimeSpan GetDelay(int attempt)
    {
        if (InitialDelay <= TimeSpan.Zero) return TimeSpan.Zero;
        if (attempt <= 0) return InitialDelay > MaxDelay ? MaxDelay : InitialDelay;

        if (BackoffType != BackoffType.Exponential)
            return InitialDelay > MaxDelay ? MaxDelay : InitialDelay;

        var maxShift = 62 - (int)Math.Floor(Math.Log2(Math.Max(InitialDelay.Ticks, 1)));
        var shift = Math.Min(attempt - 1, Math.Max(maxShift, 0));
        var ticks = InitialDelay.Ticks * (1L << shift);
        var delay = TimeSpan.FromTicks(ticks);
        return delay > MaxDelay ? MaxDelay : delay;
    }
}
