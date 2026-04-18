namespace Surefire;

internal sealed class Backoff
{
    private readonly Func<double> _nextDouble;

    public Backoff() : this(Random.Shared.NextDouble)
    {
    }

    public Backoff(Func<double> nextDouble) => _nextDouble = nextDouble;

    public TimeSpan NextDelay(int attempt, TimeSpan initial, TimeSpan max,
        bool jitter = true, BackoffType backoffType = BackoffType.Exponential)
    {
        TimeSpan delay;
        if (backoffType == BackoffType.Fixed || attempt <= 0)
        {
            delay = initial > max ? max : initial;
        }
        else
        {
            var multiplier = Math.Pow(2, attempt);
            var ticks = double.IsInfinity(multiplier) || multiplier > max.Ticks
                ? max.Ticks
                : initial.Ticks * multiplier;
            delay = TimeSpan.FromTicks((long)Math.Min(ticks, max.Ticks));
            if (delay > max)
            {
                delay = max;
            }
        }

        if (jitter && delay > TimeSpan.Zero)
        {
            delay = TimeSpan.FromTicks((long)(delay.Ticks * (0.5 + _nextDouble() * 0.5)));
        }

        return delay > max ? max : delay;
    }
}