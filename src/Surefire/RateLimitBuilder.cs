namespace Surefire;

public sealed class RateLimitBuilder
{
    internal RateLimitDefinition Definition { get; }

    internal RateLimitBuilder(string name) => Definition = new RateLimitDefinition { Name = name };

    public RateLimitBuilder FixedWindow(int maxPermits, TimeSpan window)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxPermits, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        Definition.Type = RateLimitType.FixedWindow;
        Definition.MaxPermits = maxPermits;
        Definition.Window = window;
        return this;
    }

    public RateLimitBuilder SlidingWindow(int maxPermits, TimeSpan window)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxPermits, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        Definition.Type = RateLimitType.SlidingWindow;
        Definition.MaxPermits = maxPermits;
        Definition.Window = window;
        return this;
    }
}
