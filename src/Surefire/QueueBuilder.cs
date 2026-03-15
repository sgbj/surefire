namespace Surefire;

public sealed class QueueBuilder
{
    internal QueueDefinition Definition { get; }

    internal QueueBuilder(string name) => Definition = new QueueDefinition { Name = name };

    public QueueBuilder WithPriority(int priority)
    {
        Definition.Priority = priority;
        return this;
    }

    public QueueBuilder WithMaxConcurrency(int maxConcurrency)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrency, 1);
        Definition.MaxConcurrency = maxConcurrency;
        return this;
    }

    public QueueBuilder WithRateLimit(string rateLimitName)
    {
        Definition.RateLimitName = rateLimitName;
        return this;
    }
}
