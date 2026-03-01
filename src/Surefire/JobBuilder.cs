namespace Surefire;

public sealed class JobBuilder
{
    private readonly RegisteredJob registeredJob;

    internal JobBuilder(RegisteredJob registeredJob) => this.registeredJob = registeredJob;

    public JobBuilder WithCron(string cronExpression)
    {
        registeredJob.Definition.CronExpression = cronExpression;
        return this;
    }

    public JobBuilder WithDescription(string description)
    {
        registeredJob.Definition.Description = description;
        return this;
    }

    public JobBuilder WithTags(params string[] tags)
    {
        registeredJob.Definition.Tags = tags;
        return this;
    }

    public JobBuilder WithTimeout(TimeSpan timeout)
    {
        registeredJob.Definition.Timeout = timeout;
        return this;
    }

    public JobBuilder WithMaxConcurrency(int maxConcurrency)
    {
        registeredJob.Definition.MaxConcurrency = maxConcurrency;
        return this;
    }

    public JobBuilder WithRetry(int maxAttempts)
    {
        registeredJob.Definition.RetryPolicy = new RetryPolicy { MaxAttempts = maxAttempts };
        return this;
    }

    public JobBuilder WithRetry(Action<RetryPolicy> configure)
    {
        var policy = new RetryPolicy();
        configure(policy);
        registeredJob.Definition.RetryPolicy = policy;
        return this;
    }

    public JobBuilder OnSuccess(Delegate callback)
    {
        registeredJob.OnSuccessCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }

    public JobBuilder OnFailure(Delegate callback)
    {
        registeredJob.OnFailureCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }

    public JobBuilder OnRetry(Delegate callback)
    {
        registeredJob.OnRetryCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }

    public JobBuilder OnDeadLetter(Delegate callback)
    {
        registeredJob.OnDeadLetterCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }
}
