namespace Surefire;

public sealed class JobBuilder
{
    private readonly RegisteredJob _registeredJob;

    internal JobBuilder(RegisteredJob registeredJob) => _registeredJob = registeredJob;

    public JobBuilder WithCron(string cronExpression)
    {
        _registeredJob.Definition.CronExpression = cronExpression;
        return this;
    }

    public JobBuilder WithDescription(string description)
    {
        _registeredJob.Definition.Description = description;
        return this;
    }

    public JobBuilder WithTags(params string[] tags)
    {
        _registeredJob.Definition.Tags = tags;
        return this;
    }

    public JobBuilder WithTimeout(TimeSpan timeout)
    {
        _registeredJob.Definition.Timeout = timeout;
        return this;
    }

    public JobBuilder WithMaxConcurrency(int maxConcurrency)
    {
        _registeredJob.Definition.MaxConcurrency = maxConcurrency;
        return this;
    }

    public JobBuilder WithRetry(int maxAttempts)
    {
        _registeredJob.Definition.RetryPolicy = new RetryPolicy { MaxAttempts = maxAttempts };
        return this;
    }

    public JobBuilder WithRetry(Action<RetryPolicy> configure)
    {
        var policy = new RetryPolicy();
        configure(policy);
        _registeredJob.Definition.RetryPolicy = policy;
        return this;
    }

    public JobBuilder OnSuccess(Delegate callback)
    {
        _registeredJob.OnSuccessCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }

    public JobBuilder OnFailure(Delegate callback)
    {
        _registeredJob.OnFailureCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }

    public JobBuilder OnRetry(Delegate callback)
    {
        _registeredJob.OnRetryCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }

    public JobBuilder OnDeadLetter(Delegate callback)
    {
        _registeredJob.OnDeadLetterCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }
}
