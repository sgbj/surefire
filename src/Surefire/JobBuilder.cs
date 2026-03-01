namespace Surefire;

/// <summary>
///     Fluent builder for configuring a job definition during registration.
/// </summary>
public sealed class JobBuilder
{
    internal JobBuilder(JobDefinition definition) => Definition = definition;
    internal JobDefinition Definition { get; }
    internal List<Type> FilterTypes { get; } = [];
    internal List<Delegate> OnSuccessCallbacks { get; } = [];
    internal List<Delegate> OnRetryCallbacks { get; } = [];
    internal List<Delegate> OnDeadLetterCallbacks { get; } = [];

    /// <summary>
    ///     Sets a cron expression for scheduled execution.
    /// </summary>
    /// <param name="cronExpression">A cron expression (5 or 6 fields).</param>
    /// <param name="timeZoneId">An optional IANA time zone ID. Null uses UTC.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithCron(string cronExpression, string? timeZoneId = null)
    {
        if (string.IsNullOrWhiteSpace(cronExpression))
        {
            throw new ArgumentException("Cron expression cannot be empty.", nameof(cronExpression));
        }

        Definition.CronExpression = cronExpression;
        Definition.TimeZoneId = timeZoneId;
        return this;
    }

    /// <summary>
    ///     Sets a human-readable description for the job.
    /// </summary>
    /// <param name="description">The description text.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithDescription(string description)
    {
        ArgumentNullException.ThrowIfNull(description);
        Definition.Description = description;
        return this;
    }

    /// <summary>
    ///     Sets categorization tags for the job.
    /// </summary>
    /// <param name="tags">One or more tags.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithTags(params string[] tags)
    {
        ArgumentNullException.ThrowIfNull(tags);
        if (tags.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException("Tags cannot contain null, empty, or whitespace values.", nameof(tags));
        }

        Definition.Tags = tags;
        return this;
    }

    /// <summary>
    ///     Sets a hard timeout per execution attempt.
    /// </summary>
    /// <param name="timeout">The maximum execution duration.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithTimeout(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be greater than zero.");
        }

        Definition.Timeout = timeout;
        return this;
    }

    /// <summary>
    ///     Sets the maximum number of concurrent instances of this job across all nodes.
    /// </summary>
    /// <param name="maxConcurrency">The concurrency limit.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithMaxConcurrency(int maxConcurrency)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrency, 1);
        Definition.MaxConcurrency = maxConcurrency;
        return this;
    }

    /// <summary>
    ///     Marks this job as continuous. A continuous job automatically restarts on completion.
    ///     Sets <see cref="JobDefinition.MaxConcurrency" /> to 1 if not already configured.
    /// </summary>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder Continuous()
    {
        Definition.IsContinuous = true;
        Definition.MaxConcurrency ??= 1;
        return this;
    }

    /// <summary>
    ///     Sets the default priority for runs of this job. Higher values are claimed first.
    /// </summary>
    /// <param name="priority">The priority value.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithPriority(int priority)
    {
        Definition.Priority = priority;
        return this;
    }

    /// <summary>
    ///     Assigns this job to a named queue.
    /// </summary>
    /// <param name="queue">The queue name.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithQueue(string queue)
    {
        if (string.IsNullOrWhiteSpace(queue))
        {
            throw new ArgumentException("Queue name cannot be empty.", nameof(queue));
        }

        Definition.Queue = queue;
        return this;
    }

    /// <summary>
    ///     Associates this job with a named rate limiter.
    /// </summary>
    /// <param name="rateLimitName">The rate limit name.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithRateLimit(string rateLimitName)
    {
        if (string.IsNullOrWhiteSpace(rateLimitName))
        {
            throw new ArgumentException("Rate limit name cannot be empty.", nameof(rateLimitName));
        }

        Definition.RateLimitName = rateLimitName;
        return this;
    }

    /// <summary>
    ///     Configures the job to retry on failure with the specified maximum number of retries.
    /// </summary>
    /// <param name="maxRetries">The maximum number of retry attempts. 3 means up to 4 total executions.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithRetry(int maxRetries)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxRetries);
        Definition.RetryPolicy = new() { MaxRetries = maxRetries };
        return this;
    }

    /// <summary>
    ///     Configures the retry policy with detailed settings.
    /// </summary>
    /// <param name="configure">An action to configure a <see cref="RetryPolicyBuilder" />.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithRetry(Action<RetryPolicyBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var policyBuilder = new RetryPolicyBuilder();
        configure(policyBuilder);
        Definition.RetryPolicy = policyBuilder.Build();
        return this;
    }

    /// <summary>
    ///     Sets the misfire policy for this job's cron schedule.
    /// </summary>
    /// <param name="policy">The misfire policy.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithMisfirePolicy(MisfirePolicy policy)
    {
        Definition.MisfirePolicy = policy;
        return this;
    }

    /// <summary>
    ///     Registers a filter for this job. Per-job filters run after global filters.
    /// </summary>
    /// <typeparam name="T">The filter type implementing <see cref="IJobFilter" />.</typeparam>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder UseFilter<T>() where T : class, IJobFilter
    {
        FilterTypes.Add(typeof(T));
        return this;
    }

    /// <summary>
    ///     Registers a callback invoked when this job completes successfully.
    /// </summary>
    /// <param name="callback">The callback delegate.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder OnSuccess(Delegate callback)
    {
        OnSuccessCallbacks.Add(callback);
        return this;
    }

    /// <summary>
    ///     Registers a callback invoked when this job transitions to retrying.
    /// </summary>
    /// <param name="callback">The callback delegate.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder OnRetry(Delegate callback)
    {
        OnRetryCallbacks.Add(callback);
        return this;
    }

    /// <summary>
    ///     Registers a callback invoked when this job exhausts all retries.
    /// </summary>
    /// <param name="callback">The callback delegate.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder OnDeadLetter(Delegate callback)
    {
        OnDeadLetterCallbacks.Add(callback);
        return this;
    }
}