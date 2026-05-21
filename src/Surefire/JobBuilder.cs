using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace Surefire;

/// <summary>
///     Fluent builder for configuring a job definition during registration.
/// </summary>
public sealed class JobBuilder
{
    private const string CallbackReflectionMessage =
        "Registering a lifecycle callback from a Delegate inspects its parameters and return type " +
        "via reflection. Build a CallbackDescriptor instead when publishing with trimming or Native AOT.";

    private readonly Action _sync;

    internal JobBuilder(JobDefinition definition, Action sync)
    {
        Definition = definition;
        _sync = sync;
    }

    internal JobDefinition Definition { get; }
    internal List<Func<IServiceProvider, IJobFilter>> FilterFactories { get; } = [];
    internal List<CompiledCallback> OnSuccessCallbacks { get; } = [];
    internal List<CompiledCallback> OnRetryCallbacks { get; } = [];
    internal List<CompiledCallback> OnDeadLetterCallbacks { get; } = [];

    /// <summary>
    ///     Sets a cron expression for scheduled execution.
    /// </summary>
    /// <param name="cronExpression">A 5-field or 6-field cron expression.</param>
    /// <param name="timeZoneId">An optional time zone ID. Null uses UTC.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithCron(string cronExpression, string? timeZoneId = null)
    {
        if (string.IsNullOrWhiteSpace(cronExpression))
        {
            throw new ArgumentException("Cron expression cannot be empty.", nameof(cronExpression));
        }

        CronScheduleValidation.Validate(cronExpression, timeZoneId);
        Definition.CronExpression = cronExpression;
        Definition.TimeZoneId = timeZoneId;
        _sync();
        return this;
    }

    /// <summary>
    ///     Sets a description for the job.
    /// </summary>
    /// <param name="description">The description text.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithDescription(string description)
    {
        ArgumentNullException.ThrowIfNull(description);
        Definition.Description = description;
        _sync();
        return this;
    }

    /// <summary>
    ///     Sets tags for the job.
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
        _sync();
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
        _sync();
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
        _sync();
        return this;
    }

    /// <summary>
    ///     Marks this job as continuous. Continuous jobs restart after each run regardless of
    ///     outcome and default to one instance at a time. Combine with <see cref="WithMaxConcurrency" />
    ///     to run multiple in parallel.
    /// </summary>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder Continuous()
    {
        if (Definition.IsDurable)
        {
            throw new InvalidOperationException(
                "Continuous and Durable are mutually exclusive: a durable orchestrator's replay " +
                "semantics are undefined for re-firing on terminal.");
        }

        Definition.IsContinuous = true;
        Definition.MaxConcurrency ??= 1;
        _sync();
        return this;
    }

    /// <summary>
    ///     Marks this job as a durable orchestrator. The handler may call any
    ///     <see cref="IJobClient" /> method; each call is recorded and replayed deterministically
    ///     on resume. While awaiting child runs the orchestrator is suspended and does not consume
    ///     a worker, job, or queue concurrency slot.
    /// </summary>
    /// <remarks>
    ///     Durable handlers must be deterministic: their <see cref="IJobClient" /> call sequence
    ///     must depend only on run arguments and prior call results. Avoid <see cref="DateTime.UtcNow" />,
    ///     <see cref="Random" />, <see cref="Guid.NewGuid" />, and direct IO inside the handler;
    ///     wrap them in a child job instead. Cannot be combined with <see cref="Continuous" />.
    /// </remarks>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder Durable()
    {
        if (Definition.IsContinuous)
        {
            throw new InvalidOperationException(
                "Continuous and Durable are mutually exclusive: a durable orchestrator's replay " +
                "semantics are undefined for re-firing on terminal.");
        }

        Definition.IsDurable = true;
        _sync();
        return this;
    }

    /// <summary>
    ///     Sets the priority for runs of this job. Higher values are claimed first.
    /// </summary>
    /// <param name="priority">The priority value.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithPriority(int priority)
    {
        Definition.Priority = priority;
        _sync();
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
        _sync();
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
        _sync();
        return this;
    }

    /// <summary>
    ///     Sets the maximum number of retries on failure, preserving any previously configured
    ///     backoff settings.
    /// </summary>
    /// <param name="maxRetries">The maximum number of retry attempts. 3 means up to 4 total executions.</param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithRetry(int maxRetries)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxRetries);
        Definition.RetryPolicy = Definition.RetryPolicy with { MaxRetries = maxRetries };
        _sync();
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
        _sync();
        return this;
    }

    /// <summary>
    ///     Sets the misfire policy for this job's cron schedule.
    /// </summary>
    /// <param name="policy">The misfire policy.</param>
    /// <param name="fireAllLimit">
    ///     Optional cap on missed fires to catch up when <paramref name="policy" /> is
    ///     <see cref="Surefire.MisfirePolicy.FireAll" />. Once the cap is reached, older missed
    ///     fires are skipped and scheduling resumes from the present. Null means unlimited.
    /// </param>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder WithMisfirePolicy(MisfirePolicy policy, int? fireAllLimit = null)
    {
        if (fireAllLimit is { } limit && limit < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(fireAllLimit),
                "fireAllLimit must be greater than zero when specified.");
        }

        if (policy != MisfirePolicy.FireAll && fireAllLimit is { })
        {
            throw new ArgumentException("fireAllLimit can only be set when policy is FireAll.",
                nameof(fireAllLimit));
        }

        Definition.MisfirePolicy = policy;
        Definition.FireAllLimit = policy == MisfirePolicy.FireAll ? fireAllLimit : null;
        _sync();
        return this;
    }

    /// <summary>
    ///     Registers a filter for this job. Per-job filters run inside any global filters.
    /// </summary>
    /// <typeparam name="T">The filter type implementing <see cref="IJobFilter" />.</typeparam>
    /// <returns>This builder for chaining.</returns>
    public JobBuilder UseFilter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] T>()
        where T : class, IJobFilter
    {
        FilterFactories.Add(static services => ActivatorUtilities.GetServiceOrCreateInstance<T>(services));
        _sync();
        return this;
    }

    /// <summary>Registers a callback that fires after a successful run.</summary>
    [RequiresUnreferencedCode(CallbackReflectionMessage)]
    [RequiresDynamicCode(CallbackReflectionMessage)]
    public JobBuilder OnSuccess(Delegate callback) => AddDelegate(OnSuccessCallbacks, callback);

    /// <summary>Registers a callback that fires after a failed attempt when a retry is scheduled.</summary>
    [RequiresUnreferencedCode(CallbackReflectionMessage)]
    [RequiresDynamicCode(CallbackReflectionMessage)]
    public JobBuilder OnRetry(Delegate callback) => AddDelegate(OnRetryCallbacks, callback);

    /// <summary>Registers a callback that fires after retries are exhausted.</summary>
    [RequiresUnreferencedCode(CallbackReflectionMessage)]
    [RequiresDynamicCode(CallbackReflectionMessage)]
    public JobBuilder OnDeadLetter(Delegate callback) => AddDelegate(OnDeadLetterCallbacks, callback);

    /// <summary>Registers an on-success callback from a pre-built <see cref="CallbackDescriptor" />.</summary>
    public JobBuilder OnSuccess(CallbackDescriptor descriptor) => AddDescriptor(OnSuccessCallbacks, descriptor);

    /// <summary>Registers an on-retry callback from a pre-built <see cref="CallbackDescriptor" />.</summary>
    public JobBuilder OnRetry(CallbackDescriptor descriptor) => AddDescriptor(OnRetryCallbacks, descriptor);

    /// <summary>Registers an on-dead-letter callback from a pre-built <see cref="CallbackDescriptor" />.</summary>
    public JobBuilder OnDeadLetter(CallbackDescriptor descriptor) => AddDescriptor(OnDeadLetterCallbacks, descriptor);

    [RequiresUnreferencedCode(CallbackReflectionMessage)]
    [RequiresDynamicCode(CallbackReflectionMessage)]
    private JobBuilder AddDelegate(List<CompiledCallback> list, Delegate callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        return AddDescriptor(list, ReflectionDescriptorBuilder.BuildCallback(callback));
    }

    private JobBuilder AddDescriptor(List<CompiledCallback> list, CallbackDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        list.Add(CompiledCallback.FromDescriptor(descriptor));
        _sync();
        return this;
    }
}
