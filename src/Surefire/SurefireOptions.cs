using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace Surefire;

/// <summary>
///     Configures global behavior for the Surefire job processing engine.
/// </summary>
public sealed class SurefireOptions
{
    internal List<Delegate> OnSuccessCallbacks { get; } = [];
    internal List<Delegate> OnRetryCallbacks { get; } = [];
    internal List<Delegate> OnDeadLetterCallbacks { get; } = [];
    internal IReadOnlyList<CompiledCallback> CompiledOnSuccessCallbacks { get; private set; } = [];
    internal IReadOnlyList<CompiledCallback> CompiledOnRetryCallbacks { get; private set; } = [];
    internal IReadOnlyList<CompiledCallback> CompiledOnDeadLetterCallbacks { get; private set; } = [];
    internal List<Type> FilterTypes { get; } = [];
    internal List<QueueDefinition> Queues { get; } = [];
    internal List<RateLimitDefinition> RateLimits { get; } = [];
    internal List<Action<IServiceCollection>> ServiceConfigurators { get; } = [];

    /// <summary>
    ///     Gets or sets the name of this processing node. Must be unique per process — two processes
    ///     sharing a <c>NodeName</c> still run correctly but can't be distinguished in the dashboard.
    ///     Defaults to <c>{MachineName}:{ProcessId}:{8-char random}</c>, which is unique across
    ///     replicas, containers, and restarts while remaining human-readable.
    /// </summary>
    public string NodeName
    {
        get;
        set
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException("NodeName cannot be empty.", nameof(value));
            }

            field = value;
        }
    } = $"{Environment.MachineName}:{Environment.ProcessId}:{Guid.NewGuid().ToString("N")[..8]}";

    /// <summary>
    ///     Gets or sets the interval between polling attempts when no notifications are available.
    /// </summary>
    public TimeSpan PollingInterval
    {
        get;
        set
        {
            if (value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "PollingInterval must be greater than zero.");
            }

            field = value;
        }
    } = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     Gets or sets the interval between heartbeat updates sent to the store.
    /// </summary>
    public TimeSpan HeartbeatInterval
    {
        get;
        set
        {
            if (value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value),
                    "HeartbeatInterval must be greater than zero.");
            }

            field = value;
        }
    } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Gets or sets the duration after which a node or entity with no heartbeat is considered inactive.
    /// </summary>
    public TimeSpan InactiveThreshold
    {
        get;
        set
        {
            if (value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value),
                    "InactiveThreshold must be greater than zero.");
            }

            field = value;
        }
    } = TimeSpan.FromMinutes(2);

    /// <summary>
    ///     Gets or sets how long completed runs are retained before being purged.
    ///     Null disables automatic purging.
    /// </summary>
    public TimeSpan? RetentionPeriod
    {
        get;
        set
        {
            if (value is { } retention && retention < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value),
                    "RetentionPeriod must be greater than or equal to zero when specified.");
            }

            field = value;
        }
    } = TimeSpan.FromDays(7);

    /// <summary>
    ///     Gets or sets the interval between retention purge checks.
    /// </summary>
    public TimeSpan RetentionCheckInterval
    {
        get;
        set
        {
            if (value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value),
                    "RetentionCheckInterval must be greater than zero.");
            }

            field = value;
        }
    } = TimeSpan.FromMinutes(5);

    /// <summary>
    ///     Gets or sets the maximum time to wait for running jobs to complete during shutdown.
    /// </summary>
    public TimeSpan ShutdownTimeout
    {
        get;
        set
        {
            if (value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "ShutdownTimeout must be greater than zero.");
            }

            field = value;
        }
    } = TimeSpan.FromSeconds(15);

    /// <summary>
    ///     Gets or sets the JSON serializer options used for all argument and result serialization.
    /// </summary>
    public JsonSerializerOptions SerializerOptions
    {
        get;
        set => field = value ?? throw new ArgumentNullException(nameof(value));
    } = new(JsonSerializerOptions.Web) { AllowOutOfOrderMetadataProperties = true };

    /// <summary>
    ///     Gets or sets the maximum number of runs executing simultaneously on this node.
    ///     Defaults to <c>min(ProcessorCount * 5, 20)</c>. Set to <c>null</c> for unlimited.
    /// </summary>
    public int? MaxNodeConcurrency
    {
        get;
        set
        {
            if (value is { } max && max < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "MaxNodeConcurrency must be greater than zero.");
            }

            field = value;
        }
    } = Math.Min(Environment.ProcessorCount * 5, 20);

    /// <summary>
    ///     Gets or sets whether to run database migrations automatically on startup.
    /// </summary>
    public bool AutoMigrate { get; set; } = true;

    /// <summary>
    ///     Registers a global callback invoked when any job completes successfully.
    /// </summary>
    /// <param name="callback">The callback delegate.</param>
    /// <returns>This instance for chaining.</returns>
    public SurefireOptions OnSuccess(Delegate callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        OnSuccessCallbacks.Add(callback);
        return this;
    }

    /// <summary>
    ///     Registers a global callback invoked when any job transitions to retrying.
    /// </summary>
    /// <param name="callback">The callback delegate.</param>
    /// <returns>This instance for chaining.</returns>
    public SurefireOptions OnRetry(Delegate callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        OnRetryCallbacks.Add(callback);
        return this;
    }

    /// <summary>
    ///     Registers a global callback invoked when any job exhausts all retries.
    /// </summary>
    /// <param name="callback">The callback delegate.</param>
    /// <returns>This instance for chaining.</returns>
    public SurefireOptions OnDeadLetter(Delegate callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        OnDeadLetterCallbacks.Add(callback);
        return this;
    }

    /// <summary>
    ///     Registers a global filter applied to all jobs. Global filters run before per-job filters.
    /// </summary>
    /// <typeparam name="T">The filter type implementing <see cref="IJobFilter" />.</typeparam>
    /// <returns>This instance for chaining.</returns>
    public SurefireOptions UseFilter<T>() where T : class, IJobFilter
    {
        FilterTypes.Add(typeof(T));
        return this;
    }

    /// <summary>
    ///     Registers a named queue with default settings.
    /// </summary>
    /// <param name="name">The unique name of the queue.</param>
    /// <returns>This instance for chaining.</returns>
    public SurefireOptions AddQueue(string name)
    {
        ValidateName(name, nameof(name), "Queue name");
        Queues.Add(new() { Name = name });
        return this;
    }

    /// <summary>
    ///     Registers a named queue with custom configuration.
    /// </summary>
    /// <param name="name">The unique name of the queue.</param>
    /// <param name="configure">An action to configure the queue definition.</param>
    /// <returns>This instance for chaining.</returns>
    public SurefireOptions AddQueue(string name, Action<QueueDefinition> configure)
    {
        ValidateName(name, nameof(name), "Queue name");
        ArgumentNullException.ThrowIfNull(configure);

        var queue = new QueueDefinition { Name = name };
        configure(queue);
        ValidateQueue(queue, nameof(configure));
        Queues.Add(queue);
        return this;
    }

    /// <summary>
    ///     Registers a fixed window rate limiter.
    /// </summary>
    /// <param name="name">The unique name of the rate limit.</param>
    /// <param name="maxPermits">The maximum number of permits allowed within each window.</param>
    /// <param name="window">The duration of each fixed window.</param>
    /// <returns>This instance for chaining.</returns>
    public SurefireOptions AddFixedWindowLimiter(string name, int maxPermits, TimeSpan window)
    {
        ValidateRateLimitArguments(name, maxPermits, window);
        RateLimits.Add(new()
        {
            Name = name,
            Type = RateLimitType.FixedWindow,
            MaxPermits = maxPermits,
            Window = window
        });
        return this;
    }

    /// <summary>
    ///     Registers a sliding window rate limiter.
    /// </summary>
    /// <param name="name">The unique name of the rate limit.</param>
    /// <param name="maxPermits">The maximum number of permits allowed within each window.</param>
    /// <param name="window">The duration of the sliding window.</param>
    /// <returns>This instance for chaining.</returns>
    public SurefireOptions AddSlidingWindowLimiter(string name, int maxPermits, TimeSpan window)
    {
        ValidateRateLimitArguments(name, maxPermits, window);
        RateLimits.Add(new()
        {
            Name = name,
            Type = RateLimitType.SlidingWindow,
            MaxPermits = maxPermits,
            Window = window
        });
        return this;
    }

    /// <summary>
    ///     Registers deferred service configuration that runs during <c>AddSurefire</c> registration.
    /// </summary>
    /// <param name="configureServices">The service registration callback.</param>
    /// <returns>This instance for chaining.</returns>
    public SurefireOptions ConfigureServices(Action<IServiceCollection> configureServices)
    {
        ArgumentNullException.ThrowIfNull(configureServices);
        ServiceConfigurators.Add(configureServices);
        return this;
    }

    internal void Validate()
    {
        if (InactiveThreshold < HeartbeatInterval * 2)
        {
            throw new InvalidOperationException(
                $"InactiveThreshold ({InactiveThreshold}) must be at least 2x HeartbeatInterval ({HeartbeatInterval}) " +
                "to avoid premature stale detection between heartbeat cycles.");
        }

        foreach (var queue in Queues)
        {
            ValidateQueue(queue, nameof(Queues));
        }

        foreach (var rateLimit in RateLimits)
        {
            ValidateRateLimitArguments(rateLimit.Name, rateLimit.MaxPermits, rateLimit.Window);
        }

        ValidateNoDuplicateNames(Queues.Select(q => q.Name), "queue");
        ValidateNoDuplicateNames(RateLimits.Select(r => r.Name), "rate limit");
    }

    internal static void ValidateNoDuplicateNames(IEnumerable<string> names, string kind)
    {
        var seen = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in names)
        {
            if (seen.TryGetValue(name, out var prior))
            {
                if (string.Equals(prior, name, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException($"Duplicate {kind} name '{name}'.");
                }

                throw new InvalidOperationException(
                    $"{kind} names '{prior}' and '{name}' differ only in case; Surefire treats them as " +
                    "distinct, which is almost always a configuration mistake.");
            }

            seen[name] = name;
        }
    }

    internal SurefireOptions Freeze()
    {
        var clone = new SurefireOptions
        {
            NodeName = NodeName,
            PollingInterval = PollingInterval,
            HeartbeatInterval = HeartbeatInterval,
            InactiveThreshold = InactiveThreshold,
            RetentionPeriod = RetentionPeriod,
            RetentionCheckInterval = RetentionCheckInterval,
            ShutdownTimeout = ShutdownTimeout,
            SerializerOptions = new(SerializerOptions),
            MaxNodeConcurrency = MaxNodeConcurrency,
            AutoMigrate = AutoMigrate
        };

        clone.CompiledOnSuccessCallbacks = OnSuccessCallbacks.Select(CompiledCallback.Build).ToArray();
        clone.CompiledOnRetryCallbacks = OnRetryCallbacks.Select(CompiledCallback.Build).ToArray();
        clone.CompiledOnDeadLetterCallbacks = OnDeadLetterCallbacks.Select(CompiledCallback.Build).ToArray();
        clone.FilterTypes.AddRange(FilterTypes);
        clone.Queues.AddRange(Queues.Select(queue => queue.Clone()));
        clone.RateLimits.AddRange(RateLimits.Select(rateLimit => rateLimit.Clone()));
        clone.ServiceConfigurators.AddRange(ServiceConfigurators);
        return clone;
    }

    private static void ValidateRateLimitArguments(string name, int maxPermits, TimeSpan window)
    {
        ValidateName(name, nameof(name), "Rate limit name");
        ArgumentOutOfRangeException.ThrowIfLessThan(maxPermits, 1);
        if (window <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(window), "Rate limit window must be greater than zero.");
        }
    }

    private static void ValidateQueue(QueueDefinition queue, string paramName)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ValidateName(queue.Name, paramName, "Queue name");
        if (queue.MaxConcurrency is { } maxConcurrency && maxConcurrency < 1)
        {
            throw new ArgumentOutOfRangeException(paramName,
                "Queue MaxConcurrency must be greater than zero when specified.");
        }

        if (queue.RateLimitName is { } rateLimitName && string.IsNullOrWhiteSpace(rateLimitName))
        {
            throw new ArgumentException("Queue RateLimitName cannot be empty when specified.", paramName);
        }
    }

    private static void ValidateName(string value, string paramName, string displayName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException($"{displayName} cannot be empty.", paramName);
        }
    }
}