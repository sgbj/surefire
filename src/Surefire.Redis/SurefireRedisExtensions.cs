using StackExchange.Redis;
using Surefire;
using Surefire.Redis;
using Microsoft.Extensions.Logging;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
///     Extension methods for configuring Surefire to use Redis.
/// </summary>
public static class SurefireRedisExtensions
{
    /// <summary>
    ///     Configures Surefire to use Redis for both job storage and notifications.
    ///     A single connection is shared between the store and notification provider.
    /// </summary>
    public static SurefireOptions UseRedis(this SurefireOptions options, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(options);
        var providerOptions = new RedisOptions(connectionString);
        return options.ConfigureServices(services =>
        {
            services.AddSingleton<IJobStore>(sp =>
                new RedisJobStore(providerOptions, sp.GetRequiredService<TimeProvider>()));
            services.AddSingleton<INotificationProvider>(sp =>
                new RedisNotificationProvider(
                    providerOptions,
                    sp.GetRequiredService<ILogger<RedisNotificationProvider>>()));
        });
    }

    /// <summary>
    ///     Configures Surefire to use Redis for both job storage and notifications
    ///     using an existing connection multiplexer. The caller retains ownership of the multiplexer.
    /// </summary>
    public static SurefireOptions UseRedis(this SurefireOptions options, IConnectionMultiplexer multiplexer)
    {
        ArgumentNullException.ThrowIfNull(options);
        var providerOptions = new RedisOptions(multiplexer);
        return options.ConfigureServices(services =>
        {
            services.AddSingleton<IJobStore>(sp =>
                new RedisJobStore(providerOptions, sp.GetRequiredService<TimeProvider>()));
            services.AddSingleton<INotificationProvider>(sp =>
                new RedisNotificationProvider(
                    providerOptions,
                    sp.GetRequiredService<ILogger<RedisNotificationProvider>>()));
        });
    }

    /// <summary>
    ///     Configures Surefire to use Redis for job storage only.
    /// </summary>
    public static SurefireOptions UseRedisStore(this SurefireOptions options, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(options);
        var providerOptions = new RedisOptions(connectionString);
        return options.ConfigureServices(services =>
            services.AddSingleton<IJobStore>(sp =>
                new RedisJobStore(providerOptions, sp.GetRequiredService<TimeProvider>())));
    }

    /// <summary>
    ///     Configures Surefire to use Redis for job storage only using an existing connection multiplexer.
    /// </summary>
    public static SurefireOptions UseRedisStore(this SurefireOptions options, IConnectionMultiplexer multiplexer)
    {
        ArgumentNullException.ThrowIfNull(options);
        var providerOptions = new RedisOptions(multiplexer);
        return options.ConfigureServices(services =>
            services.AddSingleton<IJobStore>(sp =>
                new RedisJobStore(providerOptions, sp.GetRequiredService<TimeProvider>())));
    }

    /// <summary>
    ///     Configures Surefire to use Redis for notifications only.
    /// </summary>
    public static SurefireOptions UseRedisNotifications(this SurefireOptions options, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(options);
        var providerOptions = new RedisOptions(connectionString);
        return options.ConfigureServices(services =>
            services.AddSingleton<INotificationProvider>(sp =>
                new RedisNotificationProvider(
                    providerOptions,
                    sp.GetRequiredService<ILogger<RedisNotificationProvider>>())));
    }

    /// <summary>
    ///     Configures Surefire to use Redis for notifications only using an existing connection multiplexer.
    /// </summary>
    public static SurefireOptions UseRedisNotifications(this SurefireOptions options,
        IConnectionMultiplexer multiplexer)
    {
        ArgumentNullException.ThrowIfNull(options);
        var providerOptions = new RedisOptions(multiplexer);
        return options.ConfigureServices(services =>
            services.AddSingleton<INotificationProvider>(sp =>
                new RedisNotificationProvider(
                    providerOptions,
                    sp.GetRequiredService<ILogger<RedisNotificationProvider>>())));
    }
}