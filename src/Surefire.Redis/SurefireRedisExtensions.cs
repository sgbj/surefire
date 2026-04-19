using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Surefire;
using Surefire.Redis;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
///     Extension methods for configuring Surefire to use Redis.
/// </summary>
public static class SurefireRedisExtensions
{
    /// <summary>
    ///     Configures Surefire to use Redis for both job storage and notifications.
    ///     Resolves <see cref="IConnectionMultiplexer" /> from DI — typically registered by
    ///     <c>builder.AddRedisClient("name")</c> (Aspire) or
    ///     <c>services.AddSingleton&lt;IConnectionMultiplexer&gt;(...)</c>.
    /// </summary>
    public static SurefireOptions UseRedis(this SurefireOptions options)
        => options.UseRedis(static sp => sp.GetRequiredService<IConnectionMultiplexer>());

    /// <summary>
    ///     Configures Surefire to use Redis for both job storage and notifications using a factory
    ///     that resolves the <see cref="IConnectionMultiplexer" />. Use for keyed DI or custom resolution.
    ///     The factory must return the same instance on each call (typically by resolving a singleton).
    /// </summary>
    public static SurefireOptions UseRedis(this SurefireOptions options,
        Func<IServiceProvider, IConnectionMultiplexer> connectionFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(connectionFactory);
        return options.ConfigureServices(services =>
        {
            services.RemoveAll<IJobStore>();
            services.AddSingleton<IJobStore>(sp =>
                new RedisJobStore(connectionFactory(sp), sp.GetRequiredService<TimeProvider>(),
                    sp.GetRequiredService<ILogger<RedisJobStore>>()));
            services.RemoveAll<INotificationProvider>();
            services.AddSingleton<INotificationProvider>(sp =>
                new RedisNotificationProvider(connectionFactory(sp),
                    sp.GetRequiredService<TimeProvider>(),
                    sp.GetRequiredService<ILogger<RedisNotificationProvider>>()));
        });
    }

    /// <summary>
    ///     Configures Surefire to use Redis for job storage only, resolving
    ///     <see cref="IConnectionMultiplexer" /> from DI.
    /// </summary>
    public static SurefireOptions UseRedisStore(this SurefireOptions options)
        => options.UseRedisStore(static sp => sp.GetRequiredService<IConnectionMultiplexer>());

    /// <summary>
    ///     Configures Surefire to use Redis for job storage only using a factory that resolves the
    ///     <see cref="IConnectionMultiplexer" />.
    /// </summary>
    public static SurefireOptions UseRedisStore(this SurefireOptions options,
        Func<IServiceProvider, IConnectionMultiplexer> connectionFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(connectionFactory);
        return options.ConfigureServices(services =>
        {
            services.RemoveAll<IJobStore>();
            services.AddSingleton<IJobStore>(sp =>
                new RedisJobStore(connectionFactory(sp), sp.GetRequiredService<TimeProvider>(),
                    sp.GetRequiredService<ILogger<RedisJobStore>>()));
        });
    }

    /// <summary>
    ///     Configures Surefire to use Redis for notifications only, resolving
    ///     <see cref="IConnectionMultiplexer" /> from DI.
    /// </summary>
    public static SurefireOptions UseRedisNotifications(this SurefireOptions options)
        => options.UseRedisNotifications(static sp => sp.GetRequiredService<IConnectionMultiplexer>());

    /// <summary>
    ///     Configures Surefire to use Redis for notifications only using a factory that resolves the
    ///     <see cref="IConnectionMultiplexer" />.
    /// </summary>
    public static SurefireOptions UseRedisNotifications(this SurefireOptions options,
        Func<IServiceProvider, IConnectionMultiplexer> connectionFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(connectionFactory);
        return options.ConfigureServices(services =>
        {
            services.RemoveAll<INotificationProvider>();
            services.AddSingleton<INotificationProvider>(sp =>
                new RedisNotificationProvider(connectionFactory(sp),
                    sp.GetRequiredService<TimeProvider>(),
                    sp.GetRequiredService<ILogger<RedisNotificationProvider>>()));
        });
    }
}