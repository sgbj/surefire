using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Npgsql;
using Surefire;
using Surefire.PostgreSql;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
///     Extension methods for configuring Surefire to use PostgreSQL.
/// </summary>
public static class SurefirePostgreSqlExtensions
{
    /// <summary>
    ///     Configures Surefire to use PostgreSQL for both job storage and notifications.
    ///     Resolves <see cref="NpgsqlDataSource" /> from DI — typically registered by
    ///     <c>builder.AddNpgsqlDataSource("name")</c> (Aspire) or
    ///     <c>services.AddNpgsqlDataSource(...)</c> (<c>Npgsql.DependencyInjection</c>).
    /// </summary>
    /// <param name="options">The Surefire options.</param>
    /// <param name="commandTimeout">
    ///     Optional timeout applied to each command. Defaults to <see cref="NpgsqlCommand" />'s
    ///     own default when null.
    /// </param>
    public static SurefireOptions UsePostgreSql(this SurefireOptions options, TimeSpan? commandTimeout = null)
        => options.UsePostgreSql(static sp => sp.GetRequiredService<NpgsqlDataSource>(), commandTimeout);

    /// <summary>
    ///     Configures Surefire to use PostgreSQL for both job storage and notifications using a factory
    ///     that resolves the <see cref="NpgsqlDataSource" />. Use for keyed DI or custom resolution.
    ///     The factory must return the same instance on each call (typically by resolving a singleton).
    /// </summary>
    public static SurefireOptions UsePostgreSql(this SurefireOptions options,
        Func<IServiceProvider, NpgsqlDataSource> dataSourceFactory, TimeSpan? commandTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(dataSourceFactory);
        return options.ConfigureServices(services =>
        {
            services.RemoveAll<IJobStore>();
            services.AddSingleton<IJobStore>(sp =>
                new PostgreSqlJobStore(dataSourceFactory(sp), commandTimeout,
                    sp.GetRequiredService<TimeProvider>()));
            services.RemoveAll<INotificationProvider>();
            services.AddSingleton<INotificationProvider>(sp =>
                new PostgreSqlNotificationProvider(dataSourceFactory(sp),
                    sp.GetRequiredService<TimeProvider>(),
                    sp.GetRequiredService<Backoff>(),
                    sp.GetRequiredService<ILogger<PostgreSqlNotificationProvider>>()));
        });
    }

    /// <summary>
    ///     Configures Surefire to use PostgreSQL for job storage only, resolving
    ///     <see cref="NpgsqlDataSource" /> from DI.
    /// </summary>
    public static SurefireOptions UsePostgreSqlStore(this SurefireOptions options, TimeSpan? commandTimeout = null)
        => options.UsePostgreSqlStore(static sp => sp.GetRequiredService<NpgsqlDataSource>(), commandTimeout);

    /// <summary>
    ///     Configures Surefire to use PostgreSQL for job storage only using a factory that resolves the
    ///     <see cref="NpgsqlDataSource" />.
    /// </summary>
    public static SurefireOptions UsePostgreSqlStore(this SurefireOptions options,
        Func<IServiceProvider, NpgsqlDataSource> dataSourceFactory, TimeSpan? commandTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(dataSourceFactory);
        return options.ConfigureServices(services =>
        {
            services.RemoveAll<IJobStore>();
            services.AddSingleton<IJobStore>(sp =>
                new PostgreSqlJobStore(dataSourceFactory(sp), commandTimeout,
                    sp.GetRequiredService<TimeProvider>()));
        });
    }

    /// <summary>
    ///     Configures Surefire to use PostgreSQL for notifications only, resolving
    ///     <see cref="NpgsqlDataSource" /> from DI.
    /// </summary>
    public static SurefireOptions UsePostgreSqlNotifications(this SurefireOptions options)
        => options.UsePostgreSqlNotifications(static sp => sp.GetRequiredService<NpgsqlDataSource>());

    /// <summary>
    ///     Configures Surefire to use PostgreSQL for notifications only using a factory that resolves the
    ///     <see cref="NpgsqlDataSource" />.
    /// </summary>
    public static SurefireOptions UsePostgreSqlNotifications(this SurefireOptions options,
        Func<IServiceProvider, NpgsqlDataSource> dataSourceFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(dataSourceFactory);
        return options.ConfigureServices(services =>
        {
            services.RemoveAll<INotificationProvider>();
            services.AddSingleton<INotificationProvider>(sp =>
                new PostgreSqlNotificationProvider(dataSourceFactory(sp),
                    sp.GetRequiredService<TimeProvider>(),
                    sp.GetRequiredService<Backoff>(),
                    sp.GetRequiredService<ILogger<PostgreSqlNotificationProvider>>()));
        });
    }
}