using Microsoft.Extensions.Logging;
using Surefire;
using Surefire.PostgreSql;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
///     Extension methods for configuring Surefire to use PostgreSQL.
/// </summary>
public static class SurefirePostgreSqlExtensions
{
    public static SurefireOptions UsePostgreSql(this SurefireOptions options, PostgreSqlOptions providerOptions)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(providerOptions);
        return options.ConfigureServices(services =>
        {
            services.AddSingleton(new PostgreSqlRuntimeOptions(providerOptions));
            services.AddSingleton<IJobStore>(sp =>
                new PostgreSqlJobStore(sp.GetRequiredService<PostgreSqlRuntimeOptions>(),
                    sp.GetRequiredService<TimeProvider>()));
            services.AddSingleton<INotificationProvider>(sp =>
                new PostgreSqlNotificationProvider(
                    sp.GetRequiredService<PostgreSqlRuntimeOptions>(),
                    sp.GetRequiredService<ILogger<PostgreSqlNotificationProvider>>()));
        });
    }

    /// <summary>
    ///     Configures Surefire to use PostgreSQL for both job storage and notifications.
    /// </summary>
    /// <param name="options">The Surefire options.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The options for chaining.</returns>
    public static SurefireOptions UsePostgreSql(this SurefireOptions options, string connectionString)
        => options.UsePostgreSql(new PostgreSqlOptions { ConnectionString = connectionString });

    /// <summary>
    ///     Configures Surefire to use PostgreSQL for job storage only.
    /// </summary>
    /// <param name="options">The Surefire options.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The options for chaining.</returns>
    public static SurefireOptions UsePostgreSqlStore(this SurefireOptions options, PostgreSqlOptions providerOptions)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(providerOptions);
        return options.ConfigureServices(services =>
        {
            services.AddSingleton(new PostgreSqlRuntimeOptions(providerOptions));
            services.AddSingleton<IJobStore>(sp =>
                new PostgreSqlJobStore(sp.GetRequiredService<PostgreSqlRuntimeOptions>(),
                    sp.GetRequiredService<TimeProvider>()));
        });
    }

    public static SurefireOptions UsePostgreSqlStore(this SurefireOptions options, string connectionString)
        => options.UsePostgreSqlStore(new PostgreSqlOptions { ConnectionString = connectionString });

    /// <summary>
    ///     Configures Surefire to use PostgreSQL for notifications only.
    /// </summary>
    /// <param name="options">The Surefire options.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The options for chaining.</returns>
    public static SurefireOptions UsePostgreSqlNotifications(this SurefireOptions options, PostgreSqlOptions providerOptions)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(providerOptions);
        return options.ConfigureServices(services =>
        {
            services.AddSingleton(new PostgreSqlRuntimeOptions(providerOptions));
            services.AddSingleton<INotificationProvider>(sp =>
                new PostgreSqlNotificationProvider(sp.GetRequiredService<PostgreSqlRuntimeOptions>(),
                    sp.GetRequiredService<ILogger<PostgreSqlNotificationProvider>>()));
        });
    }

    public static SurefireOptions UsePostgreSqlNotifications(this SurefireOptions options, string connectionString)
        => options.UsePostgreSqlNotifications(new PostgreSqlOptions { ConnectionString = connectionString });
}
