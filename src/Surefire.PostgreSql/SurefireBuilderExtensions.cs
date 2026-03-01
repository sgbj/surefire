using Microsoft.Extensions.Logging;
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
    /// </summary>
    /// <param name="options">The Surefire options.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The options for chaining.</returns>
    public static SurefireOptions UsePostgreSql(this SurefireOptions options, string connectionString)
    {
        return options
            .UsePostgreSqlStore(connectionString)
            .UsePostgreSqlNotifications(connectionString);
    }

    /// <summary>
    ///     Configures Surefire to use PostgreSQL for job storage only.
    /// </summary>
    /// <param name="options">The Surefire options.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The options for chaining.</returns>
    public static SurefireOptions UsePostgreSqlStore(this SurefireOptions options, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(options);
        var providerOptions = new PostgreSqlOptions(connectionString);
        return options.ConfigureServices(services =>
            services.AddSingleton<IJobStore>(sp =>
                new PostgreSqlJobStore(providerOptions, sp.GetRequiredService<TimeProvider>())));
    }

    /// <summary>
    ///     Configures Surefire to use PostgreSQL for notifications only.
    /// </summary>
    /// <param name="options">The Surefire options.</param>
    /// <param name="connectionString">The PostgreSQL connection string.</param>
    /// <returns>The options for chaining.</returns>
    public static SurefireOptions UsePostgreSqlNotifications(this SurefireOptions options, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(options);
        var providerOptions = new PostgreSqlOptions(connectionString);
        return options.ConfigureServices(services =>
            services.AddSingleton<INotificationProvider>(sp =>
                new PostgreSqlNotificationProvider(providerOptions,
                    sp.GetRequiredService<ILogger<PostgreSqlNotificationProvider>>())));
    }
}