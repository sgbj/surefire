using Microsoft.Extensions.DependencyInjection.Extensions;
using Surefire;
using Surefire.SqlServer;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
///     Extension methods for configuring Surefire to use SQL Server.
/// </summary>
public static class SurefireSqlServerExtensions
{
    /// <summary>
    ///     Configures Surefire to use SQL Server for job storage.
    /// </summary>
    /// <param name="options">The Surefire options.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <param name="commandTimeout">
    ///     Optional timeout applied to each command. Defaults to <see cref="Microsoft.Data.SqlClient.SqlCommand" />'s
    ///     own default (30 seconds) when null.
    /// </param>
    public static SurefireOptions UseSqlServer(this SurefireOptions options, string connectionString,
        TimeSpan? commandTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return options.UseSqlServer(_ => connectionString, commandTimeout);
    }

    /// <summary>
    ///     Configures Surefire to use SQL Server for job storage using a factory that resolves the
    ///     connection string at service-resolution time.
    /// </summary>
    public static SurefireOptions UseSqlServer(this SurefireOptions options,
        Func<IServiceProvider, string> connectionStringFactory, TimeSpan? commandTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(connectionStringFactory);
        return options.ConfigureServices(services =>
        {
            services.RemoveAll<IJobStore>();
            services.AddSingleton<IJobStore>(sp =>
                new SqlServerJobStore(connectionStringFactory(sp), commandTimeout,
                    sp.GetRequiredService<TimeProvider>()));
        });
    }
}