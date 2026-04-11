using Surefire;
using Surefire.SqlServer;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
///     Extension methods for configuring Surefire to use SQL Server.
/// </summary>
public static class SurefireSqlServerExtensions
{
    public static SurefireOptions UseSqlServer(this SurefireOptions options, SqlServerOptions providerOptions)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(providerOptions);
        return options.ConfigureServices(services =>
            services.AddSingleton<IJobStore>(sp =>
                new SqlServerJobStore(providerOptions, sp.GetRequiredService<TimeProvider>())));
    }

    /// <summary>
    ///     Configures Surefire to use SQL Server for job storage.
    /// </summary>
    /// <param name="options">The Surefire options.</param>
    /// <param name="connectionString">The SQL Server connection string.</param>
    /// <returns>The options for chaining.</returns>
    public static SurefireOptions UseSqlServer(this SurefireOptions options, string connectionString)
        => options.UseSqlServer(new SqlServerOptions { ConnectionString = connectionString });
}
