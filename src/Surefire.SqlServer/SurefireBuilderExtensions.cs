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
    /// <returns>The options for chaining.</returns>
    public static SurefireOptions UseSqlServer(this SurefireOptions options, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.ConfigureServices(services =>
            services.AddSingleton<IJobStore>(sp =>
                new SqlServerJobStore(connectionString, sp.GetRequiredService<TimeProvider>())));
    }
}