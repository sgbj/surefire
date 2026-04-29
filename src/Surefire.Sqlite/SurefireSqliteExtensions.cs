using Microsoft.Extensions.DependencyInjection.Extensions;
using Surefire;
using Surefire.Sqlite;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
///     Extension methods for configuring Surefire to use SQLite.
/// </summary>
public static class SurefireSqliteExtensions
{
    /// <summary>
    ///     Configures Surefire to use SQLite for job storage.
    /// </summary>
    /// <param name="options">The Surefire options.</param>
    /// <param name="connectionString">The SQLite connection string.</param>
    /// <param name="commandTimeout">
    ///     Optional busy-timeout applied to each connection. Defaults to 30 seconds when null.
    /// </param>
    public static SurefireOptions UseSqlite(this SurefireOptions options, string connectionString,
        TimeSpan? commandTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        return options.ConfigureServices(services =>
        {
            services.RemoveAll<IJobStore>();
            services.AddSingleton<IJobStore>(sp =>
                new SqliteJobStore(connectionString, commandTimeout,
                    sp.GetRequiredService<TimeProvider>()));
        });
    }
}
