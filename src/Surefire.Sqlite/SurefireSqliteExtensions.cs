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
    /// <returns>The options for chaining.</returns>
    public static SurefireOptions UseSqlite(this SurefireOptions options, string connectionString)
    {
        ArgumentNullException.ThrowIfNull(options);
        return options.ConfigureServices(services =>
        {
            services.AddSingleton(new SqliteOptions { ConnectionString = connectionString });
            services.AddSingleton<IJobStore, SqliteJobStore>();
        });
    }
}