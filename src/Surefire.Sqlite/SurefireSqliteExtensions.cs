using Surefire;
using Surefire.Sqlite;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
///     Extension methods for configuring Surefire to use SQLite.
/// </summary>
public static class SurefireSqliteExtensions
{
    public static SurefireOptions UseSqlite(this SurefireOptions options, SqliteOptions providerOptions)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(providerOptions);
        return options.ConfigureServices(services =>
        {
            services.AddSingleton(providerOptions);
            services.AddSingleton<IJobStore, SqliteJobStore>();
        });
    }

    /// <summary>
    ///     Configures Surefire to use SQLite for job storage.
    /// </summary>
    /// <param name="options">The Surefire options.</param>
    /// <param name="connectionString">The SQLite connection string.</param>
    /// <returns>The options for chaining.</returns>
    public static SurefireOptions UseSqlite(this SurefireOptions options, string connectionString)
        => options.UseSqlite(new SqliteOptions { ConnectionString = connectionString });
}
