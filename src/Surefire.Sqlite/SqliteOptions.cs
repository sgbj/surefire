namespace Surefire.Sqlite;

/// <summary>
///     Configuration options for the SQLite job store.
/// </summary>
public sealed class SqliteOptions
{
    /// <summary>
    ///     Gets or sets the SQLite connection string.
    /// </summary>
    public required string ConnectionString { get; set; }

    /// <summary>
    ///     Gets or sets the default command timeout applied to SQLite commands.
    /// </summary>
    public TimeSpan? CommandTimeout { get; set; }
}
