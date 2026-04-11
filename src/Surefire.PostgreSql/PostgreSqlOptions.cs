namespace Surefire.PostgreSql;

/// <summary>
///     Configuration options for PostgreSQL storage and notifications.
/// </summary>
public sealed class PostgreSqlOptions
{
    /// <summary>
    ///     Gets or sets the PostgreSQL connection string.
    /// </summary>
    public required string ConnectionString { get; set; }

    /// <summary>
    ///     Gets or sets the default command timeout applied to PostgreSQL commands.
    /// </summary>
    public TimeSpan? CommandTimeout { get; set; }
}
