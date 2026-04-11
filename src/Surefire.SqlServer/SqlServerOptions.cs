namespace Surefire.SqlServer;

/// <summary>
///     Configuration options for the SQL Server job store.
/// </summary>
public sealed class SqlServerOptions
{
    /// <summary>
    ///     Gets or sets the SQL Server connection string.
    /// </summary>
    public required string ConnectionString { get; set; }

    /// <summary>
    ///     Gets or sets the default command timeout applied to SQL Server commands.
    /// </summary>
    public TimeSpan? CommandTimeout { get; set; }
}
