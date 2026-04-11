using Npgsql;

namespace Surefire.PostgreSql;

/// <summary>
///     Manages a shared NpgsqlDataSource for PostgreSQL stores and notification providers.
/// </summary>
internal sealed class PostgreSqlRuntimeOptions : IAsyncDisposable
{
    private NpgsqlDataSource? _dataSource;

    internal PostgreSqlRuntimeOptions(PostgreSqlOptions options)
    {
        ConnectionString = options.ConnectionString;
        CommandTimeoutSeconds = CommandTimeouts.ToSeconds(options.CommandTimeout, nameof(options.CommandTimeout));

        var builder = new NpgsqlConnectionStringBuilder(options.ConnectionString);
        if (CommandTimeoutSeconds is { } timeoutSeconds)
        {
            builder.CommandTimeout = timeoutSeconds;
        }

        _dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
    }

    internal string ConnectionString { get; }
    internal int? CommandTimeoutSeconds { get; }
    internal NpgsqlDataSource DataSource => _dataSource ?? throw new ObjectDisposedException(nameof(PostgreSqlRuntimeOptions));

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _dataSource, null) is { } ds)
        {
            await ds.DisposeAsync();
        }
    }
}
