using Npgsql;

namespace Surefire.PostgreSql;

/// <summary>
///     Manages a shared NpgsqlDataSource for PostgreSQL stores and notification providers.
/// </summary>
internal sealed class PostgreSqlOptions : IAsyncDisposable
{
    private NpgsqlDataSource? _dataSource;

    internal PostgreSqlOptions(string connectionString)
    {
        ConnectionString = connectionString;
        _dataSource = NpgsqlDataSource.Create(connectionString);
    }

    internal string ConnectionString { get; }
    internal NpgsqlDataSource DataSource => _dataSource ?? throw new ObjectDisposedException(nameof(PostgreSqlOptions));

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _dataSource, null) is { } ds)
        {
            await ds.DisposeAsync();
        }
    }
}