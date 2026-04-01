using StackExchange.Redis;

namespace Surefire.Redis;

/// <summary>
///     Manages a Redis connection with ownership semantics.
///     When created from a connection string, the connection is owned and disposed.
///     When created from an existing multiplexer, the caller retains ownership.
/// </summary>
internal sealed class RedisOptions : IAsyncDisposable
{
    private readonly string? _connectionString;
    private readonly bool _ownsConnection;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private IConnectionMultiplexer? _connection;

    internal RedisOptions(string connectionString)
    {
        _connectionString = connectionString;
        _ownsConnection = true;
    }

    internal RedisOptions(IConnectionMultiplexer connection)
    {
        _connection = connection;
        _ownsConnection = false;
    }

    public async ValueTask DisposeAsync()
    {
        if (_ownsConnection && Interlocked.Exchange(ref _connection, null) is { } conn)
        {
            await conn.CloseAsync();
            conn.Dispose();
        }

        _semaphore.Dispose();
    }

    internal async Task<IConnectionMultiplexer> GetConnectionAsync()
    {
        if (_connection is { })
        {
            return _connection;
        }

        await _semaphore.WaitAsync();
        try
        {
            return _connection ??= await ConnectionMultiplexer.ConnectAsync(_connectionString!);
        }
        finally
        {
            _semaphore.Release();
        }
    }
}