using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using Surefire.Redis;
using Surefire.Tests.Conformance;
using Testcontainers.Redis;

namespace Surefire.Tests.Redis;

public sealed class RedisFixture : IAsyncLifetime, IStoreTestFixture
{
    private readonly RedisContainer _container = new RedisBuilder()
        .WithImage("redis:7-alpine")
        .Build();

    private ConnectionMultiplexer? _adminConnection;

    private ConnectionMultiplexer? _connection;
    private IServer? _server;
    private RedisJobStore? _store;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        _connection = await ConnectionMultiplexer.ConnectAsync(_container.GetConnectionString());
        _store = new(_connection, TimeProvider.System,
            NullLogger<RedisJobStore>.Instance);
        await _store.MigrateAsync();

        _adminConnection =
            await ConnectionMultiplexer.ConnectAsync(_container.GetConnectionString() + ",allowAdmin=true");
        _server = _adminConnection.GetServers()[0];
    }

    public async ValueTask DisposeAsync()
    {
        if (_adminConnection is { })
        {
            await _adminConnection.DisposeAsync();
        }

        if (_connection is { })
        {
            await _connection.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    Task<IJobStore> IStoreTestFixture.CreateStoreAsync() => Task.FromResult<IJobStore>(_store!);

    async Task IStoreTestFixture.CleanAsync()
    {
        await _server!.FlushDatabaseAsync();
    }
}
