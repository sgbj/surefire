using StackExchange.Redis;
using Surefire.Redis;
using Surefire.Tests.Conformance;
using Testcontainers.Redis;

namespace Surefire.Tests.Redis;

internal sealed class RedisFixture : IAsyncLifetime, IStoreTestFixture
{
    private readonly RedisContainer _container = new RedisBuilder()
        .WithImage("redis:7-alpine")
        .Build();

    private ConnectionMultiplexer? _mux;

    private RedisOptions? _options;
    private IServer? _server;
    private RedisJobStore? _store;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        _options = new(_container.GetConnectionString());
        _store = new(_options, TimeProvider.System);
        await _store.MigrateAsync();

        _mux = await ConnectionMultiplexer.ConnectAsync(_container.GetConnectionString() + ",allowAdmin=true");
        _server = _mux.GetServers()[0];
    }

    public async Task DisposeAsync()
    {
        if (_options is { })
        {
            await _options.DisposeAsync();
        }

        if (_mux is { })
        {
            await _mux.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    public Task<IJobStore> CreateStoreAsync() => Task.FromResult<IJobStore>(_store!);

    public async Task CleanAsync()
    {
        await _server!.FlushDatabaseAsync();
    }
}