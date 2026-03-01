using Npgsql;
using Surefire.PostgreSql;
using Surefire.Tests.Conformance;
using Testcontainers.PostgreSql;

namespace Surefire.Tests.PostgreSql;

public sealed class PostgreSqlFixture : IAsyncLifetime, IStoreTestFixture
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder()
        .WithImage("postgres:17-alpine")
        .Build();

    private PostgreSqlOptions? _options;
    private PostgreSqlJobStore? _store;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        _options = new(_container.GetConnectionString());
        _store = new(_options, TimeProvider.System);
        await _store.MigrateAsync();
    }

    public async Task DisposeAsync()
    {
        if (_options is { })
        {
            await _options.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    public Task<IJobStore> CreateStoreAsync()
        => Task.FromResult<IJobStore>(_store!);

    public async Task CleanAsync()
    {
        await using var conn = new NpgsqlConnection(_container.GetConnectionString());
        await conn.OpenAsync();
        await StoreFixtureCleanup.ExecuteDeleteAllAsync(conn, StoreFixtureCleanup.DefaultDeleteAllScript);
    }
}