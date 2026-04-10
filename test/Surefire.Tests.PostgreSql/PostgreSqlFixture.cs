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
    private string _connectionString = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        var csb = new NpgsqlConnectionStringBuilder(_container.GetConnectionString())
        {
            // Keep pool below server max to queue excess callers instead of failing with "too many clients".
            MaxPoolSize = 50
        };
        _connectionString = csb.ConnectionString;

        _options = new(_connectionString);
        _store = new(_options, TimeProvider.System);
        await _store.MigrateAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_options is { })
        {
            await _options.DisposeAsync();
        }

        await _container.DisposeAsync();
    }

    Task<IJobStore> IStoreTestFixture.CreateStoreAsync()
        => Task.FromResult<IJobStore>(_store!);

    async Task IStoreTestFixture.CleanAsync()
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await StoreFixtureCleanup.ExecuteDeleteAllAsync(conn, StoreFixtureCleanup.DefaultDeleteAllScript);
    }
}