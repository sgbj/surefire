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

    private string _connectionString = null!;

    private NpgsqlDataSource? _dataSource;
    private PostgreSqlJobStore? _store;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        var csb = new NpgsqlConnectionStringBuilder(_container.GetConnectionString())
        {
            MaxPoolSize = 50
        };
        _connectionString = csb.ConnectionString;

        _dataSource = NpgsqlDataSource.Create(_connectionString);
        _store = new(_dataSource, null, TimeProvider.System);
        await _store.MigrateAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_dataSource is { })
        {
            await _dataSource.DisposeAsync();
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