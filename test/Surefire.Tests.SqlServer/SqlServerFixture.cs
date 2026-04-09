using Microsoft.Data.SqlClient;
using Surefire.SqlServer;
using Surefire.Tests.Conformance;
using Testcontainers.MsSql;

namespace Surefire.Tests.SqlServer;

internal sealed class SqlServerFixture : IAsyncLifetime, IStoreTestFixture
{
    private readonly MsSqlContainer _container = new MsSqlBuilder()
        .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
        .Build();

    private SqlServerJobStore? _store;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        _store = new(_container.GetConnectionString(), TimeProvider.System);
        await _store.MigrateAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    public Task<IJobStore> CreateStoreAsync()
        => Task.FromResult<IJobStore>(_store!);

    public async Task CleanAsync()
    {
        await using var conn = new SqlConnection(_container.GetConnectionString());
        await conn.OpenAsync();
        await StoreFixtureCleanup.ExecuteDeleteAllAsync(conn, StoreFixtureCleanup.SqlServerDeleteAllScript);
    }
}