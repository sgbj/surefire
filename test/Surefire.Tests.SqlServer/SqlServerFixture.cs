using Microsoft.Data.SqlClient;
using Surefire.SqlServer;
using Surefire.Tests.Conformance;
using Testcontainers.MsSql;

namespace Surefire.Tests.SqlServer;

public sealed class SqlServerFixture : IAsyncLifetime, IStoreTestFixture
{
    private readonly MsSqlContainer _container = new MsSqlBuilder()
        .WithImage("mcr.microsoft.com/mssql/server:2022-latest")
        .Build();

    private SqlServerJobStore? _store;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        _store = new(_container.GetConnectionString(), null, TimeProvider.System);
        await _store.MigrateAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    Task<IJobStore> IStoreTestFixture.CreateStoreAsync()
        => Task.FromResult<IJobStore>(_store!);

    async Task IStoreTestFixture.CleanAsync()
    {
        await using var conn = new SqlConnection(_container.GetConnectionString());
        await conn.OpenAsync();
        await StoreFixtureCleanup.ExecuteDeleteAllAsync(conn, StoreFixtureCleanup.SqlServerDeleteAllScript);
    }
}