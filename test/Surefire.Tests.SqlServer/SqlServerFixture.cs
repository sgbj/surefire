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

    private string _connectionString = null!;
    private SqlServerJobStore? _store;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        // Create a dedicated user database for Surefire. Testcontainers defaults to `master`,
        // but a production-shaped test must exercise the migration path on a real user DB.
        const string dbName = "surefire_tests";
        var masterConnectionString = _container.GetConnectionString();
        await using (var masterConn = new SqlConnection(masterConnectionString))
        {
            await masterConn.OpenAsync();
            await using var createCmd = masterConn.CreateCommand();
            createCmd.CommandText = $"""
                                     IF DB_ID(N'{dbName}') IS NULL
                                         CREATE DATABASE [{dbName}];
                                     """;
            await createCmd.ExecuteNonQueryAsync();
        }

        var builder = new SqlConnectionStringBuilder(masterConnectionString) { InitialCatalog = dbName };
        _connectionString = builder.ConnectionString;

        _store = new(_connectionString, null, TimeProvider.System);
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
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await StoreFixtureCleanup.ExecuteDeleteAllAsync(conn, StoreFixtureCleanup.SqlServerDeleteAllScript);
    }
}