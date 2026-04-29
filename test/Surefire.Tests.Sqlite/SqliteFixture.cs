using System.Diagnostics;
using Microsoft.Data.Sqlite;
using Surefire.Sqlite;
using Surefire.Tests.Conformance;

namespace Surefire.Tests.Sqlite;

public sealed class SqliteFixture : IAsyncLifetime, IStoreTestFixture
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"surefire_{Guid.NewGuid():N}.db");
    private SqliteJobStore? _store;

    public async ValueTask InitializeAsync()
    {
        _store = new($"Data Source={_dbPath}", null, TimeProvider.System);
        await _store.MigrateAsync();
    }

    public ValueTask DisposeAsync()
    {
        SqliteConnection.ClearAllPools();

        DeleteFileWithRetry(_dbPath);
        DeleteFileWithRetry($"{_dbPath}-journal");
        DeleteFileWithRetry($"{_dbPath}-wal");
        DeleteFileWithRetry($"{_dbPath}-shm");

        return ValueTask.CompletedTask;
    }

    Task<IJobStore> IStoreTestFixture.CreateStoreAsync()
        => Task.FromResult<IJobStore>(_store!);

    async Task IStoreTestFixture.CleanAsync()
    {
        await using var conn = new SqliteConnection($"Data Source={_dbPath}");
        await conn.OpenAsync();
        await StoreFixtureCleanup.ExecuteDeleteAllAsync(conn, StoreFixtureCleanup.DefaultDeleteAllScript);
    }

    private static void DeleteFileWithRetry(string path)
    {
        if (!File.Exists(path))
        {
            return;
        }

        const int maxAttempts = 20;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                File.Delete(path);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts && ex is IOException or UnauthorizedAccessException)
            {
                Trace.TraceWarning("Retrying SQLite fixture file cleanup for '{0}' (attempt {1}/{2}): {3}",
                    path,
                    attempt,
                    maxAttempts,
                    ex.Message);
                Thread.Sleep(100);
            }
        }

        throw new InvalidOperationException($"Failed to delete SQLite test file '{path}' after retries.");
    }
}
