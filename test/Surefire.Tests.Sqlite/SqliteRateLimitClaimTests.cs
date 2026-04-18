using Microsoft.Data.Sqlite;
using Surefire.Sqlite;

namespace Surefire.Tests.Sqlite;

public sealed class SqliteRateLimitClaimTests
{
    [Fact]
    public async Task ClaimRunAsync_FailedClaimRace_DoesNotConsumeExtraRateLimitPermit()
    {
        var ct = TestContext.Current.CancellationToken;
        var dbPath = Path.Combine(Path.GetTempPath(), $"surefire_rate_limit_{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={dbPath}";

        try
        {
            var storeA = new SqliteJobStore(connectionString, null, TimeProvider.System);
            var storeB = new SqliteJobStore(connectionString, null, TimeProvider.System);
            await storeA.MigrateAsync(ct);

            var jobName = $"job-{Guid.CreateVersion7():N}";
            await storeA.UpsertRateLimitAsync(new()
            {
                Name = "claims",
                Type = RateLimitType.FixedWindow,
                MaxPermits = 2,
                Window = TimeSpan.FromMinutes(1)
            }, ct);
            await storeA.UpsertJobAsync(new()
            {
                Name = jobName,
                Queue = "default",
                RateLimitName = "claims"
            }, ct);
            await storeA.UpsertQueueAsync(new() { Name = "default" }, ct);

            var now = DateTimeOffset.UtcNow;
            await storeA.CreateRunsAsync([
                new()
                {
                    Id = Guid.CreateVersion7().ToString("N"),
                    JobName = jobName,
                    Status = JobStatus.Pending,
                    CreatedAt = now,
                    NotBefore = now,
                    Attempt = 0,
                    Progress = 0
                }
            ], cancellationToken: ct);

            var claimTasks = new[]
            {
                Task.Run(() => storeA.ClaimRunAsync("node-a", [jobName], ["default"], ct), ct),
                Task.Run(() => storeB.ClaimRunAsync("node-b", [jobName], ["default"], ct), ct)
            };
            var claims = await Task.WhenAll(claimTasks);
            Assert.Equal(1, claims.Count(c => c is { }));

            var secondRun = new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = jobName,
                Status = JobStatus.Pending,
                CreatedAt = DateTimeOffset.UtcNow,
                NotBefore = DateTimeOffset.UtcNow,
                Attempt = 0,
                Progress = 0
            };
            await storeA.CreateRunsAsync([secondRun], cancellationToken: ct);

            var secondClaim = await storeA.ClaimRunAsync("node-c", [jobName], ["default"], ct);
            Assert.NotNull(secondClaim);
            Assert.Equal(secondRun.Id, secondClaim!.Id);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            SafeDelete(dbPath);
            SafeDelete($"{dbPath}-wal");
            SafeDelete($"{dbPath}-shm");
            SafeDelete($"{dbPath}-journal");
        }
    }

    [Fact]
    public async Task TryCreateRunAsync_ConcurrentMaxActive1_OnlyOneRunIsCreated()
    {
        var ct = TestContext.Current.CancellationToken;
        var dbPath = Path.Combine(Path.GetTempPath(), $"surefire_max_active_{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={dbPath}";

        try
        {
            var storeA = new SqliteJobStore(connectionString, null, TimeProvider.System);
            var storeB = new SqliteJobStore(connectionString, null, TimeProvider.System);
            await storeA.MigrateAsync(ct);

            var jobName = $"job-{Guid.CreateVersion7():N}";
            await storeA.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

            var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var now = DateTimeOffset.UtcNow;

            var runA = new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = jobName,
                Status = JobStatus.Pending,
                CreatedAt = now,
                NotBefore = now,
                Attempt = 0,
                Progress = 0
            };

            var runB = new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = jobName,
                Status = JobStatus.Pending,
                CreatedAt = now,
                NotBefore = now,
                Attempt = 0,
                Progress = 0
            };

            var createA = Task.Run(async () =>
            {
                await startGate.Task;
                return await storeA.TryCreateRunAsync(runA, 1, cancellationToken: ct);
            }, ct);
            var createB = Task.Run(async () =>
            {
                await startGate.Task;
                return await storeB.TryCreateRunAsync(runB, 1, cancellationToken: ct);
            }, ct);

            startGate.TrySetResult();

            var results = await Task.WhenAll(createA, createB);
            Assert.Equal(1, results.Count(created => created));

            var runs = await storeA.GetRunsAsync(new()
            {
                JobName = jobName,
                ExactJobName = true,
                IsTerminal = false,
                OrderBy = RunOrderBy.CreatedAt
            }, 0, 10, ct);
            Assert.Single(runs.Items);
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            SafeDelete(dbPath);
            SafeDelete($"{dbPath}-wal");
            SafeDelete($"{dbPath}-shm");
            SafeDelete($"{dbPath}-journal");
        }
    }

    private static void SafeDelete(string path)
    {
        if (!File.Exists(path))
        {
            return;
        }

        for (var attempt = 0; attempt < 20; attempt++)
        {
            try
            {
                File.Delete(path);
                return;
            }
            catch (IOException) when (attempt < 19)
            {
                Thread.Sleep(50);
            }
            catch (UnauthorizedAccessException) when (attempt < 19)
            {
                Thread.Sleep(50);
            }
        }

        // Best effort cleanup for temporary test database files.
    }
}