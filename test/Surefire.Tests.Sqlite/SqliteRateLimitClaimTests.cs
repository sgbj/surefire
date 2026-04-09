using Surefire.Sqlite;

namespace Surefire.Tests.Sqlite;

public sealed class SqliteRateLimitClaimTests
{
    [Fact]
    public async Task ClaimRunAsync_FailedClaimRace_DoesNotConsumeExtraRateLimitPermit()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"surefire_rate_limit_{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={dbPath}";

        try
        {
            var storeA = new SqliteJobStore(new() { ConnectionString = connectionString }, TimeProvider.System);
            var storeB = new SqliteJobStore(new() { ConnectionString = connectionString }, TimeProvider.System);
            await storeA.MigrateAsync();

            var jobName = $"job-{Guid.CreateVersion7():N}";
            await storeA.UpsertRateLimitAsync(new RateLimitDefinition
            {
                Name = "claims",
                Type = RateLimitType.FixedWindow,
                MaxPermits = 2,
                Window = TimeSpan.FromMinutes(1)
            });
            await storeA.UpsertJobAsync(new JobDefinition
            {
                Name = jobName,
                Queue = "default",
                RateLimitName = "claims"
            });
            await storeA.UpsertQueueAsync(new QueueDefinition { Name = "default" });

            var now = DateTimeOffset.UtcNow;
            await storeA.CreateRunsAsync([
                new RunRecord
                {
                    Id = Guid.CreateVersion7().ToString("N"),
                    JobName = jobName,
                    Status = JobStatus.Pending,
                    CreatedAt = now,
                    NotBefore = now,
                    Attempt = 0,
                    Progress = 0
                }
            ]);

            var claimTasks = new[]
            {
                Task.Run(() => storeA.ClaimRunAsync("node-a", [jobName], ["default"])),
                Task.Run(() => storeB.ClaimRunAsync("node-b", [jobName], ["default"]))
            };
            var claims = await Task.WhenAll(claimTasks);
            Assert.Equal(1, claims.Count(c => c is not null));

            var secondRun = new RunRecord
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = jobName,
                Status = JobStatus.Pending,
                CreatedAt = DateTimeOffset.UtcNow,
                NotBefore = DateTimeOffset.UtcNow,
                Attempt = 0,
                Progress = 0
            };
            await storeA.CreateRunsAsync([secondRun]);

            var secondClaim = await storeA.ClaimRunAsync("node-c", [jobName], ["default"]);
            Assert.NotNull(secondClaim);
            Assert.Equal(secondRun.Id, secondClaim!.Id);
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
            SafeDelete(dbPath);
            SafeDelete($"{dbPath}-wal");
            SafeDelete($"{dbPath}-shm");
            SafeDelete($"{dbPath}-journal");
        }
    }

    [Fact]
    public async Task TryCreateRunAsync_ConcurrentMaxActive1_OnlyOneRunIsCreated()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"surefire_max_active_{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={dbPath}";

        try
        {
            var storeA = new SqliteJobStore(new() { ConnectionString = connectionString }, TimeProvider.System);
            var storeB = new SqliteJobStore(new() { ConnectionString = connectionString }, TimeProvider.System);
            await storeA.MigrateAsync();

            var jobName = $"job-{Guid.CreateVersion7():N}";
            await storeA.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

            var startGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var now = DateTimeOffset.UtcNow;

            var runA = new RunRecord
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = jobName,
                Status = JobStatus.Pending,
                CreatedAt = now,
                NotBefore = now,
                Attempt = 0,
                Progress = 0
            };

            var runB = new RunRecord
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
                return await storeA.TryCreateRunAsync(runA, maxActiveForJob: 1);
            });
            var createB = Task.Run(async () =>
            {
                await startGate.Task;
                return await storeB.TryCreateRunAsync(runB, maxActiveForJob: 1);
            });

            startGate.TrySetResult();

            var results = await Task.WhenAll(createA, createB);
            Assert.Equal(1, results.Count(created => created));

            var runs = await storeA.GetRunsAsync(new()
            {
                JobName = jobName,
                ExactJobName = true,
                IsTerminal = false,
                OrderBy = RunOrderBy.CreatedAt
            }, 0, 10);
            Assert.Single(runs.Items);
        }
        finally
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
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
