namespace Surefire.Tests;

public sealed class InMemoryJobStoreRwlsTests
{
    private static InMemoryJobStore CreateStore() => new(TimeProvider.System);

    private static async Task<JobRun> SeedRunAsync(InMemoryJobStore store, string jobName)
    {
        await store.UpsertJobAsync(new JobDefinition { Name = jobName });
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Pending,
            CreatedAt = DateTimeOffset.UtcNow,
            NotBefore = DateTimeOffset.UtcNow,
            Attempt = 1
        };
        await store.TryCreateRunAsync(run);
        return run;
    }

    // ── Dispose ───────────────────────────────────────────────────────────────

    [Fact]
    public void Dispose_DoesNotThrow()
    {
        var store = CreateStore();
        store.Dispose();
    }

    // ── GetRunsAsync — double-enumeration fix ─────────────────────────────────

    [Fact]
    public async Task GetRunsAsync_TotalCount_MatchesFilteredSetNotPagedSet()
    {
        var store = CreateStore();
        var jobName = "RwlsCount_" + Guid.CreateVersion7().ToString("N");

        // Seed 5 runs.
        for (var i = 0; i < 5; i++)
            await SeedRunAsync(store, jobName);

        // Page size 2 — TotalCount must still reflect the full 5, not 2.
        var result = await store.GetRunsAsync(
            new RunFilter { JobName = jobName, ExactJobName = true },
            skip: 0, take: 2);

        Assert.Equal(5, result.TotalCount);
        Assert.Equal(2, result.Items.Count);
    }

    [Fact]
    public async Task GetRunsAsync_SecondPage_ReturnsCorrectItems()
    {
        var store = CreateStore();
        var jobName = "RwlsPage_" + Guid.CreateVersion7().ToString("N");

        for (var i = 0; i < 5; i++)
            await SeedRunAsync(store, jobName);

        var page1 = await store.GetRunsAsync(
            new RunFilter { JobName = jobName, ExactJobName = true },
            skip: 0, take: 3);
        var page2 = await store.GetRunsAsync(
            new RunFilter { JobName = jobName, ExactJobName = true },
            skip: 3, take: 3);

        Assert.Equal(3, page1.Items.Count);
        Assert.Equal(2, page2.Items.Count);
        // No overlap
        var page1Ids = page1.Items.Select(r => r.Id).ToHashSet();
        Assert.DoesNotContain(page2.Items, r => page1Ids.Contains(r.Id));
    }

    // ── Basic read/write under lock ───────────────────────────────────────────

    [Fact]
    public async Task GetRunAsync_ReturnsNull_ForUnknownId()
    {
        var store = CreateStore();
        var run = await store.GetRunAsync("does-not-exist");
        Assert.Null(run);
    }

    [Fact]
    public async Task CreateAndGetRun_RoundTrip()
    {
        var store = CreateStore();
        var jobName = "RwlsRt_" + Guid.CreateVersion7().ToString("N");
        var run = await SeedRunAsync(store, jobName);

        var fetched = await store.GetRunAsync(run.Id);
        Assert.NotNull(fetched);
        Assert.Equal(run.Id, fetched.Id);
        Assert.Equal(jobName, fetched.JobName);
    }

    // ── Concurrent read/write stress ──────────────────────────────────────────

    [Fact]
    public async Task ConcurrentReadsAndWrites_DoNotDeadlock()
    {
        var store = CreateStore();
        var jobName = "RwlsConcurrent_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName });

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var writers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            for (var i = 0; i < 20; i++)
            {
                var run = new JobRun
                {
                    Id = Guid.CreateVersion7().ToString("N"),
                    JobName = jobName,
                    Status = JobStatus.Pending,
                    CreatedAt = DateTimeOffset.UtcNow,
                    NotBefore = DateTimeOffset.UtcNow,
                    Attempt = 1
                };
                await store.TryCreateRunAsync(run, cancellationToken: cts.Token);
            }
        }, cts.Token));

        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            for (var i = 0; i < 20; i++)
            {
                await store.GetRunsAsync(
                    new RunFilter { JobName = jobName, ExactJobName = true },
                    skip: 0, take: 10, cancellationToken: cts.Token);
            }
        }, cts.Token));

        await Task.WhenAll(writers.Concat(readers));
    }
}
