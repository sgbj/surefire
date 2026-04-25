using Microsoft.Extensions.Time.Testing;

namespace Surefire.Tests;

public sealed class InMemoryJobStoreLockTests
{
    private static readonly DateTimeOffset Epoch = new(2025, 6, 15, 10, 0, 0, TimeSpan.Zero);

    private static InMemoryJobStore CreateStore() => new(new FakeTimeProvider(Epoch));

    private static async Task<JobRun> SeedRunAsync(InMemoryJobStore store, string jobName, CancellationToken ct)
    {
        await store.UpsertJobsAsync([new() { Name = jobName }], ct);
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Pending,
            CreatedAt = Epoch,
            NotBefore = Epoch,
            Attempt = 1
        };
        await store.TryCreateRunAsync(run, cancellationToken: ct);
        return run;
    }

    // ── GetRunsAsync — double-enumeration fix ─────────────────────────────────

    [Fact]
    public async Task GetRunsAsync_TotalCount_MatchesFilteredSetNotPagedSet()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = CreateStore();
        var jobName = "RwlsCount_" + Guid.CreateVersion7().ToString("N");

        // Seed 5 runs.
        for (var i = 0; i < 5; i++)
        {
            await SeedRunAsync(store, jobName, ct);
        }

        // Page size 2 — TotalCount must still reflect the full 5, not 2.
        var result = await store.GetRunsAsync(
            new() { JobName = jobName },
            0, 2, ct);

        Assert.Equal(5, result.TotalCount);
        Assert.Equal(2, result.Items.Count);
    }

    [Fact]
    public async Task GetRunsAsync_SecondPage_ReturnsCorrectItems()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = CreateStore();
        var jobName = "RwlsPage_" + Guid.CreateVersion7().ToString("N");

        for (var i = 0; i < 5; i++)
        {
            await SeedRunAsync(store, jobName, ct);
        }

        var page1 = await store.GetRunsAsync(
            new() { JobName = jobName },
            0, 3, ct);
        var page2 = await store.GetRunsAsync(
            new() { JobName = jobName },
            3, 3, ct);

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
        var ct = TestContext.Current.CancellationToken;
        var store = CreateStore();
        var run = await store.GetRunAsync("does-not-exist", ct);
        Assert.Null(run);
    }

    [Fact]
    public async Task CreateAndGetRun_RoundTrip()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = CreateStore();
        var jobName = "RwlsRt_" + Guid.CreateVersion7().ToString("N");
        var run = await SeedRunAsync(store, jobName, ct);

        var fetched = await store.GetRunAsync(run.Id, ct);
        Assert.NotNull(fetched);
        Assert.Equal(run.Id, fetched.Id);
        Assert.Equal(jobName, fetched.JobName);
    }

    // ── Concurrent read/write stress ──────────────────────────────────────────

    [Fact]
    public async Task ConcurrentReadsAndWrites_DoNotDeadlock()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = CreateStore();
        var jobName = "RwlsConcurrent_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName }], ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        var writers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            for (var i = 0; i < 20; i++)
            {
                var run = new JobRun
                {
                    Id = Guid.CreateVersion7().ToString("N"),
                    JobName = jobName,
                    Status = JobStatus.Pending,
                    CreatedAt = Epoch,
                    NotBefore = Epoch,
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
                    new() { JobName = jobName },
                    0, 10, cts.Token);
            }
        }, cts.Token));

        await Task.WhenAll(writers.Concat(readers));
    }
}