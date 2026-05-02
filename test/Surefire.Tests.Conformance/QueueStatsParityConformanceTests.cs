namespace Surefire.Tests.Conformance;

public abstract class QueueStatsParityConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task GetQueueStats_IncludesImplicitDefaultQueue_WhenRunsExist()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ImplicitDefault_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var pending = CreateRun(jobName);
        var running = CreateRun(jobName);
        await Store.CreateRunsAsync([pending, running], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        var stats = await Store.GetQueueStatsAsync(ct);

        Assert.True(stats.ContainsKey("default"));
        Assert.Equal(1, stats["default"].PendingCount);
        Assert.Equal(1, stats["default"].RunningCount);
    }

    [Fact]
    public async Task GetQueueStats_IncludesImplicitNamedQueue_WhenRunsExist()
    {
        var ct = TestContext.Current.CancellationToken;
        var queueName = $"implicit-{Guid.CreateVersion7():N}";
        var jobName = $"ImplicitNamed_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.Queue = queueName;
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var stats = await Store.GetQueueStatsAsync(ct);

        Assert.True(stats.ContainsKey(queueName));
        Assert.Equal(1, stats[queueName].PendingCount);
        Assert.Equal(0, stats[queueName].RunningCount);
    }

    [Fact]
    public async Task BatchChildRuns_CountedInPendingQueueStats()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BatchStats_{Guid.CreateVersion7():N}";
        var queueName = "default";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = queueName }], ct);

        var batchId = Guid.CreateVersion7().ToString("N");
        var child1 = CreateRun(jobName) with { BatchId = batchId };

        var child2 = CreateRun(jobName) with { BatchId = batchId };

        var child3 = CreateRun(jobName) with { BatchId = batchId };

        await Store.CreateRunsAsync([child1, child2, child3], cancellationToken: ct);

        var queueStats = await Store.GetQueueStatsAsync(ct);
        Assert.True(queueStats.ContainsKey(queueName));
        Assert.Equal(3, queueStats[queueName].PendingCount);
    }

    [Fact]
    public async Task BatchChildRuns_PendingCount_NotAffectedByOtherRunTransitions()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BatchPending_{Guid.CreateVersion7():N}";
        var queueName = "default";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = queueName }], ct);

        var batchId = Guid.CreateVersion7().ToString("N");
        var child1 = CreateRun(jobName) with { BatchId = batchId };
        var child2 = CreateRun(jobName) with { BatchId = batchId };

        var unrelated = CreateRun(jobName);

        await Store.CreateRunsAsync([child1, child2, unrelated], cancellationToken: ct);

        var stats1 = await Store.GetQueueStatsAsync(ct);
        Assert.True(stats1.ContainsKey(queueName));
        Assert.Equal(3, stats1[queueName].PendingCount);

        var claimed = (await Store.ClaimRunsAsync("node1", [jobName], [queueName], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        await Store.TryTransitionRunAsync(RunStatusTransition.RunningToSucceeded(
            claimed.Id, claimed.Attempt, now, claimed.NotBefore, "node1", 1, null, null,
            claimed.StartedAt, now), ct);

        var stats2 = await Store.GetQueueStatsAsync(ct);
        Assert.True(stats2.ContainsKey(queueName));
        Assert.Equal(2, stats2[queueName].PendingCount);
    }
}
