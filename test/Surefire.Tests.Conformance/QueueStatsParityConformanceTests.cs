namespace Surefire.Tests.Conformance;

public abstract class QueueStatsParityConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task GetQueueStats_IncludesImplicitDefaultQueue_WhenRunsExist()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ImplicitDefault_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

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
        await Store.UpsertJobAsync(job, ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var stats = await Store.GetQueueStatsAsync(ct);

        Assert.True(stats.ContainsKey(queueName));
        Assert.Equal(1, stats[queueName].PendingCount);
        Assert.Equal(0, stats[queueName].RunningCount);
    }
}