namespace Surefire.Tests.Conformance;

public abstract class QueueStatsParityConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task GetQueueStats_IncludesImplicitDefaultQueue_WhenRunsExist()
    {
        var jobName = $"ImplicitDefault_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var pending = CreateRun(jobName);
        var running = CreateRun(jobName);
        await Store.CreateRunsAsync([pending, running]);

        var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(claimed);

        var stats = await Store.GetQueueStatsAsync();

        Assert.True(stats.ContainsKey("default"));
        Assert.Equal(1, stats["default"].PendingCount);
        Assert.Equal(1, stats["default"].RunningCount);
    }

    [Fact]
    public async Task GetQueueStats_IncludesImplicitNamedQueue_WhenRunsExist()
    {
        var queueName = $"implicit-{Guid.CreateVersion7():N}";
        var jobName = $"ImplicitNamed_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.Queue = queueName;
        await Store.UpsertJobAsync(job);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);

        var stats = await Store.GetQueueStatsAsync();

        Assert.True(stats.ContainsKey(queueName));
        Assert.Equal(1, stats[queueName].PendingCount);
        Assert.Equal(0, stats[queueName].RunningCount);
    }
}