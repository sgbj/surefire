namespace Surefire.Tests.Conformance;

public abstract class PurgeConformanceTests : StoreConformanceBase
{
    private static readonly DateTimeOffset OldTime = DateTimeOffset.UtcNow.AddDays(-30);
    private static readonly DateTimeOffset RecentTime = DateTimeOffset.UtcNow;
    private static readonly DateTimeOffset Threshold = DateTimeOffset.UtcNow.AddDays(-7);

    [Fact]
    public async Task Purge_DeletesTerminalRuns()
    {
        var jobName = $"PurgeJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        run.CreatedAt = OldTime;
        run.NotBefore = OldTime;
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.Completed;
        claimed.CompletedAt = OldTime;
        await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));

        await Store.AppendEventsAsync([
            new() { RunId = run.Id, EventType = RunEventType.Log, Payload = "test", CreatedAt = OldTime }
        ]);

        await Store.PurgeAsync(Threshold);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.Null(loaded);

        var events = await Store.GetEventsAsync(run.Id);
        Assert.Empty(events);
    }

    [Fact]
    public async Task Purge_DeletesStaleJobs()
    {
        var jobName = $"StaleJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var futureThreshold = DateTimeOffset.UtcNow.AddMinutes(5);
        await Store.PurgeAsync(futureThreshold);

        var loaded = await Store.GetJobAsync(jobName);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task Purge_DeletesStaleNodes()
    {
        var nodeName = $"stale-node-{Guid.CreateVersion7():N}";
        await Store.HeartbeatAsync(nodeName, ["Job"], ["default"], []);

        // The heartbeat sets LastHeartbeatAt to now, so we need a threshold in the future
        // to purge it, or we test with a stale node. Since HeartbeatAsync uses TimeProvider,
        // we purge with a future threshold for this test.
        var futureThreshold = DateTimeOffset.UtcNow.AddMinutes(5);
        await Store.PurgeAsync(futureThreshold);

        var nodes = await Store.GetNodesAsync();
        Assert.DoesNotContain(nodes, n => n.Name == nodeName);
    }

    [Fact]
    public async Task Purge_DeletesStaleQueues()
    {
        var queueName = $"stale-queue-{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = queueName });

        var futureThreshold = DateTimeOffset.UtcNow.AddMinutes(5);
        await Store.PurgeAsync(futureThreshold);

        var queues = await Store.GetQueuesAsync();
        Assert.DoesNotContain(queues, q => q.Name == queueName);
    }

    [Fact]
    public async Task Purge_DeletesStaleRateLimits()
    {
        var rlName = $"stale-rl-{Guid.CreateVersion7():N}";
        await Store.UpsertRateLimitAsync(new()
        {
            Name = rlName,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 10,
            Window = TimeSpan.FromMinutes(1)
        });

        var futureThreshold = DateTimeOffset.UtcNow.AddMinutes(5);
        await Store.PurgeAsync(futureThreshold);

        var jobName = $"RLJob_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.RateLimitName = rlName;
        await Store.UpsertJobAsync(job);
        await Store.UpsertQueueAsync(new() { Name = "default" });

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"]);
        Assert.NotNull(claimed);
    }

    [Fact]
    public async Task Purge_DeletesAbandonedPendingRuns()
    {
        var jobName = $"AbandonedJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        run.CreatedAt = OldTime;
        run.NotBefore = OldTime;
        await Store.CreateRunsAsync([run]);

        await Store.PurgeAsync(Threshold);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task Purge_DeletesAbandonedRetryingRuns()
    {
        var jobName = $"PurgeRetrying_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName, JobStatus.Retrying);
        run.NotBefore = OldTime;
        await Store.CreateRunsAsync([run]);

        await Store.PurgeAsync(Threshold);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.Null(stored);
    }

    [Fact]
    public async Task Purge_PreservesFutureScheduledRuns()
    {
        var jobName = $"FutureJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        run.NotBefore = DateTimeOffset.UtcNow.AddHours(1);
        await Store.CreateRunsAsync([run]);

        await Store.PurgeAsync(Threshold);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
    }

    [Fact]
    public async Task Purge_PreservesRunningRuns()
    {
        var jobName = $"RunningJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        run.CreatedAt = OldTime;
        run.NotBefore = OldTime;
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"]);
        Assert.NotNull(claimed);

        await Store.PurgeAsync(Threshold);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Running, loaded.Status);
    }

    [Fact]
    public async Task Purge_PreservesActiveEntities()
    {
        var jobName = $"ActiveJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var queueName = $"active-queue-{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = queueName });

        var nodeName = $"active-node-{Guid.CreateVersion7():N}";
        await Store.HeartbeatAsync(nodeName, [jobName], [queueName], []);

        await Store.PurgeAsync(Threshold);

        var loadedJob = await Store.GetJobAsync(jobName);
        Assert.NotNull(loadedJob);

        var queues = await Store.GetQueuesAsync();
        Assert.Contains(queues, q => q.Name == queueName);

        var nodes = await Store.GetNodesAsync();
        Assert.Contains(nodes, n => n.Name == nodeName);
    }

    [Fact]
    public async Task Purge_RateLimitWindowState_CleansUp()
    {
        var jobName = $"RatePurge_{Guid.CreateVersion7():N}";
        var rateLimitName = $"rl_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.RateLimitName = rateLimitName;
        await Store.UpsertJobAsync(job);
        await Store.UpsertRateLimitAsync(new()
        {
            Name = rateLimitName,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromHours(1)
        });

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);
        var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(claimed);

        // Purge with future threshold to remove the rate limit
        await Store.PurgeAsync(DateTimeOffset.UtcNow.AddDays(1));

        // Re-create the rate limit and a new run - should succeed if window state was cleaned up
        await Store.UpsertRateLimitAsync(new()
        {
            Name = rateLimitName,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromHours(1)
        });

        var job2 = CreateJob(jobName);
        job2.RateLimitName = rateLimitName;
        await Store.UpsertJobAsync(job2);

        var run2 = CreateRun(jobName);
        await Store.CreateRunsAsync([run2]);
        var claimed2 = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(claimed2);
    }

    [Fact]
    public async Task Purge_PreservesJobsWithActiveRuns()
    {
        var jobName = $"ActiveRunJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));
        await Store.UpsertQueueAsync(new() { Name = "default" });

        var run = CreateRun(jobName);
        run.CreatedAt = OldTime;
        run.NotBefore = OldTime;
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"]);
        Assert.NotNull(claimed);

        // Purge with a future threshold — job heartbeat is stale, but it has a running run
        var futureThreshold = DateTimeOffset.UtcNow.AddMinutes(5);
        await Store.PurgeAsync(futureThreshold);

        var loadedJob = await Store.GetJobAsync(jobName);
        Assert.NotNull(loadedJob);
    }
}