namespace Surefire.Tests.Conformance;

public abstract class RateLimitConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task UpsertRateLimit_CreatesAndUpdates()
    {
        var name = $"rl-{Guid.CreateVersion7():N}";
        var jobName = $"RLJob_{Guid.CreateVersion7():N}";
        var queueName = "default";

        var job = CreateJob(jobName);
        job.RateLimitName = name;
        await Store.UpsertJobAsync(job);
        await Store.UpsertQueueAsync(new() { Name = queueName });

        await Store.UpsertRateLimitAsync(new()
        {
            Name = name,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromHours(1)
        });

        var run1 = CreateRun(jobName);
        var run2 = CreateRun(jobName);
        await Store.CreateRunsAsync([run1]);
        await Store.CreateRunsAsync([run2]);

        var claimed1 = await Store.ClaimRunAsync("node1", [jobName], [queueName]);
        Assert.NotNull(claimed1);

        claimed1 = claimed1 with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        await Store.TryTransitionRunAsync(Transition(claimed1, JobStatus.Running));

        var claimed2 = await Store.ClaimRunAsync("node1", [jobName], [queueName]);
        Assert.Null(claimed2);

        await Store.UpsertRateLimitAsync(new()
        {
            Name = name,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 10,
            Window = TimeSpan.FromHours(1)
        });

        var claimed3 = await Store.ClaimRunAsync("node1", [jobName], [queueName]);
        Assert.NotNull(claimed3);
    }

    [Fact]
    public async Task UpsertRateLimit_PreservesRuntimeCounters()
    {
        var name = $"preserve-{Guid.CreateVersion7():N}";
        var jobName = $"PreserveRL_{Guid.CreateVersion7():N}";
        var queueName = "default";

        var job = CreateJob(jobName);
        job.RateLimitName = name;
        await Store.UpsertJobAsync(job);
        await Store.UpsertQueueAsync(new() { Name = queueName });

        await Store.UpsertRateLimitAsync(new()
        {
            Name = name,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromHours(1)
        });

        var run1 = CreateRun(jobName);
        await Store.CreateRunsAsync([run1]);
        var c1 = await Store.ClaimRunAsync("node1", [jobName], [queueName]);
        Assert.NotNull(c1);

        await Store.UpsertRateLimitAsync(new()
        {
            Name = name,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromHours(1)
        });

        var run2 = CreateRun(jobName);
        await Store.CreateRunsAsync([run2]);
        var c2 = await Store.ClaimRunAsync("node1", [jobName], [queueName]);
        Assert.Null(c2);
    }

    [Fact]
    public async Task RateLimit_SlidingWindow_BlocksWhenExhausted()
    {
        var name = $"sliding-{Guid.CreateVersion7():N}";
        var jobName = $"SlidingJob_{Guid.CreateVersion7():N}";
        var queueName = "default";

        await Store.UpsertRateLimitAsync(new()
        {
            Name = name,
            Type = RateLimitType.SlidingWindow,
            MaxPermits = 2,
            Window = TimeSpan.FromSeconds(10)
        });

        var job = CreateJob(jobName);
        job.RateLimitName = name;
        await Store.UpsertJobAsync(job);
        await Store.UpsertQueueAsync(new() { Name = queueName });

        var run1 = CreateRun(jobName);
        var run2 = CreateRun(jobName);
        var run3 = CreateRun(jobName);
        await Store.CreateRunsAsync([run1]);
        await Store.CreateRunsAsync([run2]);
        await Store.CreateRunsAsync([run3]);

        var c1 = await Store.ClaimRunAsync("node", [jobName], [queueName]);
        var c2 = await Store.ClaimRunAsync("node", [jobName], [queueName]);
        Assert.NotNull(c1);
        Assert.NotNull(c2);

        // With 2 permits used, a 3rd should be blocked
        var c3 = await Store.ClaimRunAsync("node", [jobName], [queueName]);
        Assert.Null(c3);
    }

    [Fact]
    public async Task RateLimit_FixedWindow_PermitsRecoverAfterWindowExpires()
    {
        await AssertPermitsRecoverAfterWindowAsync(RateLimitType.FixedWindow);
    }

    [Fact]
    public async Task RateLimit_SlidingWindow_PermitsRecoverAfterWindowExpires()
    {
        await AssertPermitsRecoverAfterWindowAsync(RateLimitType.SlidingWindow);
    }

    private async Task AssertPermitsRecoverAfterWindowAsync(RateLimitType type)
    {
        var suffix = Guid.CreateVersion7().ToString("N");
        var name = $"recover-{type}-{suffix}";
        var jobName = $"RecoverJob_{suffix}";
        var queueName = "default";

        await Store.UpsertRateLimitAsync(new()
        {
            Name = name,
            Type = type,
            MaxPermits = 1,
            Window = TimeSpan.FromMilliseconds(180)
        });

        var job = CreateJob(jobName);
        job.RateLimitName = name;
        await Store.UpsertJobAsync(job);
        await Store.UpsertQueueAsync(new() { Name = queueName });

        var run1 = CreateRun(jobName);
        var run2 = CreateRun(jobName);
        await Store.CreateRunsAsync([run1, run2]);

        var claimed1 = await Store.ClaimRunAsync("node", [jobName], [queueName]);
        Assert.NotNull(claimed1);

        var blocked = await Store.ClaimRunAsync("node", [jobName], [queueName]);
        Assert.Null(blocked);

        await Task.Delay(260);

        var claimedAfterWindow = await Store.ClaimRunAsync("node", [jobName], [queueName]);
        Assert.NotNull(claimedAfterWindow);
        Assert.NotEqual(claimed1.Id, claimedAfterWindow.Id);
        Assert.Contains(claimedAfterWindow.Id, new[] { run1.Id, run2.Id });
    }
}