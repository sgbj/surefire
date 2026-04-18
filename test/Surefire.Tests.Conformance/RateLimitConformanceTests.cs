namespace Surefire.Tests.Conformance;

public abstract class RateLimitConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task UpsertRateLimit_CreatesAndUpdates()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"rl-{Guid.CreateVersion7():N}";
        var jobName = $"RLJob_{Guid.CreateVersion7():N}";
        var queueName = "default";

        var job = CreateJob(jobName);
        job.RateLimitName = name;
        await Store.UpsertJobAsync(job, ct);
        await Store.UpsertQueueAsync(new() { Name = queueName }, ct);

        await Store.UpsertRateLimitAsync(new()
        {
            Name = name,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromHours(1)
        }, ct);

        var run1 = CreateRun(jobName);
        var run2 = CreateRun(jobName);
        await Store.CreateRunsAsync([run1], cancellationToken: ct);
        await Store.CreateRunsAsync([run2], cancellationToken: ct);

        var claimed1 = await Store.ClaimRunAsync("node1", [jobName], [queueName], ct);
        Assert.NotNull(claimed1);

        claimed1 = claimed1 with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        await Store.TryTransitionRunAsync(Transition(claimed1, JobStatus.Running), ct);

        var claimed2 = await Store.ClaimRunAsync("node1", [jobName], [queueName], ct);
        Assert.Null(claimed2);

        await Store.UpsertRateLimitAsync(new()
        {
            Name = name,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 10,
            Window = TimeSpan.FromHours(1)
        }, ct);

        var claimed3 = await Store.ClaimRunAsync("node1", [jobName], [queueName], ct);
        Assert.NotNull(claimed3);
    }

    [Fact]
    public async Task UpsertRateLimit_PreservesRuntimeCounters()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"preserve-{Guid.CreateVersion7():N}";
        var jobName = $"PreserveRL_{Guid.CreateVersion7():N}";
        var queueName = "default";

        var job = CreateJob(jobName);
        job.RateLimitName = name;
        await Store.UpsertJobAsync(job, ct);
        await Store.UpsertQueueAsync(new() { Name = queueName }, ct);

        await Store.UpsertRateLimitAsync(new()
        {
            Name = name,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromHours(1)
        }, ct);

        var run1 = CreateRun(jobName);
        await Store.CreateRunsAsync([run1], cancellationToken: ct);
        var c1 = await Store.ClaimRunAsync("node1", [jobName], [queueName], ct);
        Assert.NotNull(c1);

        await Store.UpsertRateLimitAsync(new()
        {
            Name = name,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromHours(1)
        }, ct);

        var run2 = CreateRun(jobName);
        await Store.CreateRunsAsync([run2], cancellationToken: ct);
        var c2 = await Store.ClaimRunAsync("node1", [jobName], [queueName], ct);
        Assert.Null(c2);
    }

    [Fact]
    public async Task RateLimit_SlidingWindow_BlocksWhenExhausted()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"sliding-{Guid.CreateVersion7():N}";
        var jobName = $"SlidingJob_{Guid.CreateVersion7():N}";
        var queueName = "default";

        await Store.UpsertRateLimitAsync(new()
        {
            Name = name,
            Type = RateLimitType.SlidingWindow,
            MaxPermits = 2,
            Window = TimeSpan.FromSeconds(10)
        }, ct);

        var job = CreateJob(jobName);
        job.RateLimitName = name;
        await Store.UpsertJobAsync(job, ct);
        await Store.UpsertQueueAsync(new() { Name = queueName }, ct);

        var run1 = CreateRun(jobName);
        var run2 = CreateRun(jobName);
        var run3 = CreateRun(jobName);
        await Store.CreateRunsAsync([run1], cancellationToken: ct);
        await Store.CreateRunsAsync([run2], cancellationToken: ct);
        await Store.CreateRunsAsync([run3], cancellationToken: ct);

        var c1 = await Store.ClaimRunAsync("node", [jobName], [queueName], ct);
        var c2 = await Store.ClaimRunAsync("node", [jobName], [queueName], ct);
        Assert.NotNull(c1);
        Assert.NotNull(c2);

        // With 2 permits used, a 3rd should be blocked
        var c3 = await Store.ClaimRunAsync("node", [jobName], [queueName], ct);
        Assert.Null(c3);
    }

    [Fact]
    public async Task RateLimit_FixedWindow_PermitsRecoverAfterWindowExpires()
    {
        var ct = TestContext.Current.CancellationToken;
        await AssertPermitsRecoverAfterWindowAsync(RateLimitType.FixedWindow, ct);
    }

    [Fact]
    public async Task RateLimit_SlidingWindow_PermitsRecoverAfterWindowExpires()
    {
        var ct = TestContext.Current.CancellationToken;
        await AssertPermitsRecoverAfterWindowAsync(RateLimitType.SlidingWindow, ct);
    }

    private async Task AssertPermitsRecoverAfterWindowAsync(RateLimitType type, CancellationToken ct)
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
        }, ct);

        var job = CreateJob(jobName);
        job.RateLimitName = name;
        await Store.UpsertJobAsync(job, ct);
        await Store.UpsertQueueAsync(new() { Name = queueName }, ct);

        var run1 = CreateRun(jobName);
        var run2 = CreateRun(jobName);
        await Store.CreateRunsAsync([run1, run2], cancellationToken: ct);

        var claimed1 = await Store.ClaimRunAsync("node", [jobName], [queueName], ct);
        Assert.NotNull(claimed1);

        var blocked = await Store.ClaimRunAsync("node", [jobName], [queueName], ct);
        Assert.Null(blocked);

        await Task.Delay(260, ct);

        var claimedAfterWindow = await Store.ClaimRunAsync("node", [jobName], [queueName], ct);
        Assert.NotNull(claimedAfterWindow);
        Assert.NotEqual(claimed1.Id, claimedAfterWindow.Id);
        Assert.Contains(claimedAfterWindow.Id, new[] { run1.Id, run2.Id });
    }
}