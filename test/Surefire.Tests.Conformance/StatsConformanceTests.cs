namespace Surefire.Tests.Conformance;

public abstract class StatsConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task GetDashboardStats_ReturnsCorrectCounts()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StatsJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var now = DateTimeOffset.UtcNow;
        var runs = new[]
        {
            CreateRun(jobName),
            CreateRun(jobName, JobStatus.Running),
            CreateRun(jobName, JobStatus.Succeeded),
            CreateRun(jobName, JobStatus.Cancelled),
            CreateRun(jobName, JobStatus.Failed)
        };

        for (var i = 0; i < runs.Length; i++)
        {
            runs[i] = runs[i].Status is JobStatus.Succeeded or JobStatus.Cancelled or JobStatus.Failed
                ? runs[i] with { CreatedAt = now, NotBefore = now, StartedAt = now, CompletedAt = now }
                : runs[i] with { CreatedAt = now, NotBefore = now };
        }

        await Store.CreateRunsAsync(runs, cancellationToken: ct);

        var stats = await Store.GetDashboardStatsAsync(cancellationToken: ct);

        Assert.Equal(5, stats.TotalRuns);
        Assert.Equal(2, stats.ActiveRuns);
        Assert.True(stats.RunsByStatus.ContainsKey("Pending"));
        Assert.Equal(1, stats.RunsByStatus["Pending"]);
        Assert.True(stats.RunsByStatus.ContainsKey("Running"));
        Assert.Equal(1, stats.RunsByStatus["Running"]);
        Assert.True(stats.RunsByStatus.ContainsKey("Succeeded"));
        Assert.Equal(1, stats.RunsByStatus["Succeeded"]);
        Assert.True(stats.RunsByStatus.ContainsKey("Cancelled"));
        Assert.Equal(1, stats.RunsByStatus["Cancelled"]);
        Assert.True(stats.RunsByStatus.ContainsKey("Failed"));
        Assert.Equal(1, stats.RunsByStatus["Failed"]);
        Assert.True(stats.TotalJobs >= 1);
        Assert.Equal(1d / 3d, stats.SuccessRate, 5);
        Assert.NotEmpty(stats.Timeline);

        Assert.Equal(1, stats.Timeline.Sum(b => b.Pending));
        Assert.Equal(1, stats.Timeline.Sum(b => b.Running));
        Assert.Equal(1, stats.Timeline.Sum(b => b.Succeeded));
        Assert.Equal(1, stats.Timeline.Sum(b => b.Cancelled));
        Assert.Equal(1, stats.Timeline.Sum(b => b.Failed));
    }

    [Fact]
    public async Task GetJobStats_ReturnsCorrectCounts()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"JobStatsJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var r1 = CreateRun(jobName);
        await Store.CreateRunsAsync([r1], cancellationToken: ct);
        var c1 = (await Store.ClaimRunsAsync("node1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(c1);
        c1 = c1 with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        await Store.TryTransitionRunAsync(Transition(c1, JobStatus.Running), ct);

        var r2 = CreateRun(jobName);
        await Store.CreateRunsAsync([r2], cancellationToken: ct);
        var c2 = (await Store.ClaimRunsAsync("node1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(c2);
        c2 = c2 with { Status = JobStatus.Failed, CompletedAt = DateTimeOffset.UtcNow, Reason = "permanent failure" };
        await Store.TryTransitionRunAsync(Transition(c2, JobStatus.Running), ct);

        var r3 = CreateRun(jobName);
        await Store.CreateRunsAsync([r3], cancellationToken: ct);

        var stats = await Store.GetJobStatsAsync(jobName, ct);

        Assert.Equal(3, stats.TotalRuns);
        Assert.Equal(1, stats.SucceededRuns);
        Assert.Equal(1, stats.FailedRuns);
    }

    [Fact]
    public async Task GetJobStats_NoRuns_ReturnsZeros()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"EmptyStatsJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var stats = await Store.GetJobStatsAsync(jobName, ct);

        Assert.Equal(0, stats.TotalRuns);
        Assert.Equal(0, stats.SucceededRuns);
        Assert.Equal(0, stats.FailedRuns);
        Assert.Equal(0.0, stats.SuccessRate);
        Assert.Null(stats.AvgDuration);
        Assert.Null(stats.LastRunAt);
    }

    [Fact]
    public async Task GetJobStats_UnknownJob_ReturnsZeros()
    {
        var ct = TestContext.Current.CancellationToken;
        var stats = await Store.GetJobStatsAsync($"DoesNotExist_{Guid.CreateVersion7():N}", ct);

        Assert.Equal(0, stats.TotalRuns);
        Assert.Equal(0, stats.SucceededRuns);
        Assert.Equal(0, stats.FailedRuns);
    }

    [Fact]
    public async Task GetQueueStats_ReturnsCorrectCounts()
    {
        var ct = TestContext.Current.CancellationToken;
        var queueName = $"stats-queue-{Guid.CreateVersion7():N}";
        await Store.UpsertQueuesAsync([new() { Name = queueName }], ct);

        var jobName = $"QStatsJob_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.Queue = queueName;
        await Store.UpsertJobsAsync([job], ct);

        var p1 = CreateRun(jobName);
        var p2 = CreateRun(jobName);
        await Store.CreateRunsAsync([p1], cancellationToken: ct);
        await Store.CreateRunsAsync([p2], cancellationToken: ct);

        var r1 = CreateRun(jobName);
        await Store.CreateRunsAsync([r1], cancellationToken: ct);
        var claimed = (await Store.ClaimRunsAsync("node1", [jobName], [queueName], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        var queueStats = await Store.GetQueueStatsAsync(ct);

        Assert.True(queueStats.ContainsKey(queueName));
        var qs = queueStats[queueName];
        Assert.Equal(2, qs.PendingCount);
        Assert.Equal(1, qs.RunningCount);
    }

    [Fact]
    public async Task GetDashboardStats_EmptyStore_ReturnsZeros()
    {
        var ct = TestContext.Current.CancellationToken;
        var stats = await Store.GetDashboardStatsAsync(cancellationToken: ct);

        Assert.Equal(0, stats.TotalRuns);
        Assert.Equal(0, stats.ActiveRuns);
        Assert.Equal(0.0, stats.SuccessRate);
    }

    [Fact]
    public async Task GetDashboardStats_WithSince_FiltersTimeline()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StatsTimeline_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var now = DateTimeOffset.UtcNow;

        var oldRun = CreateRun(jobName, JobStatus.Succeeded) with
        {
            CompletedAt = now.AddHours(-5),
            StartedAt = now.AddHours(-5)
        };

        var recentRun = CreateRun(jobName, JobStatus.Succeeded) with
        {
            CompletedAt = now.AddMinutes(-30),
            StartedAt = now.AddMinutes(-30)
        };

        await Store.CreateRunsAsync([oldRun, recentRun], cancellationToken: ct);

        var stats = await Store.GetDashboardStatsAsync(now.AddHours(-1), 30, ct);

        Assert.NotNull(stats);
        Assert.True(stats.Timeline.Count > 0);
        // All timeline buckets should be after the "since" time (allow one bucket of slack)
        foreach (var bucket in stats.Timeline)
        {
            Assert.True(bucket.Start >= now.AddHours(-1).AddMinutes(-30));
        }
    }

    [Fact]
    public async Task GetDashboardStats_NodeCount_IncludesFreshNodes()
    {
        var ct = TestContext.Current.CancellationToken;
        var freshNode = $"fresh-{Guid.CreateVersion7():N}";
        var staleNode = $"stale-{Guid.CreateVersion7():N}";

        await Store.HeartbeatAsync(freshNode, ["TestJob"], ["default"], [], ct);
        await Store.HeartbeatAsync(staleNode, ["TestJob"], ["default"], [], ct);

        // Cannot make a node stale via the public API, so just assert NodeCount >= 2.
        var stats = await Store.GetDashboardStatsAsync(cancellationToken: ct);
        Assert.True(stats.NodeCount >= 2);
    }

    [Fact]
    public async Task GetDashboardStats_ZeroBucketMinutes_DoesNotHang()
    {
        var ct = TestContext.Current.CancellationToken;
        var stats = await Store.GetDashboardStatsAsync(bucketMinutes: 0, cancellationToken: ct);
        Assert.NotNull(stats);
    }

    [Fact]
    public async Task GetDashboardStats_Timeline_CorrectBucketCounts()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BucketJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        // Create a completed run and a failed run via proper claim + CAS flow
        var r1 = CreateRun(jobName);
        var r2 = CreateRun(jobName);
        await Store.CreateRunsAsync([r1, r2], cancellationToken: ct);

        var c1 = (await Store.ClaimRunsAsync("node1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(c1);
        c1 = c1 with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        await Store.TryTransitionRunAsync(Transition(c1, JobStatus.Running), ct);

        var c2 = (await Store.ClaimRunsAsync("node1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(c2);
        c2 = c2 with { Status = JobStatus.Failed, CompletedAt = DateTimeOffset.UtcNow, Reason = "permanent failure" };
        await Store.TryTransitionRunAsync(Transition(c2, JobStatus.Running), ct);

        var since = TruncateToMilliseconds(DateTimeOffset.UtcNow.AddHours(-1));
        var stats = await Store.GetDashboardStatsAsync(since, cancellationToken: ct);

        var totalCompleted = stats.Timeline.Sum(b => b.Succeeded);
        var totalDeadLetter = stats.Timeline.Sum(b => b.Failed);

        Assert.True(totalCompleted >= 1,
            $"Expected >= 1 completed, got {totalCompleted}. Buckets: {stats.Timeline.Count}");
        Assert.True(totalDeadLetter >= 1,
            $"Expected >= 1 dead letter, got {totalDeadLetter}. Buckets: {stats.Timeline.Count}");
    }
}
