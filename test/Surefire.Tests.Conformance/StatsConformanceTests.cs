namespace Surefire.Tests.Conformance;

public abstract class StatsConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task GetDashboardStats_ReturnsCorrectCounts()
    {
        var jobName = $"StatsJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));
        await Store.UpsertQueueAsync(new() { Name = "default" });

        var now = DateTimeOffset.UtcNow;
        var runs = new[]
        {
            CreateRun(jobName),
            CreateRun(jobName, JobStatus.Running),
            CreateRun(jobName, JobStatus.Succeeded),
            CreateRun(jobName, JobStatus.Retrying),
            CreateRun(jobName, JobStatus.Cancelled),
            CreateRun(jobName, JobStatus.Failed)
        };

        for (var i = 0; i < runs.Length; i++)
        {
            runs[i] = runs[i].Status is JobStatus.Succeeded or JobStatus.Cancelled or JobStatus.Failed
                ? runs[i] with { CreatedAt = now, NotBefore = now, StartedAt = now, CompletedAt = now }
                : runs[i] with { CreatedAt = now, NotBefore = now };
        }

        await Store.CreateRunsAsync(runs);

        var stats = await Store.GetDashboardStatsAsync();

        Assert.Equal(6, stats.TotalRuns);
        Assert.Equal(3, stats.ActiveRuns);
        Assert.True(stats.RunsByStatus.ContainsKey("Pending"));
        Assert.Equal(1, stats.RunsByStatus["Pending"]);
        Assert.True(stats.RunsByStatus.ContainsKey("Running"));
        Assert.Equal(1, stats.RunsByStatus["Running"]);
        Assert.True(stats.RunsByStatus.ContainsKey("Succeeded"));
        Assert.Equal(1, stats.RunsByStatus["Succeeded"]);
        Assert.True(stats.RunsByStatus.ContainsKey("Retrying"));
        Assert.Equal(1, stats.RunsByStatus["Retrying"]);
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
        Assert.Equal(1, stats.Timeline.Sum(b => b.Retrying));
        Assert.Equal(1, stats.Timeline.Sum(b => b.Cancelled));
        Assert.Equal(1, stats.Timeline.Sum(b => b.Failed));
    }

    [Fact]
    public async Task GetJobStats_ReturnsCorrectCounts()
    {
        var jobName = $"JobStatsJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));
        await Store.UpsertQueueAsync(new() { Name = "default" });

        var r1 = CreateRun(jobName);
        await Store.CreateRunsAsync([r1]);
        var c1 = await Store.ClaimRunAsync("node1", [jobName], ["default"]);
        Assert.NotNull(c1);
        c1 = c1 with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        await Store.TryTransitionRunAsync(Transition(c1, JobStatus.Running));

        var r2 = CreateRun(jobName);
        await Store.CreateRunsAsync([r2]);
        var c2 = await Store.ClaimRunAsync("node1", [jobName], ["default"]);
        Assert.NotNull(c2);
        c2 = c2 with { Status = JobStatus.Failed, CompletedAt = DateTimeOffset.UtcNow, Error = "permanent failure" };
        await Store.TryTransitionRunAsync(Transition(c2, JobStatus.Running));

        var r3 = CreateRun(jobName);
        await Store.CreateRunsAsync([r3]);

        var stats = await Store.GetJobStatsAsync(jobName);

        Assert.Equal(3, stats.TotalRuns);
        Assert.Equal(1, stats.SucceededRuns);
        Assert.Equal(1, stats.FailedRuns);
    }

    [Fact]
    public async Task GetQueueStats_ReturnsCorrectCounts()
    {
        var queueName = $"stats-queue-{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = queueName });

        var jobName = $"QStatsJob_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.Queue = queueName;
        await Store.UpsertJobAsync(job);

        var p1 = CreateRun(jobName);
        var p2 = CreateRun(jobName);
        await Store.CreateRunsAsync([p1]);
        await Store.CreateRunsAsync([p2]);

        var r1 = CreateRun(jobName);
        await Store.CreateRunsAsync([r1]);
        var claimed = await Store.ClaimRunAsync("node1", [jobName], [queueName]);
        Assert.NotNull(claimed);

        var queueStats = await Store.GetQueueStatsAsync();

        Assert.True(queueStats.ContainsKey(queueName));
        var qs = queueStats[queueName];
        Assert.Equal(2, qs.PendingCount);
        Assert.Equal(1, qs.RunningCount);
    }

    [Fact]
    public async Task GetDashboardStats_EmptyStore_ReturnsZeros()
    {
        var stats = await Store.GetDashboardStatsAsync();

        Assert.Equal(0, stats.TotalRuns);
        Assert.Equal(0, stats.ActiveRuns);
        Assert.Equal(0.0, stats.SuccessRate);
    }

    [Fact]
    public async Task GetDashboardStats_WithSince_FiltersTimeline()
    {
        var jobName = $"StatsTimeline_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));
        await Store.UpsertQueueAsync(new() { Name = "default" });

        var now = DateTimeOffset.UtcNow;

        var oldRun = CreateRun(jobName, JobStatus.Succeeded) with
        {
            CompletedAt = now.AddHours(-5),
            StartedAt = now.AddHours(-5)
        };

        // Create a completed run recently (after "since")
        var recentRun = CreateRun(jobName, JobStatus.Succeeded) with
        {
            CompletedAt = now.AddMinutes(-30),
            StartedAt = now.AddMinutes(-30)
        };

        await Store.CreateRunsAsync([oldRun, recentRun]);

        var stats = await Store.GetDashboardStatsAsync(now.AddHours(-1), 30);

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
        var freshNode = $"fresh-{Guid.CreateVersion7():N}";
        var staleNode = $"stale-{Guid.CreateVersion7():N}";

        await Store.HeartbeatAsync(freshNode, ["TestJob"], ["default"], []);
        await Store.HeartbeatAsync(staleNode, ["TestJob"], ["default"], []);

        // Purge the stale node's heartbeat by re-heartbeating with a very old time
        // is not possible via the API. Instead, create a third fresh node and verify
        // that the count includes recent nodes. We just check NodeCount > 0 since
        // we can't easily make a node stale via the public API.
        var stats = await Store.GetDashboardStatsAsync();
        Assert.True(stats.NodeCount >= 2);
    }

    [Fact]
    public async Task GetDashboardStats_ZeroBucketMinutes_DoesNotHang()
    {
        var stats = await Store.GetDashboardStatsAsync(bucketMinutes: 0);
        Assert.NotNull(stats);
    }

    [Fact]
    public async Task GetDashboardStats_Timeline_CorrectBucketCounts()
    {
        var jobName = $"BucketJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));
        await Store.UpsertQueueAsync(new() { Name = "default" });

        // Create a completed run and a failed run via proper claim + CAS flow
        var r1 = CreateRun(jobName);
        var r2 = CreateRun(jobName);
        await Store.CreateRunsAsync([r1, r2]);

        var c1 = await Store.ClaimRunAsync("node1", [jobName], ["default"]);
        Assert.NotNull(c1);
        c1 = c1 with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        await Store.TryTransitionRunAsync(Transition(c1, JobStatus.Running));

        var c2 = await Store.ClaimRunAsync("node1", [jobName], ["default"]);
        Assert.NotNull(c2);
        c2 = c2 with { Status = JobStatus.Failed, CompletedAt = DateTimeOffset.UtcNow, Error = "permanent failure" };
        await Store.TryTransitionRunAsync(Transition(c2, JobStatus.Running));

        var since = TruncateToMilliseconds(DateTimeOffset.UtcNow.AddHours(-1));
        var stats = await Store.GetDashboardStatsAsync(since);

        var totalCompleted = stats.Timeline.Sum(b => b.Succeeded);
        var totalDeadLetter = stats.Timeline.Sum(b => b.Failed);

        Assert.True(totalCompleted >= 1,
            $"Expected >= 1 completed, got {totalCompleted}. Buckets: {stats.Timeline.Count}");
        Assert.True(totalDeadLetter >= 1,
            $"Expected >= 1 dead letter, got {totalDeadLetter}. Buckets: {stats.Timeline.Count}");
    }
}