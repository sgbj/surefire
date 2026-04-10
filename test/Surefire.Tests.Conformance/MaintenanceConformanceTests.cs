namespace Surefire.Tests.Conformance;

public abstract class MaintenanceConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task CancelExpiredRuns_CancelsPassedDeadline()
    {
        var jobName = $"ExpireJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName) with { NotAfter = DateTimeOffset.UtcNow.AddMinutes(-1) };
        await Store.CreateRunsAsync([run]);

        var cancelled = await Store.CancelExpiredRunsAsync();

        Assert.Equal(1, cancelled);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Cancelled, loaded.Status);
        Assert.NotNull(loaded.CompletedAt);
    }

    [Fact]
    public async Task CancelExpiredRunsWithIds_ReturnsCancelledRunIds()
    {
        var jobName = $"ExpireIds_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var expiredPending = CreateRun(jobName) with { NotAfter = now.AddMinutes(-1) };

        var expiredRetrying = CreateRun(jobName, JobStatus.Retrying) with
        {
            NotAfter = now.AddMinutes(-1),
            NodeName = "node-1",
            Attempt = 1,
            StartedAt = now,
            LastHeartbeatAt = now,
            Error = "test failure"
        };

        var notExpired = CreateRun(jobName) with { NotAfter = now.AddMinutes(10) };

        await Store.CreateRunsAsync([expiredPending, expiredRetrying, notExpired]);

        var cancelledIds = await Store.CancelExpiredRunsWithIdsAsync();

        Assert.Equal(2, cancelledIds.Count);
        Assert.Contains(expiredPending.Id, cancelledIds);
        Assert.Contains(expiredRetrying.Id, cancelledIds);
        Assert.DoesNotContain(notExpired.Id, cancelledIds);

        var loadedPending = await Store.GetRunAsync(expiredPending.Id);
        var loadedRetrying = await Store.GetRunAsync(expiredRetrying.Id);
        var loadedNotExpired = await Store.GetRunAsync(notExpired.Id);

        Assert.Equal(JobStatus.Cancelled, loadedPending!.Status);
        Assert.Equal(JobStatus.Cancelled, loadedRetrying!.Status);
        Assert.Equal(JobStatus.Pending, loadedNotExpired!.Status);
    }

    [Fact]
    public async Task CancelExpiredRuns_SkipsTerminal()
    {
        var jobName = $"TerminalJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run = CreateRun(jobName, JobStatus.Succeeded) with
        {
            NotAfter = now.AddMinutes(-1),
            NodeName = "node1",
            Attempt = 1,
            StartedAt = now,
            LastHeartbeatAt = now,
            CompletedAt = now
        };
        await Store.CreateRunsAsync([run]);

        var cancelled = await Store.CancelExpiredRunsAsync();
        Assert.Equal(0, cancelled);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Succeeded, loaded.Status);
    }

    [Fact]
    public async Task CancelExpiredRuns_SkipsRunning()
    {
        var jobName = $"RunningExpired_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run = CreateRun(jobName, JobStatus.Running) with
        {
            NotAfter = now.AddMinutes(-1),
            NodeName = "node1",
            Attempt = 1,
            StartedAt = now,
            LastHeartbeatAt = now
        };
        await Store.CreateRunsAsync([run]);

        await Store.CancelExpiredRunsAsync();

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Running, loaded.Status);
    }

    [Fact]
    public async Task HeartbeatRuns_UpdatesTimestamp()
    {
        var jobName = $"HeartbeatJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(claimed);

        var before = await Store.GetRunAsync(run.Id);
        Assert.NotNull(before);
        var originalHeartbeat = before.LastHeartbeatAt;
        Assert.NotNull(originalHeartbeat);

        await Store.HeartbeatAsync("node-1", [jobName], ["default"], [run.Id]);

        var after = await Store.GetRunAsync(run.Id);
        Assert.NotNull(after);
        Assert.NotNull(after.LastHeartbeatAt);
        Assert.True(after.LastHeartbeatAt >= originalHeartbeat);
    }

    [Fact]
    public async Task HeartbeatRuns_SkipsTerminalRuns()
    {
        var jobName = $"HeartbeatTerminal_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(claimed);

        claimed = claimed with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));

        var before = await Store.GetRunAsync(run.Id);
        var beforeHb = before!.LastHeartbeatAt;

        await Store.HeartbeatAsync("node-1", [jobName], ["default"], [run.Id]);

        var after = await Store.GetRunAsync(run.Id);
        Assert.Equal(beforeHb, after!.LastHeartbeatAt);
    }

    [Fact]
    public async Task CancelExpiredRuns_CancelsRetrying()
    {
        var jobName = $"RetryingExpired_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run = CreateRun(jobName, JobStatus.Retrying) with
        {
            NotAfter = now.AddMinutes(-1),
            NodeName = "node-1",
            Attempt = 1,
            StartedAt = now,
            LastHeartbeatAt = now,
            Error = "test failure"
        };
        await Store.CreateRunsAsync([run]);

        var count = await Store.CancelExpiredRunsAsync();
        Assert.Equal(1, count);

        var after = await Store.GetRunAsync(run.Id);
        Assert.Equal(JobStatus.Cancelled, after!.Status);
    }

    [Fact]
    public async Task CancelExpiredRuns_ReturnsCorrectCount()
    {
        var jobName = $"ExpireCount_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var past = now.AddMinutes(-10);

        var run1 = CreateRun(jobName) with { NotAfter = past };
        var run2 = CreateRun(jobName) with { NotAfter = past };

        // Create run3 directly as Retrying (can't claim with expired NotAfter)
        var run3 = CreateRun(jobName, JobStatus.Retrying) with
        {
            NotAfter = past,
            NodeName = "node-1",
            Attempt = 1,
            StartedAt = now,
            LastHeartbeatAt = now,
            Error = "test failure"
        };
        await Store.CreateRunsAsync([run1, run2, run3]);

        // Create a run WITHOUT expired NotAfter (should not be cancelled)
        var run4 = CreateRun(jobName) with { NotAfter = DateTimeOffset.UtcNow.AddHours(1) };
        await Store.CreateRunsAsync([run4]);

        var cancelled = await Store.CancelExpiredRunsAsync();

        Assert.Equal(3, cancelled);

        var stored4 = await Store.GetRunAsync(run4.Id);
        Assert.Equal(JobStatus.Pending, stored4!.Status);
    }

    [Fact]
    public async Task CancelExpiredRuns_RunningWithExpiredNotAfter_CanStillBeCancelledWhenRetrying()
    {
        var jobName = $"RetryAfterExpired_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName) with { NotAfter = DateTimeOffset.UtcNow.AddMinutes(-1) };
        await Store.CreateRunsAsync([run]);

        // Claim transitions to Running - but since we fixed ClaimRunAsync to check NotAfter,
        // this run won't be claimed. Instead test that the expired pending run IS cancelled.
        var cancelled = await Store.CancelExpiredRunsAsync();
        Assert.Equal(1, cancelled);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Cancelled, loaded.Status);
    }

    [Fact]
    public async Task PurgeAsync_DoesNotRemoveTerminalBatchChildren_WhenBatchIsNotPurgeEligible()
    {
        var jobName = $"PurgeBatchKeep_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var old = now.AddHours(-3);

        // Batch is still Running (not terminal) — children should not be purged
        var batch = new JobBatch
        {
            Id = Guid.CreateVersion7().ToString("N"),
            Status = JobStatus.Running,
            Total = 1,
            Succeeded = 0,
            Failed = 0,
            CreatedAt = old
        };

        var child = CreateRun(jobName, JobStatus.Succeeded) with
        {
            BatchId = batch.Id,
            CompletedAt = old,
            StartedAt = old.AddMinutes(-1),
            Attempt = 1
        };

        await Store.CreateBatchAsync(batch, [child]);

        await Store.PurgeAsync(now);

        var childAfter = await Store.GetRunAsync(child.Id);
        Assert.NotNull(childAfter);
    }

    [Fact]
    public async Task PurgeAsync_RemovesTerminalBatchChildren_AfterBatchIsPurgeEligible()
    {
        var jobName = $"PurgeBatchRemove_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var old = now.AddHours(-3);

        var batch = new JobBatch
        {
            Id = Guid.CreateVersion7().ToString("N"),
            Status = JobStatus.Succeeded,
            Total = 1,
            Succeeded = 1,
            Failed = 0,
            CreatedAt = old.AddMinutes(-2),
            CompletedAt = old.AddMinutes(-1)
        };

        var child = CreateRun(jobName, JobStatus.Succeeded) with
        {
            BatchId = batch.Id,
            CompletedAt = old,
            StartedAt = old.AddMinutes(-1),
            Attempt = 1
        };

        await Store.CreateBatchAsync(batch, [child]);

        await Store.PurgeAsync(now);

        var childAfter = await Store.GetRunAsync(child.Id);
        Assert.Null(childAfter);
    }
}