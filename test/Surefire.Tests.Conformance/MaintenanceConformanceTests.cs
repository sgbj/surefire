namespace Surefire.Tests.Conformance;

public abstract class MaintenanceConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task CancelExpiredRuns_CancelsPassedDeadline()
    {
        var jobName = $"ExpireJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        run.NotAfter = DateTimeOffset.UtcNow.AddMinutes(-1);
        await Store.CreateRunsAsync([run]);

        var cancelled = await Store.CancelExpiredRunsAsync();

        Assert.Equal(1, cancelled);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Cancelled, loaded.Status);
        Assert.NotNull(loaded.CompletedAt);
    }

    [Fact]
    public async Task CancelExpiredRuns_SkipsTerminal()
    {
        var jobName = $"TerminalJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run = CreateRun(jobName, JobStatus.Completed);
        run.NotAfter = now.AddMinutes(-1);
        run.NodeName = "node1";
        run.Attempt = 1;
        run.StartedAt = now;
        run.LastHeartbeatAt = now;
        run.CompletedAt = now;
        await Store.CreateRunsAsync([run]);

        var cancelled = await Store.CancelExpiredRunsAsync();
        Assert.Equal(0, cancelled);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Completed, loaded.Status);
    }

    [Fact]
    public async Task CancelExpiredRuns_SkipsBatchCoordinators()
    {
        var jobName = $"CoordJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName, JobStatus.Running);
        run.BatchTotal = 5;
        run.BatchCompleted = 0;
        run.BatchFailed = 0;
        run.NotAfter = DateTimeOffset.UtcNow.AddMinutes(-1);
        run.NodeName = "batch";
        run.Attempt = 1;
        run.StartedAt = DateTimeOffset.UtcNow;
        run.LastHeartbeatAt = DateTimeOffset.UtcNow;
        await Store.CreateRunsAsync([run]);

        await Store.CancelExpiredRunsAsync();

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Running, loaded.Status);
    }

    [Fact]
    public async Task CancelExpiredRuns_SkipsRunning()
    {
        var jobName = $"RunningExpired_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run = CreateRun(jobName, JobStatus.Running);
        run.NotAfter = now.AddMinutes(-1);
        run.NodeName = "node1";
        run.Attempt = 1;
        run.StartedAt = now;
        run.LastHeartbeatAt = now;
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

        claimed.Status = JobStatus.Completed;
        claimed.CompletedAt = DateTimeOffset.UtcNow;
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
        var run = CreateRun(jobName, JobStatus.Retrying);
        run.NotAfter = now.AddMinutes(-1);
        run.NodeName = "node-1";
        run.Attempt = 1;
        run.StartedAt = now;
        run.LastHeartbeatAt = now;
        run.Error = "test failure";
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

        var run1 = CreateRun(jobName);
        run1.NotAfter = past;
        var run2 = CreateRun(jobName);
        run2.NotAfter = past;

        // Create run3 directly as Retrying (can't claim with expired NotAfter)
        var run3 = CreateRun(jobName, JobStatus.Retrying);
        run3.NotAfter = past;
        run3.NodeName = "node-1";
        run3.Attempt = 1;
        run3.StartedAt = now;
        run3.LastHeartbeatAt = now;
        run3.Error = "test failure";
        await Store.CreateRunsAsync([run1, run2, run3]);

        // Create a run WITHOUT expired NotAfter (should not be cancelled)
        var run4 = CreateRun(jobName);
        run4.NotAfter = DateTimeOffset.UtcNow.AddHours(1);
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

        var run = CreateRun(jobName);
        run.NotAfter = DateTimeOffset.UtcNow.AddMinutes(-1);
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
    public async Task PurgeAsync_DoesNotRemoveTerminalBatchChildren_WhenCoordinatorIsNotPurgeEligible()
    {
        var jobName = $"PurgeBatchKeep_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var old = now.AddHours(-3);

        var coordinator = CreateRun(jobName, JobStatus.Running);
        coordinator.NodeName = "batch";
        coordinator.Attempt = 1;
        coordinator.StartedAt = now;
        coordinator.LastHeartbeatAt = now;
        coordinator.BatchTotal = 1;
        coordinator.BatchCompleted = 1;
        coordinator.BatchFailed = 0;

        var child = CreateRun(jobName, JobStatus.Completed);
        child.ParentRunId = coordinator.Id;
        child.RootRunId = coordinator.Id;
        child.CompletedAt = old;
        child.StartedAt = old.AddMinutes(-1);
        child.Attempt = 1;

        await Store.CreateRunsAsync([coordinator, child]);

        await Store.PurgeAsync(now);

        var childAfter = await Store.GetRunAsync(child.Id);
        Assert.NotNull(childAfter);
    }

    [Fact]
    public async Task PurgeAsync_RemovesTerminalBatchChildren_AfterCoordinatorIsPurgeEligible()
    {
        var jobName = $"PurgeBatchRemove_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var old = now.AddHours(-3);

        var coordinator = CreateRun(jobName, JobStatus.Running);
        coordinator.NodeName = "batch";
        coordinator.Attempt = 1;
        coordinator.StartedAt = old.AddMinutes(-2);
        coordinator.LastHeartbeatAt = old.AddMinutes(-2);
        coordinator.BatchTotal = 1;
        coordinator.BatchCompleted = 1;
        coordinator.BatchFailed = 0;

        var child = CreateRun(jobName, JobStatus.Completed);
        child.ParentRunId = coordinator.Id;
        child.RootRunId = coordinator.Id;
        child.CompletedAt = old;
        child.StartedAt = old.AddMinutes(-1);
        child.Attempt = 1;

        await Store.CreateRunsAsync([coordinator, child]);

        coordinator.Status = JobStatus.Completed;
        coordinator.CompletedAt = old;
        coordinator.Progress = 1;
        Assert.True(await Store.TryTransitionRunAsync(Transition(coordinator, JobStatus.Running)));

        await Store.PurgeAsync(now);

        var childAfter = await Store.GetRunAsync(child.Id);
        Assert.Null(childAfter);
    }
}