namespace Surefire.Tests.Conformance;

public abstract class MaintenanceConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task CancelExpiredRuns_CancelsPassedDeadline()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ExpireJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var run = CreateRun(jobName) with { NotAfter = DateTimeOffset.UtcNow.AddMinutes(-1) };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var cancelled = (await Store.CancelExpiredRunsWithIdsAsync(ct)).Count;

        Assert.Equal(1, cancelled);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Cancelled, loaded.Status);
        Assert.NotNull(loaded.CompletedAt);
    }

    [Fact]
    public async Task CancelExpiredRunsWithIds_ReturnsCancelledRunIds()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ExpireIds_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var expiredPending = CreateRun(jobName) with { NotAfter = now.AddMinutes(-1) };

        var expiredPending2 = CreateRun(jobName) with
        {
            NotAfter = now.AddMinutes(-1)
        };

        var notExpired = CreateRun(jobName) with { NotAfter = now.AddMinutes(10) };

        await Store.CreateRunsAsync([expiredPending, expiredPending2, notExpired], cancellationToken: ct);

        var cancelledIds = await Store.CancelExpiredRunsWithIdsAsync(ct);

        Assert.Equal(2, cancelledIds.Count);
        Assert.Contains(expiredPending.Id, cancelledIds);
        Assert.Contains(expiredPending2.Id, cancelledIds);
        Assert.DoesNotContain(notExpired.Id, cancelledIds);

        var loadedPending = await Store.GetRunAsync(expiredPending.Id, ct);
        var loadedPending2 = await Store.GetRunAsync(expiredPending2.Id, ct);
        var loadedNotExpired = await Store.GetRunAsync(notExpired.Id, ct);

        Assert.Equal(JobStatus.Cancelled, loadedPending!.Status);
        Assert.Equal(JobStatus.Cancelled, loadedPending2!.Status);
        Assert.Equal(JobStatus.Pending, loadedNotExpired!.Status);

        var pendingStatusEvents =
            await Store.GetEventsAsync(expiredPending.Id, types: [RunEventType.Status], cancellationToken: ct);
        var pending2StatusEvents =
            await Store.GetEventsAsync(expiredPending2.Id, types: [RunEventType.Status], cancellationToken: ct);

        Assert.Contains(pendingStatusEvents,
            e => RunStatusEvents.TryGetStatus(e, out var status) && status == JobStatus.Cancelled);
        Assert.Contains(pending2StatusEvents,
            e => RunStatusEvents.TryGetStatus(e, out var status) && status == JobStatus.Cancelled);
    }

    [Fact]
    public async Task CancelExpiredRuns_SkipsTerminal()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TerminalJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

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
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var cancelled = (await Store.CancelExpiredRunsWithIdsAsync(ct)).Count;
        Assert.Equal(0, cancelled);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Succeeded, loaded.Status);
    }

    [Fact]
    public async Task CancelExpiredRuns_SkipsRunning()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"RunningExpired_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run = CreateRun(jobName, JobStatus.Running) with
        {
            NotAfter = now.AddMinutes(-1),
            NodeName = "node1",
            Attempt = 1,
            StartedAt = now,
            LastHeartbeatAt = now
        };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        await Store.CancelExpiredRunsWithIdsAsync(ct);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Running, loaded.Status);
    }

    [Fact]
    public async Task HeartbeatRuns_UpdatesTimestamp()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"HeartbeatJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);

        var before = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(before);
        var originalHeartbeat = before.LastHeartbeatAt;
        Assert.NotNull(originalHeartbeat);

        await Store.HeartbeatAsync("node-1", [jobName], ["default"], [run.Id], ct);

        var after = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(after);
        Assert.NotNull(after.LastHeartbeatAt);
        Assert.True(after.LastHeartbeatAt >= originalHeartbeat);
    }

    [Fact]
    public async Task HeartbeatRuns_SkipsTerminalRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"HeartbeatTerminal_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);

        claimed = claimed with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running), ct);

        var before = await Store.GetRunAsync(run.Id, ct);
        var beforeHb = before!.LastHeartbeatAt;

        await Store.HeartbeatAsync("node-1", [jobName], ["default"], [run.Id], ct);

        var after = await Store.GetRunAsync(run.Id, ct);
        Assert.Equal(beforeHb, after!.LastHeartbeatAt);
    }

    [Fact]
    public async Task CancelExpiredRuns_CancelsPendingWithExpiredNotAfter()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"PendingExpired_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run = CreateRun(jobName) with
        {
            NotAfter = now.AddMinutes(-1)
        };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var count = (await Store.CancelExpiredRunsWithIdsAsync(ct)).Count;
        Assert.Equal(1, count);

        var after = await Store.GetRunAsync(run.Id, ct);
        Assert.Equal(JobStatus.Cancelled, after!.Status);
    }

    [Fact]
    public async Task CancelExpiredRuns_ReturnsCorrectCount()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ExpireCount_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var past = now.AddMinutes(-10);

        var run1 = CreateRun(jobName) with { NotAfter = past };
        var run2 = CreateRun(jobName) with { NotAfter = past };

        // Create run3 as another expired Pending run
        var run3 = CreateRun(jobName) with
        {
            NotAfter = past
        };
        await Store.CreateRunsAsync([run1, run2, run3], cancellationToken: ct);

        // Create a run WITHOUT expired NotAfter (should not be cancelled)
        var run4 = CreateRun(jobName) with { NotAfter = DateTimeOffset.UtcNow.AddHours(1) };
        await Store.CreateRunsAsync([run4], cancellationToken: ct);

        var cancelled = (await Store.CancelExpiredRunsWithIdsAsync(ct)).Count;

        Assert.Equal(3, cancelled);

        var stored4 = await Store.GetRunAsync(run4.Id, ct);
        Assert.Equal(JobStatus.Pending, stored4!.Status);
    }

    [Fact]
    public async Task CancelExpiredRuns_PendingWithExpiredNotAfter_IsCancelled()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"RetryAfterExpired_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var run = CreateRun(jobName) with { NotAfter = DateTimeOffset.UtcNow.AddMinutes(-1) };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        // Claim transitions to Running - but since we fixed ClaimRunAsync to check NotAfter,
        // this run won't be claimed. Instead test that the expired pending run IS cancelled.
        var cancelled = (await Store.CancelExpiredRunsWithIdsAsync(ct)).Count;
        Assert.Equal(1, cancelled);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Cancelled, loaded.Status);
    }

    // ── GetExternallyStoppedRunIdsAsync ─────────────────────────────────────

    [Fact]
    public async Task GetExternallyStoppedRunIds_EmptyInput_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var result = await Store.GetExternallyStoppedRunIdsAsync([], ct);
        Assert.Empty(result);
    }

    [Fact]
    public async Task GetExternallyStoppedRunIds_RunningRuns_AreNotReturned()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StoppedRunning_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);
        await Store.UpsertQueueAsync(new() { Name = "default" }, ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);
        var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);

        var stopped = await Store.GetExternallyStoppedRunIdsAsync([claimed.Id], ct);
        Assert.Empty(stopped);
    }

    [Fact]
    public async Task GetExternallyStoppedRunIds_TerminalRuns_AreReturned()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StoppedTerminal_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);
        await Store.UpsertQueueAsync(new() { Name = "default" }, ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var succeeded = CreateRun(jobName, JobStatus.Succeeded) with
        {
            NodeName = "node-1", Attempt = 1, StartedAt = now,
            CompletedAt = now, LastHeartbeatAt = now
        };
        var failed = CreateRun(jobName, JobStatus.Failed) with
        {
            NodeName = "node-1", Attempt = 1, StartedAt = now,
            CompletedAt = now, LastHeartbeatAt = now, Reason = "boom"
        };
        var cancelled = CreateRun(jobName, JobStatus.Cancelled) with
        {
            NodeName = "node-1", Attempt = 1, StartedAt = now,
            CompletedAt = now, CancelledAt = now, LastHeartbeatAt = now
        };
        await Store.CreateRunsAsync([succeeded, failed, cancelled], cancellationToken: ct);

        var stopped = await Store.GetExternallyStoppedRunIdsAsync(
            [succeeded.Id, failed.Id, cancelled.Id], ct);

        Assert.Equal(3, stopped.Count);
        Assert.Contains(succeeded.Id, stopped);
        Assert.Contains(failed.Id, stopped);
        Assert.Contains(cancelled.Id, stopped);
    }

    [Fact]
    public async Task GetExternallyStoppedRunIds_CancelledRuns_AreReturned()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StoppedCancelled_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);
        await Store.UpsertQueueAsync(new() { Name = "default" }, ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);
        var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);

        // Cancel the running run (Running → Cancelled, direct)
        Assert.True((await Store.TryCancelRunAsync(claimed.Id, cancellationToken: ct)).Transitioned);

        var stopped = await Store.GetExternallyStoppedRunIdsAsync([claimed.Id], ct);
        Assert.Single(stopped);
        Assert.Equal(claimed.Id, stopped[0]);
    }

    [Fact]
    public async Task GetExternallyStoppedRunIds_DeletedRuns_AreReturned()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StoppedDeleted_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        // Use an ID that was never inserted — simulates a purged run
        var fakeId = Guid.CreateVersion7().ToString("N");

        var stopped = await Store.GetExternallyStoppedRunIdsAsync([fakeId], ct);
        Assert.Single(stopped);
        Assert.Equal(fakeId, stopped[0]);
    }

    [Fact]
    public async Task GetExternallyStoppedRunIds_MixedStates_OnlyReturnsNonRunning()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StoppedMixed_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);
        await Store.UpsertQueueAsync(new() { Name = "default" }, ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        // Running run (should NOT be returned)
        var runA = CreateRun(jobName);
        await Store.CreateRunsAsync([runA], cancellationToken: ct);
        var claimedA = await Store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimedA);

        // Terminal run (should be returned)
        var runB = CreateRun(jobName, JobStatus.Failed) with
        {
            NodeName = "node-1", Attempt = 1, StartedAt = now,
            CompletedAt = now, LastHeartbeatAt = now, Reason = "boom"
        };
        await Store.CreateRunsAsync([runB], cancellationToken: ct);

        // Nonexistent (should be returned)
        var fakeId = Guid.CreateVersion7().ToString("N");

        var stopped = await Store.GetExternallyStoppedRunIdsAsync(
            [claimedA.Id, runB.Id, fakeId], ct);

        Assert.Equal(2, stopped.Count);
        Assert.Contains(runB.Id, stopped);
        Assert.Contains(fakeId, stopped);
        Assert.DoesNotContain(claimedA.Id, stopped);
    }

    // ── CancelExpiredRuns batch counter integration ────────────────────────

    [Fact]
    public async Task CancelExpiredRuns_BatchChild_IncrementsBatchCancelledCounter()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ExpireBatchCounter_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var expiringRun = CreateRun(jobName) with { NotAfter = now.AddMinutes(-1) };
        var normalRun = CreateRun(jobName);

        var batch = new JobBatch
        {
            Id = Guid.CreateVersion7().ToString("N"),
            Status = JobStatus.Pending,
            Total = 2,
            CreatedAt = now
        };
        await Store.CreateBatchAsync(batch,
            [expiringRun with { BatchId = batch.Id }, normalRun with { BatchId = batch.Id }],
            cancellationToken: ct);

        var cancelledIds = await Store.CancelExpiredRunsWithIdsAsync(ct);

        Assert.Contains(expiringRun.Id, cancelledIds);

        var updatedBatch = await Store.GetBatchAsync(batch.Id, ct);
        Assert.NotNull(updatedBatch);
        Assert.Equal(1, updatedBatch.Cancelled);
    }

    [Fact]
    public async Task CancelExpiredRuns_AllBatchChildrenExpired_BatchCountersMatchTotal()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ExpireBatchComplete_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run = CreateRun(jobName) with { NotAfter = now.AddMinutes(-1) };

        var batch = new JobBatch
        {
            Id = Guid.CreateVersion7().ToString("N"),
            Status = JobStatus.Pending,
            Total = 1,
            CreatedAt = now
        };
        await Store.CreateBatchAsync(batch, [run with { BatchId = batch.Id }], cancellationToken: ct);

        await Store.CancelExpiredRunsWithIdsAsync(ct);

        var updatedBatch = await Store.GetBatchAsync(batch.Id, ct);
        Assert.NotNull(updatedBatch);
        Assert.Equal(1, updatedBatch.Cancelled);
        Assert.Equal(updatedBatch.Total,
            updatedBatch.Succeeded + updatedBatch.Failed + updatedBatch.Cancelled);
    }

    // ── Purge ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task PurgeAsync_DoesNotRemoveTerminalBatchChildren_WhenBatchIsNotPurgeEligible()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"PurgeBatchKeep_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

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

        await Store.CreateBatchAsync(batch, [child], cancellationToken: ct);

        await Store.PurgeAsync(now, ct);

        var childAfter = await Store.GetRunAsync(child.Id, ct);
        Assert.NotNull(childAfter);
    }

    [Fact]
    public async Task PurgeAsync_RemovesTerminalBatchChildren_AfterBatchIsPurgeEligible()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"PurgeBatchRemove_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

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

        await Store.CreateBatchAsync(batch, [child], cancellationToken: ct);

        await Store.PurgeAsync(now, ct);

        var childAfter = await Store.GetRunAsync(child.Id, ct);
        Assert.Null(childAfter);
    }
}