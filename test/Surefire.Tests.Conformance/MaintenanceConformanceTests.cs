namespace Surefire.Tests.Conformance;

public abstract class MaintenanceConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task CancelExpiredRuns_CancelsPassedDeadline()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ExpireJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var past = now.AddMinutes(-10);

        var run1 = CreateRun(jobName) with { NotAfter = past };
        var run2 = CreateRun(jobName) with { NotAfter = past };

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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run = CreateRun(jobName) with { NotAfter = DateTimeOffset.UtcNow.AddMinutes(-1) };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        // ClaimRunsAsync skips expired runs, so the expired pending run won't be claimed;
        // assert it is cancelled instead.
        var cancelled = (await Store.CancelExpiredRunsWithIdsAsync(ct)).Count;
        Assert.Equal(1, cancelled);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Cancelled, loaded.Status);
    }

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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);
        var claimed = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        var stopped = await Store.GetExternallyStoppedRunIdsAsync([claimed.Id], ct);
        Assert.Empty(stopped);
    }

    [Fact]
    public async Task GetExternallyStoppedRunIds_TerminalRuns_AreReturned()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StoppedTerminal_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);
        var claimed = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        // Cancel the running run directly (Running to Cancelled).
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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        // An ID that was never inserted simulates a purged run.
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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        // Running run (should NOT be returned)
        var runA = CreateRun(jobName);
        await Store.CreateRunsAsync([runA], cancellationToken: ct);
        var claimedA = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
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

    [Fact]
    public async Task CancelExpiredRuns_BatchChild_IncrementsBatchCancelledCounter()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ExpireBatchCounter_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

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

    [Fact]
    public async Task PurgeAsync_DoesNotRemoveTerminalBatchChildren_WhenBatchIsNotPurgeEligible()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"PurgeBatchKeep_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var old = now.AddHours(-3);

        // Batch is still Running (not terminal); children should not be purged.
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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

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

    [Fact]
    public async Task GetStaleRunningRunIds_ReturnsOldestFirst()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StaleOrder_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var oldest = CreateRun(jobName, JobStatus.Running) with
        {
            NodeName = "node1", StartedAt = now.AddMinutes(-30), LastHeartbeatAt = now.AddMinutes(-30)
        };
        var middle = CreateRun(jobName, JobStatus.Running) with
        {
            NodeName = "node1", StartedAt = now.AddMinutes(-20), LastHeartbeatAt = now.AddMinutes(-20)
        };
        var newest = CreateRun(jobName, JobStatus.Running) with
        {
            NodeName = "node1", StartedAt = now.AddMinutes(-10), LastHeartbeatAt = now.AddMinutes(-10)
        };
        await Store.CreateRunsAsync([newest, oldest, middle], cancellationToken: ct);

        var ids = await Store.GetStaleRunningRunIdsAsync(now.AddMinutes(-5), 10, ct);

        Assert.Equal(3, ids.Count);
        Assert.Equal(oldest.Id, ids[0]);
        Assert.Equal(middle.Id, ids[1]);
        Assert.Equal(newest.Id, ids[2]);
    }

    [Fact]
    public async Task GetStaleRunningRunIds_ExcludesPendingAndFutureScheduled()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StalePending_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var running = CreateRun(jobName, JobStatus.Running) with
        {
            NodeName = "n", StartedAt = now.AddMinutes(-30), LastHeartbeatAt = now.AddMinutes(-30)
        };
        var pending = CreateRun(jobName);
        var futureScheduled = CreateRun(jobName) with { NotBefore = now.AddHours(1) };

        await Store.CreateRunsAsync([running, pending, futureScheduled], cancellationToken: ct);

        var ids = await Store.GetStaleRunningRunIdsAsync(now.AddMinutes(-5), 10, ct);

        Assert.Single(ids);
        Assert.Equal(running.Id, ids[0]);
    }

    [Fact]
    public async Task GetStaleRunningRunIds_ExcludesFreshHeartbeats()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StaleFresh_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var stale = CreateRun(jobName, JobStatus.Running) with
        {
            NodeName = "n", StartedAt = now.AddMinutes(-30), LastHeartbeatAt = now.AddMinutes(-30)
        };
        var fresh = CreateRun(jobName, JobStatus.Running) with
        {
            NodeName = "n", StartedAt = now.AddSeconds(-10), LastHeartbeatAt = now.AddSeconds(-10)
        };
        await Store.CreateRunsAsync([stale, fresh], cancellationToken: ct);

        var ids = await Store.GetStaleRunningRunIdsAsync(now.AddMinutes(-5), 10, ct);

        Assert.Single(ids);
        Assert.Equal(stale.Id, ids[0]);
    }

    [Fact]
    public async Task GetStaleRunningRunIds_RespectsTake()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StaleTake_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var runs = new List<JobRun>();
        for (var i = 0; i < 5; i++)
        {
            runs.Add(CreateRun(jobName, JobStatus.Running) with
            {
                NodeName = "n",
                StartedAt = now.AddMinutes(-30 - i),
                LastHeartbeatAt = now.AddMinutes(-30 - i)
            });
        }

        await Store.CreateRunsAsync(runs, cancellationToken: ct);

        var ids = await Store.GetStaleRunningRunIdsAsync(now.AddMinutes(-5), 3, ct);

        Assert.Equal(3, ids.Count);
    }

    [Fact]
    public async Task GetStaleRunningRunIds_RemovesRecoveredRunsFromNextCall()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StaleDrain_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run = CreateRun(jobName, JobStatus.Running) with
        {
            NodeName = "n", StartedAt = now.AddMinutes(-30), LastHeartbeatAt = now.AddMinutes(-30)
        };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var first = await Store.GetStaleRunningRunIdsAsync(now.AddMinutes(-5), 10, ct);
        Assert.Single(first);

        var transition = RunStatusTransition.RunningToPending(
            run.Id, run.Attempt, run.NotBefore, run.Reason, run.Result, run.Progress, run.LastHeartbeatAt);
        var result = await Store.TryTransitionRunAsync(transition, ct);
        Assert.True(result.Transitioned);

        var second = await Store.GetStaleRunningRunIdsAsync(now.AddMinutes(-5), 10, ct);
        Assert.Empty(second);
    }

    [Fact]
    public async Task MaxActiveForJob_DecrementsOnTerminalTransition()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"NTTerminalDec_{Guid.CreateVersion7():N}";
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var first = CreateRun(jobName);
        Assert.True(await Store.TryCreateRunAsync(first, 1, cancellationToken: ct));

        var second = CreateRun(jobName);
        Assert.False(await Store.TryCreateRunAsync(second, 1, cancellationToken: ct));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var toRunning = CreateRun(jobName, JobStatus.Running) with
        {
            Id = first.Id, NodeName = "n", StartedAt = now, LastHeartbeatAt = now, Attempt = first.Attempt
        };
        Assert.True((await Store.TryTransitionRunAsync(Transition(toRunning, JobStatus.Pending), ct)).Transitioned);

        var toTerminal = toRunning with
        {
            Status = JobStatus.Succeeded, CompletedAt = now, Attempt = toRunning.Attempt
        };
        Assert.True((await Store.TryTransitionRunAsync(Transition(toTerminal, JobStatus.Running), ct)).Transitioned);

        var third = CreateRun(jobName);
        Assert.True(await Store.TryCreateRunAsync(third, 1, cancellationToken: ct));
    }

    [Fact]
    public async Task MaxActiveForJob_DecrementsOnCancel()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"NTCancelDec_{Guid.CreateVersion7():N}";
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var first = CreateRun(jobName);
        Assert.True(await Store.TryCreateRunAsync(first, 1, cancellationToken: ct));

        var blocked = CreateRun(jobName);
        Assert.False(await Store.TryCreateRunAsync(blocked, 1, cancellationToken: ct));

        Assert.True((await Store.TryCancelRunAsync(first.Id, cancellationToken: ct)).Transitioned);

        var third = CreateRun(jobName);
        Assert.True(await Store.TryCreateRunAsync(third, 1, cancellationToken: ct));
    }

    [Fact]
    public async Task MaxActiveForJob_DecrementsOnPurgeOfPending()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"NTPurgeDec_{Guid.CreateVersion7():N}";
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var old = TruncateToMilliseconds(DateTimeOffset.UtcNow.AddHours(-2));
        var abandoned = CreateRun(jobName) with { CreatedAt = old, NotBefore = old };
        Assert.True(await Store.TryCreateRunAsync(abandoned, 1, cancellationToken: ct));

        var blocked = CreateRun(jobName);
        Assert.False(await Store.TryCreateRunAsync(blocked, 1, cancellationToken: ct));

        await Store.PurgeAsync(DateTimeOffset.UtcNow.AddHours(-1), ct);

        var fresh = CreateRun(jobName);
        Assert.True(await Store.TryCreateRunAsync(fresh, 1, cancellationToken: ct));
    }
}
