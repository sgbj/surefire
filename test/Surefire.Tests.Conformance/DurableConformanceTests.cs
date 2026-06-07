namespace Surefire.Tests.Conformance;

public abstract class DurableConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task SuspendedDurableRun_DoesNotConsumeJobConcurrencySlot()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DurableSlotJob_{Guid.CreateVersion7():N}";
        var childJobName = $"DurableSlotChildJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([
            new() { Name = jobName, Queue = "default", IsDurable = true, MaxConcurrency = 1 },
            new() { Name = childJobName, Queue = "default" }
        ], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var suspendedRunId = await SeedRunningOrchestratorAsync(jobName, ct);
        var childId = await SeedPendingChildAsync(childJobName, suspendedRunId, ct);
        var suspended = await Store.GetRunAsync(suspendedRunId, ct);
        Assert.NotNull(suspended);
        var suspend = await Store.TrySuspendRunAsync(suspendedRunId, suspended.LeaseEpoch, [childId], [],
            DateTimeOffset.UtcNow, ct);
        Assert.Equal(DurableSuspendOutcome.Suspended, suspend);

        var nextRun = CreateRun(jobName) with { IsDurable = true };
        nextRun = nextRun with { RootRunId = nextRun.Id };
        await Store.TryCreateRunAsync(nextRun, cancellationToken: ct);

        var claimed = await Store.ClaimRunsAsync("node-2", [jobName], ["default"], 1, ct);
        var claimedRun = Assert.Single(claimed);
        Assert.Equal(nextRun.Id, claimedRun.Id);
    }

    [Fact]
    public async Task WakingSuspendedDurableRun_DoesNotUndercountJobConcurrency()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DurableWakeSlotJob_{Guid.CreateVersion7():N}";
        var childJobName = $"DurableWakeChildJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([
            new() { Name = jobName, Queue = "default", IsDurable = true, MaxConcurrency = 1 },
            new() { Name = childJobName, Queue = "default" }
        ], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var orchId = await SeedRunningOrchestratorAsync(jobName, ct);
        var childId = await SeedPendingChildAsync(childJobName, orchId, ct);
        var orch = await Store.GetRunAsync(orchId, ct);
        Assert.NotNull(orch);
        var suspend = await Store.TrySuspendRunAsync(orchId, orch.LeaseEpoch, [childId], [], DateTimeOffset.UtcNow,
            ct);
        Assert.Equal(DurableSuspendOutcome.Suspended, suspend);

        await TerminateChildrenAsync([childId], ct);
        var claimed = await Store.ClaimRunsAsync("node-2", [jobName], ["default"], 1, ct);
        Assert.Equal(orchId, Assert.Single(claimed).Id);

        var extra = CreateRun(jobName) with { IsDurable = true };
        extra = extra with { RootRunId = extra.Id };
        await Store.TryCreateRunAsync(extra, cancellationToken: ct);

        var blocked = await Store.ClaimRunsAsync("node-3", [jobName], ["default"], 1, ct);
        Assert.Empty(blocked);
    }

    [Fact]
    public async Task CancelExpiredRuns_SkipsStartedSuspendedDurableRun()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DurableExpiredJob_{Guid.CreateVersion7():N}";
        var childJobName = $"DurableExpiredChildJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([
            new() { Name = jobName, Queue = "default", IsDurable = true },
            new() { Name = childJobName, Queue = "default" }
        ], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var orchId = await SeedRunningOrchestratorAsync(jobName, ct,
            TruncateToMilliseconds(DateTimeOffset.UtcNow.AddSeconds(-1)));
        var childId = await SeedPendingChildAsync(childJobName, orchId, ct);
        var orch = await Store.GetRunAsync(orchId, ct);
        Assert.NotNull(orch);

        var suspend = await Store.TrySuspendRunAsync(orchId, orch.LeaseEpoch, [childId], [], DateTimeOffset.UtcNow,
            ct);
        Assert.Equal(DurableSuspendOutcome.Suspended, suspend);

        var canceled = await Store.CancelExpiredRunsWithIdsAsync(ct);
        Assert.DoesNotContain(canceled.Runs, r => r.RunId == orchId);

        var current = await Store.GetRunAsync(orchId, ct);
        Assert.NotNull(current);
        Assert.Equal(JobStatus.Suspended, current.Status);
    }

    [Fact]
    public async Task CancelExpiredRuns_CancelsSuspendedDurableRunPastExpiresAt()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DurableLifetimeExpiredJob_{Guid.CreateVersion7():N}";
        var childJobName = $"DurableLifetimeExpiredChildJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([
            new() { Name = jobName, Queue = "default", IsDurable = true },
            new() { Name = childJobName, Queue = "default" }
        ], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var orchId = await SeedRunningOrchestratorAsync(jobName, ct,
            expiresAt: TruncateToMilliseconds(DateTimeOffset.UtcNow.AddMinutes(-1)));
        var childId = await SeedPendingChildAsync(childJobName, orchId, ct);
        var orch = await Store.GetRunAsync(orchId, ct);
        Assert.NotNull(orch);

        var suspend = await Store.TrySuspendRunAsync(orchId, orch.LeaseEpoch, [childId], [], DateTimeOffset.UtcNow,
            ct);
        Assert.Equal(DurableSuspendOutcome.Suspended, suspend);

        var canceled = await Store.CancelExpiredRunsWithIdsAsync(ct);

        Assert.Contains(canceled.Runs, r => r.RunId == orchId);
        var current = await Store.GetRunAsync(orchId, ct);
        Assert.NotNull(current);
        Assert.Equal(JobStatus.Canceled, current.Status);
    }

    // Concurrent terminals of children awaited by overlapping orchestrator sets must
    // not deadlock, must not surface a non-transient error, and the wait set must drain.
    [Fact]
    public async Task ConcurrentTerminals_WithOverlappingOrchestratorAwaits_DoesNotDeadlockOrLeak()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DurableOrchJob_{Guid.CreateVersion7():N}";
        var childJobName = $"DurableChildJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([
            new() { Name = jobName, Queue = "default", IsDurable = true },
            new() { Name = childJobName, Queue = "default" }
        ], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var orchA = await SeedRunningOrchestratorAsync(jobName, ct);
        var orchB = await SeedRunningOrchestratorAsync(jobName, ct);
        var children = new List<string>();
        for (var i = 0; i < 8; i++)
        {
            children.Add(await SeedPendingChildAsync(childJobName, orchA, ct));
        }

        var orchARun = await Store.GetRunAsync(orchA, ct);
        var orchBRun = await Store.GetRunAsync(orchB, ct);
        Assert.NotNull(orchARun);
        Assert.NotNull(orchBRun);
        await Store.TrySuspendRunAsync(orchA, orchARun.LeaseEpoch, children, [], DateTimeOffset.UtcNow,
            ct);
        await Store.TrySuspendRunAsync(orchB, orchBRun.LeaseEpoch, children, [], DateTimeOffset.UtcNow,
            ct);

        // Two threads each cancel half the children concurrently. With wrong global lock
        // order across run-and-batch terminals the wakes would deadlock.
        var t1 = Task.Run(() => TerminateChildrenAsync(children.Take(4), ct), ct);
        var t2 = Task.Run(() => TerminateChildrenAsync(children.Skip(4), ct), ct);
        await Task.WhenAll(t1, t2).WaitAsync(TimeSpan.FromSeconds(30), ct);

        var a = await Store.GetRunAsync(orchA, ct);
        var b = await Store.GetRunAsync(orchB, ct);
        Assert.NotNull(a);
        Assert.NotNull(b);
        Assert.NotEqual(JobStatus.Suspended, a.Status);
        Assert.NotEqual(JobStatus.Suspended, b.Status);
    }

    // LoadExecutionSnapshotAsync must preserve the durable replay history for both direct
    // child runs and child batches. Completed child batches are still replay history, not
    // active-only state, so they must remain in the snapshot until retention purge.
    [Fact]
    public async Task LoadExecutionSnapshot_UnderConcurrentChildTerminal_IsConsistent()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"SnapJob_{Guid.CreateVersion7():N}";
        var childJobName = $"SnapChild_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([
            new() { Name = jobName, Queue = "default", IsDurable = true },
            new() { Name = childJobName, Queue = "default" }
        ], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var orchId = await SeedRunningOrchestratorAsync(jobName, ct);
        var children = new List<string>();
        for (var i = 0; i < 16; i++)
        {
            children.Add(await SeedPendingChildAsync(childJobName, orchId, ct));
        }

        var (childBatchId, childBatchRunIds) = await SeedPendingChildBatchAsync(childJobName, orchId, 3, ct);
        children.AddRange(childBatchRunIds);

        using var stop = new CancellationTokenSource();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stop.Token, ct);
        var terminator = Task.Run(() => TerminateChildrenAsync(children, linked.Token), linked.Token);

        while (!terminator.IsCompleted)
        {
            var snap = await Store.LoadExecutionSnapshotAsync(orchId, ct);
            // Sanity: snapshot's children count never exceeds the seeded count, and
            // every child's status is one of the expected lifecycle values.
            Assert.True(snap.Children.Count <= children.Count);
            Assert.True(snap.ChildBatches.Count <= 1);
            foreach (var (_, child) in snap.Children)
            {
                Assert.True(child.Status is JobStatus.Pending or JobStatus.Running
                    or JobStatus.Succeeded or JobStatus.Failed or JobStatus.Canceled);
            }

            foreach (var (_, childBatch) in snap.ChildBatches)
            {
                Assert.Equal(orchId, childBatch.ParentRunId);
                Assert.Equal(childBatch.Total, snap.Children.Values.Count(c => c.BatchId == childBatch.Id));
            }

            await Task.Yield();
        }

        await stop.CancelAsync();
        await terminator;

        // Final snapshot after every child is terminal.
        var finalSnap = await Store.LoadExecutionSnapshotAsync(orchId, ct);
        Assert.Equal(children.Count, finalSnap.Children.Count);
        foreach (var (_, child) in finalSnap.Children)
        {
            Assert.True(child.Status.IsTerminal);
        }

        Assert.True(finalSnap.ChildBatches.TryGetValue(childBatchId, out var finalBatch),
            "Completed child batches must stay in the durable replay snapshot.");
        Assert.Equal(JobStatus.Succeeded, finalBatch.Status);
        Assert.Equal(finalBatch.Total, finalSnap.Children.Values.Count(c => c.BatchId == finalBatch.Id));
        Assert.Equal(finalBatch.Total,
            finalBatch.Succeeded + finalBatch.Failed + finalBatch.Canceled);
    }

    [Fact]
    public async Task DurableRecord_Create_Load_IsIdempotent_AndDetectsMismatch()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DurableRecordJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([new() { Name = jobName, Queue = "default", IsDurable = true }], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var orchId = await SeedRunningOrchestratorAsync(jobName, ct);
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var record = new DurableRecord(orchId, 1, DurableRecordKinds.Record, "tax-rate", "7", now);

        var created = await Store.CreateDurableRecordAsync(record, ct);
        var duplicate = await Store.CreateDurableRecordAsync(record, ct);
        var duplicateWithDifferentCreatedAt = await Store.CreateDurableRecordAsync(
            record with { CreatedAt = now.AddMilliseconds(1) }, ct);

        Assert.Equal(record, created);
        Assert.Equal(record, duplicate);
        Assert.Equal(record, duplicateWithDifferentCreatedAt);

        var snapshot = await Store.LoadExecutionSnapshotAsync(orchId, ct);
        Assert.Equal(1, snapshot.HighestRecordedStep);
        Assert.True(snapshot.Records.TryGetValue(1, out var stored));
        Assert.Equal(record, stored);

        var mismatch = record with { Payload = "8" };
        await Assert.ThrowsAsync<DurableReplayMismatchException>(() => Store.CreateDurableRecordAsync(mismatch, ct));
    }

    [Fact]
    public async Task DurableRecord_Purge_RemovesRecordsWithOrchestrator()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DurableRecordPurgeJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([new() { Name = jobName, Queue = "default", IsDurable = true }], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var orchId = await SeedRunningOrchestratorAsync(jobName, ct);
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        await Store.CreateDurableRecordAsync(
            new(orchId, 1, DurableRecordKinds.UtcNow, null, "\"2025-01-01T00:00:00Z\"", now),
            ct);

        var run = await Store.GetRunAsync(orchId, ct);
        Assert.NotNull(run);
        var completed = RunStatusTransition.RunningToSucceeded(
            orchId, run.LeaseEpoch, now, now.AddSeconds(-1), run.NodeName, 1, "{}");
        var transition = await Store.TryTransitionRunAsync(completed, ct);
        Assert.True(transition.Transitioned);

        await Store.PurgeAsync(now.AddDays(1), ct);

        Assert.Null(await Store.GetRunAsync(orchId, ct));
        var snapshot = await Store.LoadExecutionSnapshotAsync(orchId, ct);
        Assert.Empty(snapshot.Records);
    }

    private async Task<string> SeedRunningOrchestratorAsync(string jobName, CancellationToken ct,
        DateTimeOffset? notAfter = null, DateTimeOffset? expiresAt = null)
    {
        var orchId = Guid.CreateVersion7().ToString("N");
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run = new JobRun
        {
            Id = orchId,
            JobName = jobName,
            Status = JobStatus.Pending,
            CreatedAt = now,
            NotBefore = now.AddSeconds(-1),
            NotAfter = notAfter,
            ExpiresAt = expiresAt,
            IsDurable = true,
            Attempt = 1,
            RootRunId = orchId
        };
        await Store.TryCreateRunAsync(run, cancellationToken: ct);

        var startedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var transition = RunStatusTransition.PendingToRunning(
            orchId, 0, "test-node", startedAt, startedAt, startedAt);
        var result = await Store.TryTransitionRunAsync(transition, ct);
        Assert.True(result.Transitioned);
        return orchId;
    }

    private async Task<string> SeedPendingChildAsync(string childJobName, string parentRunId,
        CancellationToken ct)
    {
        var childId = Guid.CreateVersion7().ToString("N");
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var child = new JobRun
        {
            Id = childId,
            JobName = childJobName,
            Status = JobStatus.Pending,
            CreatedAt = now,
            NotBefore = now.AddSeconds(-1),
            ParentRunId = parentRunId,
            RootRunId = parentRunId,
            Attempt = 1
        };
        await Store.TryCreateRunAsync(child, cancellationToken: ct);
        return childId;
    }

    private async Task<(string BatchId, IReadOnlyList<string> RunIds)> SeedPendingChildBatchAsync(
        string childJobName, string parentRunId, int childCount, CancellationToken ct)
    {
        var batchId = Guid.CreateVersion7().ToString("N");
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var batch = new JobBatch
        {
            Id = batchId,
            Status = JobStatus.Running,
            Total = childCount,
            CreatedAt = now,
            ParentRunId = parentRunId
        };

        var runs = Enumerable.Range(0, childCount)
            .Select(_ => new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = childJobName,
                Status = JobStatus.Pending,
                CreatedAt = now,
                NotBefore = now.AddSeconds(-1),
                ParentRunId = parentRunId,
                RootRunId = parentRunId,
                BatchId = batchId,
                Attempt = 1
            })
            .ToArray();

        await Store.CreateBatchAsync(batch, runs, cancellationToken: ct);
        return (batchId, runs.Select(r => r.Id).ToArray());
    }

    private async Task TerminateChildrenAsync(IEnumerable<string> childIds, CancellationToken ct)
    {
        foreach (var id in childIds)
        {
            if (ct.IsCancellationRequested)
            {
                return;
            }

            var toFinish = await Store.GetRunAsync(id, ct);
            if (toFinish is null || toFinish.Status.IsTerminal)
            {
                continue;
            }

            var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
            if (toFinish.Status == JobStatus.Pending)
            {
                var toRunning = RunStatusTransition.PendingToRunning(
                    toFinish.Id, toFinish.LeaseEpoch, "tester",
                    now, now, now.AddSeconds(-1));
                var ok = await Store.TryTransitionRunAsync(toRunning, ct);
                if (!ok.Transitioned)
                {
                    continue;
                }

                toFinish = await Store.GetRunAsync(id, ct);
                if (toFinish is null || toFinish.Status != JobStatus.Running)
                {
                    continue;
                }
            }

            var terminal = RunStatusTransition.RunningToSucceeded(
                toFinish.Id, toFinish.LeaseEpoch, now, now.AddSeconds(-1),
                toFinish.NodeName, 1, "{}");
            await Store.TryTransitionRunAsync(terminal, ct);
        }
    }
}
