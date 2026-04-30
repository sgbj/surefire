namespace Surefire.Tests.Conformance;

public abstract class BatchConformanceTests : StoreConformanceBase
{
    private JobBatch CreateBatch(int total, string? id = null)
    {
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        return new()
        {
            Id = id ?? Guid.CreateVersion7().ToString("N"),
            Status = JobStatus.Running,
            Total = total,
            Succeeded = 0,
            Failed = 0,
            CreatedAt = now
        };
    }

    private async Task<(JobBatch Batch, JobRun[] Runs)> CreateBatchWithRunsAsync(int childCount, CancellationToken ct)
    {
        var jobName = $"BatchJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var batch = CreateBatch(childCount);
        var runs = Enumerable.Range(0, childCount)
            .Select(_ => CreateRun(jobName) with { BatchId = batch.Id })
            .ToArray();

        await Store.CreateBatchAsync(batch, runs, cancellationToken: ct);
        return (batch, runs);
    }

    [Fact]
    public async Task CreateBatch_Persists_JobBatch()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batch, _) = await CreateBatchWithRunsAsync(3, ct);

        var stored = await Store.GetBatchAsync(batch.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(batch.Id, stored.Id);
        Assert.Equal(3, stored.Total);
        Assert.Equal(0, stored.Succeeded);
        Assert.Equal(0, stored.Failed);
    }

    [Fact]
    public async Task CreateBatch_Persists_ChildRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batch, runs) = await CreateBatchWithRunsAsync(3, ct);

        foreach (var run in runs)
        {
            var stored = await Store.GetRunAsync(run.Id, ct);
            Assert.NotNull(stored);
            Assert.Equal(batch.Id, stored.BatchId);
        }
    }

    [Fact]
    public async Task CreateBatch_WithInitialEvents_PersistsEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BatchJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var batch = CreateBatch(1);
        var run = CreateRun(jobName) with { BatchId = batch.Id };

        var evt = new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.Output,
            Payload = "hello",
            CreatedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow),
            Attempt = 1
        };

        await Store.CreateBatchAsync(batch, [run], [evt], ct);

        var events = await Store.GetEventsAsync(run.Id, cancellationToken: ct);
        Assert.Single(events);
        Assert.Equal("hello", events[0].Payload);
    }

    [Fact]
    public async Task CreateBatch_EmptyRuns_Persists_BatchOnly()
    {
        var ct = TestContext.Current.CancellationToken;
        var batch = CreateBatch(0);
        await Store.CreateBatchAsync(batch, [], cancellationToken: ct);

        var stored = await Store.GetBatchAsync(batch.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(0, stored.Total);
    }

    [Fact]
    public async Task GetBatch_NonExistent_ReturnsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        var result = await Store.GetBatchAsync(Guid.CreateVersion7().ToString("N"), ct);
        Assert.Null(result);
    }

    [Fact]
    public async Task CompleteBatch_Succeeded_SetsStatus()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batch, _) = await CreateBatchWithRunsAsync(3, ct);
        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        var result = await Store.TryCompleteBatchAsync(batch.Id, JobStatus.Succeeded, completedAt, ct);
        Assert.True(result);

        var stored = await Store.GetBatchAsync(batch.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Succeeded, stored.Status);
        Assert.NotNull(stored.CompletedAt);
    }

    [Fact]
    public async Task CompleteBatch_Failed_SetsStatus()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batch, _) = await CreateBatchWithRunsAsync(3, ct);
        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        var result = await Store.TryCompleteBatchAsync(batch.Id, JobStatus.Failed, completedAt, ct);
        Assert.True(result);

        var stored = await Store.GetBatchAsync(batch.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Failed, stored.Status);
    }

    [Fact]
    public async Task CompleteBatch_AlreadyTerminal_ReturnsFalse()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batch, _) = await CreateBatchWithRunsAsync(2, ct);
        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        var first = await Store.TryCompleteBatchAsync(batch.Id, JobStatus.Succeeded, completedAt, ct);
        Assert.True(first);

        var second = await Store.TryCompleteBatchAsync(batch.Id, JobStatus.Failed, completedAt, ct);
        Assert.False(second);

        // Status should remain Succeeded (first call wins)
        var stored = await Store.GetBatchAsync(batch.Id, ct);
        Assert.Equal(JobStatus.Succeeded, stored!.Status);
    }

    [Fact]
    public async Task CompleteBatch_NonExistent_ReturnsFalse()
    {
        var ct = TestContext.Current.CancellationToken;
        var result = await Store.TryCompleteBatchAsync(Guid.CreateVersion7().ToString("N"), JobStatus.Succeeded,
            DateTimeOffset.UtcNow, ct);
        Assert.False(result);
    }

    [Fact]
    public async Task CancelBatchRuns_CancelsAllPendingRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batch, runs) = await CreateBatchWithRunsAsync(5, ct);

        await Store.CancelBatchRunsAsync(batch.Id, cancellationToken: ct);

        foreach (var run in runs)
        {
            var stored = await Store.GetRunAsync(run.Id, ct);
            Assert.NotNull(stored);
            Assert.Equal(JobStatus.Canceled, stored.Status);
        }
    }

    [Fact]
    public async Task CancelBatchRuns_DoesNotCancelAlreadyTerminalRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BatchJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var batch = CreateBatch(2);
        var pendingRun = CreateRun(jobName) with { BatchId = batch.Id };

        var succeededRun = CreateRun(jobName, JobStatus.Running) with
        {
            BatchId = batch.Id,
            Attempt = 1,
            NodeName = "node1",
            StartedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow),
            LastHeartbeatAt = TruncateToMilliseconds(DateTimeOffset.UtcNow)
        };

        await Store.CreateBatchAsync(batch, [pendingRun, succeededRun], cancellationToken: ct);

        succeededRun = succeededRun with
        {
            Status = JobStatus.Succeeded, CompletedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow)
        };
        await Store.TryTransitionRunAsync(Transition(succeededRun, JobStatus.Running), ct);

        await Store.CancelBatchRunsAsync(batch.Id, cancellationToken: ct);

        var storedPending = await Store.GetRunAsync(pendingRun.Id, ct);
        Assert.Equal(JobStatus.Canceled, storedPending!.Status);

        var storedSucceeded = await Store.GetRunAsync(succeededRun.Id, ct);
        Assert.Equal(JobStatus.Succeeded, storedSucceeded!.Status);
    }

    [Fact]
    public async Task CancelBatchRuns_NonExistentBatch_NoError()
    {
        var ct = TestContext.Current.CancellationToken;
        await Store.CancelBatchRunsAsync(Guid.CreateVersion7().ToString("N"), cancellationToken: ct);
    }

    [Fact]
    public async Task CancelBatchRuns_CanceledChildren_ContributeToJobStats()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BatchStats_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var batch = CreateBatch(2);
        var runA = CreateRun(jobName) with { BatchId = batch.Id };
        var runB = CreateRun(jobName) with { BatchId = batch.Id };
        await Store.CreateBatchAsync(batch, [runA, runB], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);
        var pendingRunId = claimed.Id == runA.Id ? runB.Id : runA.Id;

        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        Assert.True((await Store.TryTransitionRunAsync(
            RunStatusTransition.RunningToSucceeded(
                claimed.Id,
                claimed.Attempt,
                completedAt,
                claimed.NotBefore,
                claimed.NodeName,
                1,
                null,
                null,
                claimed.StartedAt,
                claimed.LastHeartbeatAt), ct)).Transitioned);

        await Store.CancelBatchRunsAsync(batch.Id, cancellationToken: ct);

        var storedPending = await Store.GetRunAsync(pendingRunId, ct);
        Assert.NotNull(storedPending);
        Assert.Equal(JobStatus.Canceled, storedPending.Status);

        var jobStats = await Store.GetJobStatsAsync(jobName, ct);
        Assert.Equal(2, jobStats.TotalRuns);
        Assert.Equal(1, jobStats.SucceededRuns);
        Assert.Equal(0.5, jobStats.SuccessRate, 5);
    }

    [Fact]
    public async Task GetCompletableBatchIds_AllRunsTerminal_BatchCompletedAtomically_NotReturned()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BatchJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var batch = CreateBatch(2);
        var run1 = CreateRun(jobName, JobStatus.Running) with
        {
            BatchId = batch.Id, Attempt = 1, NodeName = "node1", StartedAt = now, LastHeartbeatAt = now
        };
        var run2 = CreateRun(jobName, JobStatus.Running) with
        {
            BatchId = batch.Id, Attempt = 1, NodeName = "node1", StartedAt = now, LastHeartbeatAt = now
        };
        await Store.CreateBatchAsync(batch, [run1, run2], cancellationToken: ct);

        // Transition both runs to Succeeded; the atomic batch counter should complete the batch.
        var run1Succeeded = run1 with { Status = JobStatus.Succeeded, CompletedAt = now };
        await Store.TryTransitionRunAsync(Transition(run1Succeeded, JobStatus.Running), ct);
        var run2Succeeded = run2 with { Status = JobStatus.Succeeded, CompletedAt = now };
        var result = await Store.TryTransitionRunAsync(Transition(run2Succeeded, JobStatus.Running), ct);

        // Batch already completed atomically; not returned as "completable".
        Assert.NotNull(result.BatchCompletion);
        var ids = await Store.GetCompletableBatchIdsAsync(ct);
        Assert.DoesNotContain(batch.Id, ids);
    }

    [Fact]
    public async Task GetCompletableBatchIds_BatchAlreadyTerminal_NotReturned()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batch, _) = await CreateBatchWithRunsAsync(2, ct);
        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        await Store.TryCompleteBatchAsync(batch.Id, JobStatus.Succeeded, completedAt, ct);

        var ids = await Store.GetCompletableBatchIdsAsync(ct);

        Assert.DoesNotContain(batch.Id, ids);
    }

    [Fact]
    public async Task GetCompletableBatchIds_BatchWithNonTerminalRun_NotReturned()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batch, _) = await CreateBatchWithRunsAsync(2, ct);

        // No runs are terminal; batch should not appear.
        var ids = await Store.GetCompletableBatchIdsAsync(ct);

        Assert.DoesNotContain(batch.Id, ids);
    }

    [Fact]
    public async Task GetRunsByIds_ReturnsMatchingRuns_PreservingOrder()
    {
        // Bulk-fetch primitive used by StreamBatchAsync<T> / WaitBatchAsync<T>.
        // Every store must satisfy the ordered-by-input-ids, missing-ids-omitted contract.
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"GetByIds_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var runs = new List<JobRun>();
        for (var i = 0; i < 5; i++)
        {
            var run = CreateRun(jobName);
            runs.Add(run);
            await Store.CreateRunsAsync([run], cancellationToken: ct);
        }

        // Deliberately out-of-insert-order plus a bogus id that should be omitted.
        var requestedIds = new[] { runs[3].Id, runs[1].Id, "nonexistent", runs[0].Id };
        var fetched = await Store.GetRunsByIdsAsync(requestedIds, ct);

        Assert.Equal(3, fetched.Count);
        Assert.Equal(runs[3].Id, fetched[0].Id);
        Assert.Equal(runs[1].Id, fetched[1].Id);
        Assert.Equal(runs[0].Id, fetched[2].Id);
    }

    [Fact]
    public async Task GetRunsByIds_EmptyInput_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var fetched = await Store.GetRunsByIdsAsync([], ct);
        Assert.Empty(fetched);
    }

    [Fact]
    public async Task TransitionToTerminal_IncrementsBatchCounter_Atomically()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batch, runs) = await CreateBatchWithRunsAsync(3, ct);

        var claimed = (await Store.ClaimRunsAsync("node1", [runs[0].JobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var result = await Store.TryTransitionRunAsync(
            RunStatusTransition.RunningToSucceeded(
                claimed.Id,
                claimed.Attempt,
                completedAt,
                claimed.NotBefore,
                claimed.NodeName,
                1, null, null,
                claimed.StartedAt,
                claimed.LastHeartbeatAt), ct);

        Assert.True(result.Transitioned);

        // Batch counter should already be incremented; no separate call needed.
        var storedBatch = await Store.GetBatchAsync(batch.Id, ct);
        Assert.NotNull(storedBatch);
        Assert.Equal(1, storedBatch.Succeeded);
        Assert.False(storedBatch.IsTerminal);
    }

    [Fact]
    public async Task TransitionLastChild_CompletesBatch_Atomically()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batch, runs) = await CreateBatchWithRunsAsync(2, ct);

        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        for (var i = 0; i < 2; i++)
        {
            var claimed = (await Store.ClaimRunsAsync("node1", [runs[i].JobName], ["default"], 1, ct)).FirstOrDefault();
            Assert.NotNull(claimed);

            var result = await Store.TryTransitionRunAsync(
                RunStatusTransition.RunningToSucceeded(
                    claimed.Id,
                    claimed.Attempt,
                    completedAt,
                    claimed.NotBefore,
                    claimed.NodeName,
                    1, null, null,
                    claimed.StartedAt,
                    claimed.LastHeartbeatAt), ct);

            Assert.True(result.Transitioned);

            if (i == 1)
            {
                // Last child should trigger batch completion
                Assert.NotNull(result.BatchCompletion);
                Assert.Equal(batch.Id, result.BatchCompletion.BatchId);
                Assert.Equal(JobStatus.Succeeded, result.BatchCompletion.BatchStatus);
            }
        }

        var storedBatch = await Store.GetBatchAsync(batch.Id, ct);
        Assert.NotNull(storedBatch);
        Assert.True(storedBatch.IsTerminal);
        Assert.Equal(JobStatus.Succeeded, storedBatch.Status);
        Assert.Equal(2, storedBatch.Succeeded);
    }

    [Fact]
    public async Task TransitionLastChild_WithFailure_BatchStatusIsFailed()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batch, runs) = await CreateBatchWithRunsAsync(2, ct);

        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        var claimed1 = (await Store.ClaimRunsAsync("node1", [runs[0].JobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed1);
        await Store.TryTransitionRunAsync(
            RunStatusTransition.RunningToSucceeded(
                claimed1.Id, claimed1.Attempt, completedAt, claimed1.NotBefore, claimed1.NodeName,
                1, null, null, claimed1.StartedAt, claimed1.LastHeartbeatAt), ct);

        var claimed2 = (await Store.ClaimRunsAsync("node1", [runs[1].JobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed2);
        var result = await Store.TryTransitionRunAsync(
            RunStatusTransition.RunningToFailed(
                claimed2.Id, claimed2.Attempt, completedAt, claimed2.NotBefore, claimed2.NodeName,
                1, "test error", null, claimed2.StartedAt, claimed2.LastHeartbeatAt), ct);

        Assert.True(result.Transitioned);
        Assert.NotNull(result.BatchCompletion);
        Assert.Equal(JobStatus.Failed, result.BatchCompletion.BatchStatus);
    }

    [Fact]
    public async Task TransitionNonBatchRun_ReturnsNullBatchCompletion()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"NonBatch_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        var run = CreateRun(jobName);
        await Store.TryCreateRunAsync(run, cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        var result = await Store.TryTransitionRunAsync(
            RunStatusTransition.RunningToSucceeded(
                claimed.Id, claimed.Attempt, TruncateToMilliseconds(DateTimeOffset.UtcNow),
                claimed.NotBefore, claimed.NodeName, 1, null, null,
                claimed.StartedAt, claimed.LastHeartbeatAt), ct);

        Assert.True(result.Transitioned);
        Assert.Null(result.BatchCompletion);
    }
}
