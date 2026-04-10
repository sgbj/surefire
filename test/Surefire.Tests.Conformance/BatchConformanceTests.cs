namespace Surefire.Tests.Conformance;

public abstract class BatchConformanceTests : StoreConformanceBase
{
    private BatchRecord CreateBatch(int total, string? id = null)
    {
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        return new BatchRecord
        {
            Id = id ?? Guid.CreateVersion7().ToString("N"),
            Status = JobStatus.Running,
            Total = total,
            Succeeded = 0,
            Failed = 0,
            CreatedAt = now
        };
    }

    private async Task<(BatchRecord Batch, RunRecord[] Runs)> CreateBatchWithRunsAsync(int childCount)
    {
        var jobName = $"BatchJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var batch = CreateBatch(childCount);
        var runs = Enumerable.Range(0, childCount)
            .Select(_ =>
            {
                var run = CreateRun(jobName);
                run.BatchId = batch.Id;
                return run;
            })
            .ToArray();

        await Store.CreateBatchAsync(batch, runs);
        return (batch, runs);
    }

    // ── CreateBatchAsync ──────────────────────────────────────────────────────

    [Fact]
    public async Task CreateBatch_Persists_BatchRecord()
    {
        var (batch, _) = await CreateBatchWithRunsAsync(3);

        var stored = await Store.GetBatchAsync(batch.Id);
        Assert.NotNull(stored);
        Assert.Equal(batch.Id, stored.Id);
        Assert.Equal(3, stored.Total);
        Assert.Equal(0, stored.Succeeded);
        Assert.Equal(0, stored.Failed);
    }

    [Fact]
    public async Task CreateBatch_Persists_ChildRuns()
    {
        var (batch, runs) = await CreateBatchWithRunsAsync(3);

        foreach (var run in runs)
        {
            var stored = await Store.GetRunAsync(run.Id);
            Assert.NotNull(stored);
            Assert.Equal(batch.Id, stored.BatchId);
        }
    }

    [Fact]
    public async Task CreateBatch_WithInitialEvents_PersistsEvents()
    {
        var jobName = $"BatchJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var batch = CreateBatch(1);
        var run = CreateRun(jobName);
        run.BatchId = batch.Id;

        var evt = new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.Output,
            Payload = "hello",
            CreatedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow),
            Attempt = 1
        };

        await Store.CreateBatchAsync(batch, [run], [evt]);

        var events = await Store.GetEventsAsync(run.Id);
        Assert.Single(events);
        Assert.Equal("hello", events[0].Payload);
    }

    [Fact]
    public async Task CreateBatch_EmptyRuns_Persists_BatchOnly()
    {
        var batch = CreateBatch(0);
        await Store.CreateBatchAsync(batch, []);

        var stored = await Store.GetBatchAsync(batch.Id);
        Assert.NotNull(stored);
        Assert.Equal(0, stored.Total);
    }

    // ── GetBatchAsync ─────────────────────────────────────────────────────────

    [Fact]
    public async Task GetBatch_NonExistent_ReturnsNull()
    {
        var result = await Store.GetBatchAsync(Guid.CreateVersion7().ToString("N"));
        Assert.Null(result);
    }

    // ── TryIncrementBatchProgressAsync ────────────────────────────────────────

    [Fact]
    public async Task IncrementBatchProgress_Succeeded_IncrementsSucceeded()
    {
        var (batch, _) = await CreateBatchWithRunsAsync(5);

        var counters = await Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Succeeded);
        Assert.NotNull(counters);

        Assert.Equal(5, counters.Value.Total);
        Assert.Equal(1, counters.Value.Succeeded);
        Assert.Equal(0, counters.Value.Failed);
    }

    [Fact]
    public async Task IncrementBatchProgress_Failed_IncrementsFailed()
    {
        var (batch, _) = await CreateBatchWithRunsAsync(5);

        var counters = await Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Failed);
        Assert.NotNull(counters);

        Assert.Equal(5, counters.Value.Total);
        Assert.Equal(0, counters.Value.Succeeded);
        Assert.Equal(1, counters.Value.Failed);
    }

    [Fact]
    public async Task IncrementBatchProgress_Cancelled_IncrementsCancelled()
    {
        var (batch, _) = await CreateBatchWithRunsAsync(5);

        var counters = await Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Cancelled);
        Assert.NotNull(counters);

        Assert.Equal(5, counters.Value.Total);
        Assert.Equal(0, counters.Value.Succeeded);
        Assert.Equal(0, counters.Value.Failed);
        Assert.Equal(1, counters.Value.Cancelled);
    }

    [Fact]
    public async Task IncrementBatchProgress_MixedStatuses_TracksSeparateCounters()
    {
        var (batch, _) = await CreateBatchWithRunsAsync(10);

        await Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Succeeded);
        await Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Failed);
        var counters = await Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Cancelled);

        Assert.NotNull(counters);
        Assert.Equal(10, counters.Value.Total);
        Assert.Equal(1, counters.Value.Succeeded);
        Assert.Equal(1, counters.Value.Failed);
        Assert.Equal(1, counters.Value.Cancelled);
    }

    [Fact]
    public async Task IncrementBatchProgress_Returns_PostIncrementValues()
    {
        var (batch, _) = await CreateBatchWithRunsAsync(10);

        var c1 = await Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Succeeded);
        Assert.NotNull(c1);
        Assert.Equal(1, c1.Value.Succeeded);

        var c2 = await Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Succeeded);
        Assert.NotNull(c2);
        Assert.Equal(2, c2.Value.Succeeded);

        var c3 = await Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Failed);
        Assert.NotNull(c3);
        Assert.Equal(2, c3.Value.Succeeded);
        Assert.Equal(1, c3.Value.Failed);
    }

    [Fact]
    public async Task IncrementBatchProgress_Concurrent_NoLostIncrements()
    {
        const int total = 100;
        // Multiple trials help surface lost-increment races across store implementations.
        for (var trial = 0; trial < 20; trial++)
        {
            var (batch, _) = await CreateBatchWithRunsAsync(total);

            var tasks = Enumerable.Range(0, total)
                .Select(_ => Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Succeeded))
                .ToArray();

            await Task.WhenAll(tasks);

            var stored = await Store.GetBatchAsync(batch.Id);
            Assert.NotNull(stored);
            Assert.Equal(total, stored.Succeeded);
            Assert.Equal(0, stored.Failed);
        }
    }

    [Fact]
    public async Task IncrementBatchProgress_CrossesThreshold_ExactlyOnce()
    {
        const int total = 10;
        for (var trial = 0; trial < 20; trial++)
        {
            var (batch, _) = await CreateBatchWithRunsAsync(total);

            var tasks = Enumerable.Range(0, total)
                .Select(_ => Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Succeeded))
                .ToArray();

            var results = await Task.WhenAll(tasks);

            var crossedCount = results.Count(c => c is { } v && v.Succeeded + v.Failed + v.Cancelled == v.Total);
            Assert.Equal(1, crossedCount);
        }
    }

    [Fact]
    public async Task IncrementBatchProgress_AlreadyTerminal_ReturnsCurrentCounters()
    {
        var (batch, _) = await CreateBatchWithRunsAsync(3);

        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        await Store.TryCompleteBatchAsync(batch.Id, JobStatus.Succeeded, completedAt);

        // After batch is terminal, increment should still return counters (not null)
        var counters = await Store.TryIncrementBatchProgressAsync(batch.Id, JobStatus.Succeeded);
        Assert.NotNull(counters);
        Assert.Equal(3, counters.Value.Total);
    }

    [Fact]
    public async Task IncrementBatchProgress_NonExistent_ReturnsNull()
    {
        var result = await Store.TryIncrementBatchProgressAsync(Guid.CreateVersion7().ToString("N"), JobStatus.Succeeded);
        Assert.Null(result);
    }

    // ── TryCompleteBatchAsync ─────────────────────────────────────────────────

    [Fact]
    public async Task CompleteBatch_Succeeded_SetsStatus()
    {
        var (batch, _) = await CreateBatchWithRunsAsync(3);
        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        var result = await Store.TryCompleteBatchAsync(batch.Id, JobStatus.Succeeded, completedAt);
        Assert.True(result);

        var stored = await Store.GetBatchAsync(batch.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Succeeded, stored.Status);
        Assert.NotNull(stored.CompletedAt);
    }

    [Fact]
    public async Task CompleteBatch_Failed_SetsStatus()
    {
        var (batch, _) = await CreateBatchWithRunsAsync(3);
        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        var result = await Store.TryCompleteBatchAsync(batch.Id, JobStatus.Failed, completedAt);
        Assert.True(result);

        var stored = await Store.GetBatchAsync(batch.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Failed, stored.Status);
    }

    [Fact]
    public async Task CompleteBatch_AlreadyTerminal_ReturnsFalse()
    {
        var (batch, _) = await CreateBatchWithRunsAsync(2);
        var completedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        var first = await Store.TryCompleteBatchAsync(batch.Id, JobStatus.Succeeded, completedAt);
        Assert.True(first);

        var second = await Store.TryCompleteBatchAsync(batch.Id, JobStatus.Failed, completedAt);
        Assert.False(second);

        // Status should remain Succeeded (first call wins)
        var stored = await Store.GetBatchAsync(batch.Id);
        Assert.Equal(JobStatus.Succeeded, stored!.Status);
    }

    [Fact]
    public async Task CompleteBatch_NonExistent_ReturnsFalse()
    {
        var result = await Store.TryCompleteBatchAsync(Guid.CreateVersion7().ToString("N"), JobStatus.Succeeded, DateTimeOffset.UtcNow);
        Assert.False(result);
    }

    // ── CancelBatchRunsAsync ──────────────────────────────────────────────────

    [Fact]
    public async Task CancelBatchRuns_CancelsAllPendingRuns()
    {
        var (batch, runs) = await CreateBatchWithRunsAsync(5);

        await Store.CancelBatchRunsAsync(batch.Id);

        foreach (var run in runs)
        {
            var stored = await Store.GetRunAsync(run.Id);
            Assert.NotNull(stored);
            Assert.Equal(JobStatus.Cancelled, stored.Status);
        }
    }

    [Fact]
    public async Task CancelBatchRuns_DoesNotCancelAlreadyTerminalRuns()
    {
        var jobName = $"BatchJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var batch = CreateBatch(2);
        var pendingRun = CreateRun(jobName);
        pendingRun.BatchId = batch.Id;

        var succeededRun = CreateRun(jobName, JobStatus.Running);
        succeededRun.BatchId = batch.Id;
        succeededRun.Attempt = 1;
        succeededRun.NodeName = "node1";
        succeededRun.StartedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        succeededRun.LastHeartbeatAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        await Store.CreateBatchAsync(batch, [pendingRun, succeededRun]);

        succeededRun.Status = JobStatus.Succeeded;
        succeededRun.CompletedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        await Store.TryTransitionRunAsync(Transition(succeededRun, JobStatus.Running));

        await Store.CancelBatchRunsAsync(batch.Id);

        var storedPending = await Store.GetRunAsync(pendingRun.Id);
        Assert.Equal(JobStatus.Cancelled, storedPending!.Status);

        var storedSucceeded = await Store.GetRunAsync(succeededRun.Id);
        Assert.Equal(JobStatus.Succeeded, storedSucceeded!.Status);
    }

    [Fact]
    public async Task CancelBatchRuns_NonExistentBatch_NoError()
    {
        // Should complete without throwing
        await Store.CancelBatchRunsAsync(Guid.CreateVersion7().ToString("N"));
    }
}
