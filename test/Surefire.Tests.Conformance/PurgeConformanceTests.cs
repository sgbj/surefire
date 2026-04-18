namespace Surefire.Tests.Conformance;

public abstract class PurgeConformanceTests : StoreConformanceBase
{
    private DateTimeOffset OldTime => DateTimeOffset.UtcNow.AddDays(-30);
    private DateTimeOffset RecentTime => DateTimeOffset.UtcNow;
    private DateTimeOffset Threshold => DateTimeOffset.UtcNow.AddDays(-7);

    [Fact]
    public async Task Purge_DeletesTerminalRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"PurgeJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var run = CreateRun(jobName) with { CreatedAt = OldTime, NotBefore = OldTime };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);

        claimed = claimed with { Status = JobStatus.Succeeded, CompletedAt = OldTime };
        await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running), ct);

        await Store.AppendEventsAsync([
            new() { RunId = run.Id, EventType = RunEventType.Log, Payload = "test", CreatedAt = OldTime }
        ], ct);

        await Store.PurgeAsync(Threshold, ct);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.Null(loaded);

        var events = await Store.GetEventsAsync(run.Id, cancellationToken: ct);
        Assert.Empty(events);
    }

    [Fact]
    public async Task Purge_DeletesStaleJobs()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StaleJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var futureThreshold = DateTimeOffset.UtcNow.AddMinutes(5);
        await Store.PurgeAsync(futureThreshold, ct);

        var loaded = await Store.GetJobAsync(jobName, ct);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task Purge_DeletesStaleNodes()
    {
        var ct = TestContext.Current.CancellationToken;
        var nodeName = $"stale-node-{Guid.CreateVersion7():N}";
        await Store.HeartbeatAsync(nodeName, ["Job"], ["default"], [], ct);

        // The heartbeat sets LastHeartbeatAt to now, so we need a threshold in the future
        // to purge it, or we test with a stale node. Since HeartbeatAsync uses TimeProvider,
        // we purge with a future threshold for this test.
        var futureThreshold = DateTimeOffset.UtcNow.AddMinutes(5);
        await Store.PurgeAsync(futureThreshold, ct);

        var nodes = await Store.GetNodesAsync(ct);
        Assert.DoesNotContain(nodes, n => n.Name == nodeName);
    }

    [Fact]
    public async Task Purge_DeletesStaleQueues()
    {
        var ct = TestContext.Current.CancellationToken;
        var queueName = $"stale-queue-{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = queueName }, ct);

        var futureThreshold = DateTimeOffset.UtcNow.AddMinutes(5);
        await Store.PurgeAsync(futureThreshold, ct);

        var queues = await Store.GetQueuesAsync(ct);
        Assert.DoesNotContain(queues, q => q.Name == queueName);
    }

    [Fact]
    public async Task Purge_DeletesStaleRateLimits()
    {
        var ct = TestContext.Current.CancellationToken;
        var rlName = $"stale-rl-{Guid.CreateVersion7():N}";
        await Store.UpsertRateLimitAsync(new()
        {
            Name = rlName,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 10,
            Window = TimeSpan.FromMinutes(1)
        }, ct);

        var futureThreshold = DateTimeOffset.UtcNow.AddMinutes(5);
        await Store.PurgeAsync(futureThreshold, ct);

        var jobName = $"RLJob_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.RateLimitName = rlName;
        await Store.UpsertJobAsync(job, ct);
        await Store.UpsertQueueAsync(new() { Name = "default" }, ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);
    }

    [Fact]
    public async Task Purge_DeletesAbandonedPendingRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"AbandonedJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var run = CreateRun(jobName) with { CreatedAt = OldTime, NotBefore = OldTime };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        await Store.PurgeAsync(Threshold, ct);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task Purge_DeletesAbandonedOldPendingRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"PurgeOldPending_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var run = CreateRun(jobName) with { NotBefore = OldTime, CreatedAt = OldTime };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        await Store.PurgeAsync(Threshold, ct);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.Null(stored);
    }

    [Fact]
    public async Task Purge_PreservesFutureScheduledRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"FutureJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var run = CreateRun(jobName) with { NotBefore = DateTimeOffset.UtcNow.AddHours(1) };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        await Store.PurgeAsync(Threshold, ct);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
    }

    [Fact]
    public async Task Purge_PreservesRunningRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"RunningJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var run = CreateRun(jobName) with { CreatedAt = OldTime, NotBefore = OldTime };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);

        await Store.PurgeAsync(Threshold, ct);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
        Assert.Equal(JobStatus.Running, loaded.Status);
    }

    [Fact]
    public async Task Purge_PreservesActiveEntities()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ActiveJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var queueName = $"active-queue-{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = queueName }, ct);

        var nodeName = $"active-node-{Guid.CreateVersion7():N}";
        await Store.HeartbeatAsync(nodeName, [jobName], [queueName], [], ct);

        await Store.PurgeAsync(Threshold, ct);

        var loadedJob = await Store.GetJobAsync(jobName, ct);
        Assert.NotNull(loadedJob);

        var queues = await Store.GetQueuesAsync(ct);
        Assert.Contains(queues, q => q.Name == queueName);

        var nodes = await Store.GetNodesAsync(ct);
        Assert.Contains(nodes, n => n.Name == nodeName);
    }

    [Fact]
    public async Task Purge_RateLimitWindowState_CleansUp()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"RatePurge_{Guid.CreateVersion7():N}";
        var rateLimitName = $"rl_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.RateLimitName = rateLimitName;
        await Store.UpsertJobAsync(job, ct);
        await Store.UpsertRateLimitAsync(new()
        {
            Name = rateLimitName,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromHours(1)
        }, ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);
        var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);

        // Purge with future threshold to remove the rate limit
        await Store.PurgeAsync(DateTimeOffset.UtcNow.AddDays(1), ct);

        // Re-create the rate limit and a new run - should succeed if window state was cleaned up
        await Store.UpsertRateLimitAsync(new()
        {
            Name = rateLimitName,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromHours(1)
        }, ct);

        var job2 = CreateJob(jobName);
        job2.RateLimitName = rateLimitName;
        await Store.UpsertJobAsync(job2, ct);

        var run2 = CreateRun(jobName);
        await Store.CreateRunsAsync([run2], cancellationToken: ct);
        var claimed2 = await Store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimed2);
    }

    [Fact]
    public async Task Purge_PreservesJobsWithActiveRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ActiveRunJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);
        await Store.UpsertQueueAsync(new() { Name = "default" }, ct);

        var run = CreateRun(jobName) with { CreatedAt = OldTime, NotBefore = OldTime };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);

        // Purge with a future threshold — job heartbeat is stale, but it has a running run
        var futureThreshold = DateTimeOffset.UtcNow.AddMinutes(5);
        await Store.PurgeAsync(futureThreshold, ct);

        var loadedJob = await Store.GetJobAsync(jobName, ct);
        Assert.NotNull(loadedJob);
    }

    [Fact]
    public async Task Purge_DeletesTerminalBatches()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BatchJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var batchId = Guid.CreateVersion7().ToString("N");
        var runs = new[]
        {
            CreateRun(jobName) with { BatchId = batchId, CreatedAt = OldTime, NotBefore = OldTime },
            CreateRun(jobName) with { BatchId = batchId, CreatedAt = OldTime, NotBefore = OldTime }
        };
        await Store.CreateBatchAsync(
            new() { Id = batchId, CreatedAt = OldTime, Total = runs.Length, Status = JobStatus.Pending },
            runs, cancellationToken: ct);

        // Walk every child to terminal so the batch itself completes atomically inside the store.
        foreach (var run in runs)
        {
            var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"], ct);
            Assert.NotNull(claimed);
            var succeeded = claimed with { Status = JobStatus.Succeeded, CompletedAt = OldTime };
            await Store.TryTransitionRunAsync(Transition(succeeded, JobStatus.Running), ct);
        }

        var beforePurge = await Store.GetBatchAsync(batchId, ct);
        Assert.NotNull(beforePurge);
        Assert.True(beforePurge.IsTerminal);

        // The batch's CompletedAt is stamped by the store's TimeProvider during auto-completion,
        // so purge with a forward-shifted threshold that's guaranteed to include it.
        await Store.PurgeAsync(DateTimeOffset.UtcNow.AddMinutes(5), ct);

        var afterPurge = await Store.GetBatchAsync(batchId, ct);
        Assert.Null(afterPurge);
    }

    [Fact]
    public async Task Purge_PreservesNonTerminalBatches()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"OpenBatchJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        // Child's NotBefore is in the future so the "abandoned pending" rule can't harvest it;
        // this isolates the test to the batch-status purge rule.
        var future = DateTimeOffset.UtcNow.AddHours(1);
        var batchId = Guid.CreateVersion7().ToString("N");
        var runs = new[]
        {
            CreateRun(jobName) with { BatchId = batchId, CreatedAt = OldTime, NotBefore = future },
            CreateRun(jobName) with { BatchId = batchId, CreatedAt = OldTime, NotBefore = future }
        };
        await Store.CreateBatchAsync(
            new() { Id = batchId, CreatedAt = OldTime, Total = runs.Length, Status = JobStatus.Pending },
            runs, cancellationToken: ct);

        // Forward-shifted threshold would purge everything eligible; the batch is non-terminal, so
        // the batch-sweep must leave it alone and its children must not be orphaned.
        await Store.PurgeAsync(DateTimeOffset.UtcNow.AddMinutes(5), ct);

        var batch = await Store.GetBatchAsync(batchId, ct);
        Assert.NotNull(batch);
        Assert.False(batch.IsTerminal);

        foreach (var run in runs)
        {
            Assert.NotNull(await Store.GetRunAsync(run.Id, ct));
        }
    }

    [Fact]
    public async Task Purge_DeletesBatchEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BatchEventsJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var batchId = Guid.CreateVersion7().ToString("N");
        var run = CreateRun(jobName) with { BatchId = batchId, CreatedAt = OldTime, NotBefore = OldTime };
        await Store.CreateBatchAsync(
            new() { Id = batchId, CreatedAt = OldTime, Total = 1, Status = JobStatus.Pending },
            [run], cancellationToken: ct);

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);
        await Store.AppendEventsAsync([
            new() { RunId = run.Id, EventType = RunEventType.Log, Payload = "batch-log", CreatedAt = OldTime }
        ], ct);
        var succeeded = claimed with { Status = JobStatus.Succeeded, CompletedAt = OldTime };
        await Store.TryTransitionRunAsync(Transition(succeeded, JobStatus.Running), ct);

        var eventsBeforePurge = await Store.GetBatchEventsAsync(batchId, cancellationToken: ct);
        Assert.NotEmpty(eventsBeforePurge);

        await Store.PurgeAsync(DateTimeOffset.UtcNow.AddMinutes(5), ct);

        Assert.Null(await Store.GetBatchAsync(batchId, ct));
        var eventsAfterPurge = await Store.GetBatchEventsAsync(batchId, cancellationToken: ct);
        Assert.Empty(eventsAfterPurge);
    }

    [Fact]
    public async Task Purge_FreesDedupSlotForNewRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DedupPurge_{Guid.CreateVersion7():N}";
        var dedupId = $"once-{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var first = CreateRun(jobName) with
        {
            DeduplicationId = dedupId,
            CreatedAt = OldTime,
            NotBefore = OldTime
        };
        Assert.True(await Store.TryCreateRunAsync(first, cancellationToken: ct));

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);
        var succeeded = claimed with { Status = JobStatus.Succeeded, CompletedAt = OldTime };
        await Store.TryTransitionRunAsync(Transition(succeeded, JobStatus.Running), ct);

        await Store.PurgeAsync(Threshold, ct);
        Assert.Null(await Store.GetRunAsync(first.Id, ct));

        // If the dedup entry leaked past the purge, the next create with the same id would be rejected.
        var second = CreateRun(jobName) with { DeduplicationId = dedupId };
        Assert.True(await Store.TryCreateRunAsync(second, cancellationToken: ct));
        Assert.NotNull(await Store.GetRunAsync(second.Id, ct));
    }

    [Fact]
    public async Task Purge_FreesConcurrencyCounterForNewClaims()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ConcurrencyPurge_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.MaxConcurrency = 1;
        await Store.UpsertJobAsync(job, ct);

        // Orphan a pending run: sits at the concurrency boundary without ever being claimed.
        var orphan = CreateRun(jobName) with { CreatedAt = OldTime, NotBefore = OldTime };
        await Store.CreateRunsAsync([orphan], cancellationToken: ct);

        await Store.PurgeAsync(Threshold, ct);
        Assert.Null(await Store.GetRunAsync(orphan.Id, ct));

        // If counters leaked, the MaxConcurrency slot would stay consumed and this claim would fail.
        var fresh = CreateRun(jobName);
        await Store.CreateRunsAsync([fresh], cancellationToken: ct);
        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);
        Assert.Equal(fresh.Id, claimed.Id);
    }

    [Fact]
    public async Task Purge_ContinuousCycle_AccumulatesNothing()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"LoopPurge_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.MaxConcurrency = 1;
        await Store.UpsertJobAsync(job, ct);

        // Run many short cycles, each backdated so every purge sweep can clean up every
        // artifact the prior cycle produced. If any index, counter, or table leaks per cycle,
        // by the final cycle the store will refuse the create/claim or the counts won't settle.
        for (var cycle = 0; cycle < 10; cycle++)
        {
            var run = CreateRun(jobName) with
            {
                DeduplicationId = $"cycle-{cycle}",
                CreatedAt = OldTime,
                NotBefore = OldTime
            };
            Assert.True(await Store.TryCreateRunAsync(run, cancellationToken: ct));

            var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"], ct);
            Assert.NotNull(claimed);

            await Store.AppendEventsAsync([
                new() { RunId = run.Id, EventType = RunEventType.Log, Payload = $"c{cycle}", CreatedAt = OldTime }
            ], ct);

            var succeeded = claimed with { Status = JobStatus.Succeeded, CompletedAt = OldTime };
            await Store.TryTransitionRunAsync(Transition(succeeded, JobStatus.Running), ct);

            await Store.PurgeAsync(Threshold, ct);

            // After each cycle the run and its events must be gone; any leak shows up next cycle.
            Assert.Null(await Store.GetRunAsync(run.Id, ct));
            Assert.Empty(await Store.GetEventsAsync(run.Id, cancellationToken: ct));
        }

        // Final proof: a fresh dedup+claim+terminal round still works. If any counter leaked,
        // MaxConcurrency=1 would block the claim; if the dedup index leaked, the create would fail.
        var tail = CreateRun(jobName) with { DeduplicationId = "cycle-tail" };
        Assert.True(await Store.TryCreateRunAsync(tail, cancellationToken: ct));
        var finalClaim = await Store.ClaimRunAsync("node1", [jobName], ["default"], ct);
        Assert.NotNull(finalClaim);
        Assert.Equal(tail.Id, finalClaim.Id);
    }
}