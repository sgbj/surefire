using System.Collections.Concurrent;

namespace Surefire.Tests.Conformance;

/// <summary>
///     Tests for store fixes: sliding window decay, batch coordinator pending count,
///     TryCreateRunAsync for non-existent jobs, LIKE wildcard escaping, and BatchTotal=0.
/// </summary>
public abstract class StoreFixConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task TryCreateRunAsync_AllowsRunForNonExistentJob()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"NoExist_{Guid.CreateVersion7():N}";
        var run = CreateRun(jobName);
        await Store.UpsertQueueAsync(new() { Name = "default" }, ct);

        var created = await Store.TryCreateRunAsync(run, 1, cancellationToken: ct);

        Assert.True(created);
    }

    [Fact]
    public async Task TryCreateRunAsync_PersistsRunPriority()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"PriorityDefault_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(new() { Name = jobName, Priority = 42 }, ct);

        var run = CreateRun(jobName) with { Priority = 9 };

        var created = await Store.TryCreateRunAsync(run, cancellationToken: ct);

        Assert.True(created);
        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(9, stored.Priority);
    }

    [Fact]
    public async Task TryCreateRunAsync_UnknownJob_PersistsRunPriority()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"PriorityRequested_{Guid.CreateVersion7():N}";
        var run = CreateRun(jobName) with { Priority = 7 };

        var created = await Store.TryCreateRunAsync(run, cancellationToken: ct);

        Assert.True(created);
        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(7, stored.Priority);
    }

    [Fact]
    public async Task CreateRunsAsync_PersistsRunPriorityPerRun()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BatchPriorityDefault_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(new() { Name = jobName, Priority = 13 }, ct);

        var runA = CreateRun(jobName) with { Priority = 3 };
        var runB = CreateRun(jobName) with { Priority = 11 };

        await Store.CreateRunsAsync([runA, runB], cancellationToken: ct);

        var storedA = await Store.GetRunAsync(runA.Id, ct);
        var storedB = await Store.GetRunAsync(runB.Id, ct);
        Assert.NotNull(storedA);
        Assert.NotNull(storedB);
        Assert.Equal(3, storedA.Priority);
        Assert.Equal(11, storedB.Priority);
    }

    [Fact]
    public async Task UnknownJobRun_StaysPendingUntilJobRegistered()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"LateReg_{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = "default" }, ct);

        var run = CreateRun(jobName);
        var created = await Store.TryCreateRunAsync(run, 1, cancellationToken: ct);
        Assert.True(created);

        var beforeRegistration = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.Null(beforeRegistration);

        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var afterRegistration = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(afterRegistration);
        Assert.Equal(run.Id, afterRegistration.Id);
    }

    [Fact]
    public async Task TryCreateRunAsync_RejectsRunForDisabledJob()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"Disabled_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);
        await Store.SetJobEnabledAsync(jobName, false, ct);
        await Store.UpsertQueueAsync(new() { Name = "default" }, ct);

        var run = CreateRun(jobName);
        var created = await Store.TryCreateRunAsync(run, 1, cancellationToken: ct);

        Assert.False(created);
    }

    [Fact]
    public async Task GetJobsAsync_LiteralPercentInFilter_DoesNotMatchAll()
    {
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.CreateVersion7().ToString("N");
        var job1Name = $"upload_photos_{suffix}";
        var job2Name = $"upload%special_{suffix}";

        await Store.UpsertJobAsync(CreateJob(job1Name), ct);
        await Store.UpsertJobAsync(CreateJob(job2Name), ct);

        var results = await Store.GetJobsAsync(new() { Name = $"%special_{suffix}" }, ct);

        Assert.Single(results);
        Assert.Equal(job2Name, results[0].Name);
    }

    [Fact]
    public async Task GetJobsAsync_LiteralUnderscoreInFilter()
    {
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.CreateVersion7().ToString("N");
        var job1Name = $"job_a_{suffix}";
        var job2Name = $"jobXa_{suffix}";

        await Store.UpsertJobAsync(CreateJob(job1Name), ct);
        await Store.UpsertJobAsync(CreateJob(job2Name), ct);

        // Search for literal "_a_" â€” should not treat _ as single-char wildcard
        var results = await Store.GetJobsAsync(new() { Name = $"_a_{suffix}" }, ct);

        Assert.Single(results);
        Assert.Equal(job1Name, results[0].Name);
    }

    [Fact]
    public async Task BatchChildRuns_CountedInPendingQueueStats()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BatchStats_{Guid.CreateVersion7():N}";
        var queueName = "default";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);
        await Store.UpsertQueueAsync(new() { Name = queueName }, ct);

        var batchId = Guid.CreateVersion7().ToString("N");
        var child1 = CreateRun(jobName) with { BatchId = batchId };

        var child2 = CreateRun(jobName) with { BatchId = batchId };

        var child3 = CreateRun(jobName) with { BatchId = batchId };

        await Store.CreateRunsAsync([child1, child2, child3], cancellationToken: ct);

        var queueStats = await Store.GetQueueStatsAsync(ct);
        Assert.True(queueStats.ContainsKey(queueName));
        // All 3 batch children should be pending
        Assert.Equal(3, queueStats[queueName].PendingCount);
    }

    [Fact]
    public async Task UpsertJobAsync_ChangingQueue_RunStillClaimable()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"QueueMove_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.Queue = "low";
        await Store.UpsertJobAsync(job, ct);
        await Store.UpsertQueueAsync(new() { Name = "low", Priority = 1 }, ct);
        await Store.UpsertQueueAsync(new() { Name = "high", Priority = 10 }, ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        // Move the job to the "high" queue
        job.Queue = "high";
        await Store.UpsertJobAsync(job, ct);

        // The pending run should still be claimable when the node registers for both queues
        var claimed = (await Store.ClaimRunsAsync("node1", [jobName], ["low", "high"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);
    }

    [Fact]
    public async Task Claim_Concurrent_QueueMaxConcurrency_DifferentJobs_Respected()
    {
        var ct = TestContext.Current.CancellationToken;
        var queueName = $"conc-q-{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = queueName, MaxConcurrency = 2 }, ct);

        // Create 10 different jobs all in the same concurrency-limited queue
        var jobNames = new List<string>();
        for (var i = 0; i < 10; i++)
        {
            var jobName = $"QConc_{i}_{Guid.CreateVersion7():N}";
            var job = CreateJob(jobName);
            job.Queue = queueName;
            await Store.UpsertJobAsync(job, ct);
            jobNames.Add(jobName);

            var run = CreateRun(jobName);
            await Store.CreateRunsAsync([run], cancellationToken: ct);
        }

        // Claim concurrently from 10 threads â€” only 2 should succeed due to queue max_concurrency=2
        var results = new ConcurrentBag<JobRun?>();
        var tasks = Enumerable.Range(0, 10).Select(t => Task.Run(async () =>
        {
            var claimed = (await Store.ClaimRunsAsync($"node-{t}", jobNames, [queueName], 1)).FirstOrDefault();
            results.Add(claimed);
        }));
        await Task.WhenAll(tasks);

        var claimedCount = results.Count(r => r is { });
        Assert.Equal(2, claimedCount);
    }

    [Fact]
    public async Task BatchChildRuns_PendingCount_NotAffectedByOtherRunTransitions()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"BatchPending_{Guid.CreateVersion7():N}";
        var queueName = "default";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);
        await Store.UpsertQueueAsync(new() { Name = queueName }, ct);

        var batchId = Guid.CreateVersion7().ToString("N");
        var child1 = CreateRun(jobName) with { BatchId = batchId };
        var child2 = CreateRun(jobName) with { BatchId = batchId };

        // An unrelated run in the same queue
        var unrelated = CreateRun(jobName);

        await Store.CreateRunsAsync([child1, child2, unrelated], cancellationToken: ct);

        // 3 pending: 2 children + 1 unrelated
        var stats1 = await Store.GetQueueStatsAsync(ct);
        Assert.True(stats1.ContainsKey(queueName));
        Assert.Equal(3, stats1[queueName].PendingCount);

        // Claim and complete the unrelated run
        var claimed = (await Store.ClaimRunsAsync("node1", [jobName], [queueName], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        await Store.TryTransitionRunAsync(RunStatusTransition.RunningToSucceeded(
            claimed.Id, claimed.Attempt, now, claimed.NotBefore, "node1", 1, null, null,
            claimed.StartedAt, now), ct);

        // 2 pending: only the batch children remain
        var stats2 = await Store.GetQueueStatsAsync(ct);
        Assert.True(stats2.ContainsKey(queueName));
        Assert.Equal(2, stats2[queueName].PendingCount);
    }

    [Fact]
    public async Task GetEvents_SinceId_WorksWithManyEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ManyEvents_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        // Append 200 events in batches
        for (var batch = 0; batch < 4; batch++)
        {
            var events = Enumerable.Range(batch * 50, 50)
                .Select(i => new RunEvent
                {
                    RunId = run.Id,
                    EventType = RunEventType.Log,
                    Payload = $"event-{i}",
                    CreatedAt = DateTimeOffset.UtcNow
                })
                .ToList();
            await Store.AppendEventsAsync(events, ct);
        }

        var all = await Store.GetEventsAsync(run.Id, cancellationToken: ct);
        Assert.Equal(200, all.Count);

        // Use a cursor from early in the list
        var earlyCursor = all[9].Id;
        var afterCursor = await Store.GetEventsAsync(run.Id, earlyCursor, cancellationToken: ct);

        Assert.Equal(190, afterCursor.Count);
        Assert.True(afterCursor.All(e => e.Id > earlyCursor));
    }

    [Fact]
    public async Task CancelExpiredRuns_MixedStatuses_CancelsOnlyEligible()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"MixedExpire_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);
        await Store.UpsertQueueAsync(new() { Name = "default" }, ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var past = now.AddMinutes(-10);

        // Create 1 expired pending run
        var pending1 = CreateRun(jobName) with { NotAfter = past };
        await Store.CreateRunsAsync([pending1], cancellationToken: ct);

        // Create a Running run with expired NotAfter (should NOT be cancelled)
        var running = CreateRun(jobName, JobStatus.Running) with
        {
            NotAfter = past,
            NodeName = "node1",
            Attempt = 1,
            StartedAt = now,
            LastHeartbeatAt = now
        };
        await Store.CreateRunsAsync([running], cancellationToken: ct);

        // Create another expired Pending run (SHOULD be cancelled)
        var pending2 = CreateRun(jobName) with
        {
            NotAfter = past
        };
        await Store.CreateRunsAsync([pending2], cancellationToken: ct);

        // 2 Pending runs should be cancelled
        var cancelled = (await Store.CancelExpiredRunsWithIdsAsync(ct)).Count;
        Assert.Equal(2, cancelled);

        // The Running one should still be Running
        var loaded = await Store.GetRunAsync(running.Id, ct);
        Assert.Equal(JobStatus.Running, loaded!.Status);
    }
}