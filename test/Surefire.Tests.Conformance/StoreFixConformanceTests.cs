using System.Collections.Concurrent;

namespace Surefire.Tests.Conformance;

/// <summary>
///     Mixed coverage for store edge cases (LIKE wildcard escaping, sliding-window decay,
///     batch counters, runs for unregistered jobs, BatchTotal=0).
/// </summary>
public abstract class StoreFixConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task TryCreateRunAsync_AllowsRunForNonExistentJob()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"NoExist_{Guid.CreateVersion7():N}";
        var run = CreateRun(jobName);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var created = await Store.TryCreateRunAsync(run, 1, cancellationToken: ct);

        Assert.True(created);
    }

    [Fact]
    public async Task TryCreateRunAsync_PersistsRunPriority()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"PriorityDefault_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([new() { Name = jobName, Priority = 42 }], ct);

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
        await Store.UpsertJobsAsync([new() { Name = jobName, Priority = 13 }], ct);

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
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var run = CreateRun(jobName);
        var created = await Store.TryCreateRunAsync(run, 1, cancellationToken: ct);
        Assert.True(created);

        var beforeRegistration = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.Null(beforeRegistration);

        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var afterRegistration = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(afterRegistration);
        Assert.Equal(run.Id, afterRegistration.Id);
    }

    [Fact]
    public async Task TryCreateRunAsync_RejectsRunForDisabledJob()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"Disabled_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.SetJobEnabledAsync(jobName, false, ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

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

        await Store.UpsertJobsAsync([CreateJob(job1Name)], ct);
        await Store.UpsertJobsAsync([CreateJob(job2Name)], ct);

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

        await Store.UpsertJobsAsync([CreateJob(job1Name)], ct);
        await Store.UpsertJobsAsync([CreateJob(job2Name)], ct);

        // Literal "_a_" must not treat _ as a single-char wildcard.
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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = queueName }], ct);

        var batchId = Guid.CreateVersion7().ToString("N");
        var child1 = CreateRun(jobName) with { BatchId = batchId };

        var child2 = CreateRun(jobName) with { BatchId = batchId };

        var child3 = CreateRun(jobName) with { BatchId = batchId };

        await Store.CreateRunsAsync([child1, child2, child3], cancellationToken: ct);

        var queueStats = await Store.GetQueueStatsAsync(ct);
        Assert.True(queueStats.ContainsKey(queueName));
        Assert.Equal(3, queueStats[queueName].PendingCount);
    }

    [Fact]
    public async Task UpsertJobAsync_ChangingQueue_RunStillClaimable()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"QueueMove_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.Queue = "low";
        await Store.UpsertJobsAsync([job], ct);
        await Store.UpsertQueuesAsync([new() { Name = "low", Priority = 1 }], ct);
        await Store.UpsertQueuesAsync([new() { Name = "high", Priority = 10 }], ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        job.Queue = "high";
        await Store.UpsertJobsAsync([job], ct);

        // The pending run should still be claimable when the node registers for both queues.
        var claimed = (await Store.ClaimRunsAsync("node1", [jobName], ["low", "high"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);
    }

    [Fact]
    public async Task Claim_Concurrent_QueueMaxConcurrency_DifferentJobs_Respected()
    {
        var ct = TestContext.Current.CancellationToken;
        var queueName = $"conc-q-{Guid.CreateVersion7():N}";
        await Store.UpsertQueuesAsync([new() { Name = queueName, MaxConcurrency = 2 }], ct);

        var jobNames = new List<string>();
        for (var i = 0; i < 10; i++)
        {
            var jobName = $"QConc_{i}_{Guid.CreateVersion7():N}";
            var job = CreateJob(jobName);
            job.Queue = queueName;
            await Store.UpsertJobsAsync([job], ct);
            jobNames.Add(jobName);

            var run = CreateRun(jobName);
            await Store.CreateRunsAsync([run], cancellationToken: ct);
        }

        // Claim concurrently from 10 threads; only 2 should succeed due to queue max_concurrency=2.
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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = queueName }], ct);

        var batchId = Guid.CreateVersion7().ToString("N");
        var child1 = CreateRun(jobName) with { BatchId = batchId };
        var child2 = CreateRun(jobName) with { BatchId = batchId };

        var unrelated = CreateRun(jobName);

        await Store.CreateRunsAsync([child1, child2, unrelated], cancellationToken: ct);

        var stats1 = await Store.GetQueueStatsAsync(ct);
        Assert.True(stats1.ContainsKey(queueName));
        Assert.Equal(3, stats1[queueName].PendingCount);

        var claimed = (await Store.ClaimRunsAsync("node1", [jobName], [queueName], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        await Store.TryTransitionRunAsync(RunStatusTransition.RunningToSucceeded(
            claimed.Id, claimed.Attempt, now, claimed.NotBefore, "node1", 1, null, null,
            claimed.StartedAt, now), ct);

        var stats2 = await Store.GetQueueStatsAsync(ct);
        Assert.True(stats2.ContainsKey(queueName));
        Assert.Equal(2, stats2[queueName].PendingCount);
    }

    [Fact]
    public async Task GetEvents_SinceId_WorksWithManyEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ManyEvents_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

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
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);
        await Store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var past = now.AddMinutes(-10);

        var pending1 = CreateRun(jobName) with { NotAfter = past };
        await Store.CreateRunsAsync([pending1], cancellationToken: ct);

        // Running run with expired NotAfter; should NOT be cancelled.
        var running = CreateRun(jobName, JobStatus.Running) with
        {
            NotAfter = past,
            NodeName = "node1",
            Attempt = 1,
            StartedAt = now,
            LastHeartbeatAt = now
        };
        await Store.CreateRunsAsync([running], cancellationToken: ct);

        // Another expired Pending run; should be cancelled.
        var pending2 = CreateRun(jobName) with
        {
            NotAfter = past
        };
        await Store.CreateRunsAsync([pending2], cancellationToken: ct);

        var cancelled = (await Store.CancelExpiredRunsWithIdsAsync(ct)).Count;
        Assert.Equal(2, cancelled);

        var loaded = await Store.GetRunAsync(running.Id, ct);
        Assert.Equal(JobStatus.Running, loaded!.Status);
    }
}
