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
        var jobName = $"NoExist_{Guid.CreateVersion7():N}";
        var run = CreateRun(jobName);
        await Store.UpsertQueueAsync(new() { Name = "default" });

        var created = await Store.TryCreateRunAsync(run, 1);

        Assert.True(created);
    }

    [Fact]
    public async Task TryCreateRunAsync_PersistsRunPriority()
    {
        var jobName = $"PriorityDefault_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(new() { Name = jobName, Priority = 42 });

        var run = CreateRun(jobName);
        run.Priority = 9;

        var created = await Store.TryCreateRunAsync(run);

        Assert.True(created);
        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(9, stored.Priority);
    }

    [Fact]
    public async Task TryCreateRunAsync_UnknownJob_PersistsRunPriority()
    {
        var jobName = $"PriorityRequested_{Guid.CreateVersion7():N}";
        var run = CreateRun(jobName);
        run.Priority = 7;

        var created = await Store.TryCreateRunAsync(run);

        Assert.True(created);
        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(7, stored.Priority);
    }

    [Fact]
    public async Task CreateRunsAsync_PersistsRunPriorityPerRun()
    {
        var jobName = $"BatchPriorityDefault_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(new() { Name = jobName, Priority = 13 });

        var runA = CreateRun(jobName);
        var runB = CreateRun(jobName);
        runA.Priority = 3;
        runB.Priority = 11;

        await Store.CreateRunsAsync([runA, runB]);

        var storedA = await Store.GetRunAsync(runA.Id);
        var storedB = await Store.GetRunAsync(runB.Id);
        Assert.NotNull(storedA);
        Assert.NotNull(storedB);
        Assert.Equal(3, storedA.Priority);
        Assert.Equal(11, storedB.Priority);
    }

    [Fact]
    public async Task UnknownJobRun_StaysPendingUntilJobRegistered()
    {
        var jobName = $"LateReg_{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = "default" });

        var run = CreateRun(jobName);
        var created = await Store.TryCreateRunAsync(run, 1);
        Assert.True(created);

        var beforeRegistration = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.Null(beforeRegistration);

        await Store.UpsertJobAsync(CreateJob(jobName));

        var afterRegistration = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(afterRegistration);
        Assert.Equal(run.Id, afterRegistration.Id);
    }

    [Fact]
    public async Task TryCreateRunAsync_RejectsRunForDisabledJob()
    {
        var jobName = $"Disabled_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));
        await Store.SetJobEnabledAsync(jobName, false);
        await Store.UpsertQueueAsync(new() { Name = "default" });

        var run = CreateRun(jobName);
        var created = await Store.TryCreateRunAsync(run, 1);

        Assert.False(created);
    }

    [Fact]
    public async Task GetJobsAsync_LiteralPercentInFilter_DoesNotMatchAll()
    {
        var suffix = Guid.CreateVersion7().ToString("N");
        var job1Name = $"upload_photos_{suffix}";
        var job2Name = $"upload%special_{suffix}";

        await Store.UpsertJobAsync(CreateJob(job1Name));
        await Store.UpsertJobAsync(CreateJob(job2Name));

        var results = await Store.GetJobsAsync(new() { Name = $"%special_{suffix}" });

        Assert.Single(results);
        Assert.Equal(job2Name, results[0].Name);
    }

    [Fact]
    public async Task GetJobsAsync_LiteralUnderscoreInFilter()
    {
        var suffix = Guid.CreateVersion7().ToString("N");
        var job1Name = $"job_a_{suffix}";
        var job2Name = $"jobXa_{suffix}";

        await Store.UpsertJobAsync(CreateJob(job1Name));
        await Store.UpsertJobAsync(CreateJob(job2Name));

        // Search for literal "_a_" — should not treat _ as single-char wildcard
        var results = await Store.GetJobsAsync(new() { Name = $"_a_{suffix}" });

        Assert.Single(results);
        Assert.Equal(job1Name, results[0].Name);
    }

    [Fact]
    public async Task BatchCoordinator_NotCountedInPendingQueueStats()
    {
        var jobName = $"BatchStats_{Guid.CreateVersion7():N}";
        var queueName = "default";
        await Store.UpsertJobAsync(CreateJob(jobName));
        await Store.UpsertQueueAsync(new() { Name = queueName });

        var coordinator = CreateRun(jobName);
        coordinator.BatchTotal = 3;
        coordinator.BatchCompleted = 0;
        coordinator.BatchFailed = 0;

        var child1 = CreateRun(jobName);
        child1.ParentRunId = coordinator.Id;
        child1.RootRunId = coordinator.Id;

        var child2 = CreateRun(jobName);
        child2.ParentRunId = coordinator.Id;
        child2.RootRunId = coordinator.Id;

        var child3 = CreateRun(jobName);
        child3.ParentRunId = coordinator.Id;
        child3.RootRunId = coordinator.Id;

        await Store.CreateRunsAsync([coordinator, child1, child2, child3]);

        var queueStats = await Store.GetQueueStatsAsync();
        Assert.True(queueStats.ContainsKey(queueName));
        // Only the 3 children should be pending, not the coordinator
        Assert.Equal(3, queueStats[queueName].PendingCount);
    }

    [Fact]
    public async Task IncrementBatchCounter_ZeroBatchTotal_ProgressIsOne()
    {
        var jobName = $"ZeroBatch_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        run.BatchTotal = 0;
        run.BatchCompleted = 0;
        run.BatchFailed = 0;
        run.NodeName = "node1";
        await Store.CreateRunsAsync([run]);

        // Transition to Running first
        run.Status = JobStatus.Running;
        run.Attempt = 1;
        run.StartedAt = DateTimeOffset.UtcNow;
        run.LastHeartbeatAt = DateTimeOffset.UtcNow;
        await Store.TryTransitionRunAsync(Transition(run, JobStatus.Pending));

        var counters = await Store.TryIncrementBatchCounterAsync(run.Id, false);
        var stored = await Store.GetRunAsync(run.Id);

        Assert.NotNull(stored);
        Assert.True(double.IsFinite(stored.Progress), "Progress should be finite, not Infinity");
    }

    [Fact]
    public async Task UpsertJobAsync_ChangingQueue_RunStillClaimable()
    {
        var jobName = $"QueueMove_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.Queue = "low";
        await Store.UpsertJobAsync(job);
        await Store.UpsertQueueAsync(new() { Name = "low", Priority = 1 });
        await Store.UpsertQueueAsync(new() { Name = "high", Priority = 10 });

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);

        // Move the job to the "high" queue
        job.Queue = "high";
        await Store.UpsertJobAsync(job);

        // The pending run should still be claimable when the node registers for both queues
        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["low", "high"]);
        Assert.NotNull(claimed);
    }

    [Fact]
    public async Task Claim_Concurrent_QueueMaxConcurrency_DifferentJobs_Respected()
    {
        var queueName = $"conc-q-{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = queueName, MaxConcurrency = 2 });

        // Create 10 different jobs all in the same concurrency-limited queue
        var jobNames = new List<string>();
        for (var i = 0; i < 10; i++)
        {
            var jobName = $"QConc_{i}_{Guid.CreateVersion7():N}";
            var job = CreateJob(jobName);
            job.Queue = queueName;
            await Store.UpsertJobAsync(job);
            jobNames.Add(jobName);

            var run = CreateRun(jobName);
            await Store.CreateRunsAsync([run]);
        }

        // Claim concurrently from 10 threads — only 2 should succeed due to queue max_concurrency=2
        var results = new ConcurrentBag<RunRecord?>();
        var tasks = Enumerable.Range(0, 10).Select(t => Task.Run(async () =>
        {
            var claimed = await Store.ClaimRunAsync($"node-{t}", jobNames, [queueName]);
            results.Add(claimed);
        }));
        await Task.WhenAll(tasks);

        var claimedCount = results.Count(r => r is { });
        Assert.Equal(2, claimedCount);
    }

    [Fact]
    public async Task BatchCoordinator_StatusTransition_DoesNotCorruptPendingCount()
    {
        var jobName = $"BatchPending_{Guid.CreateVersion7():N}";
        var queueName = "default";
        await Store.UpsertJobAsync(CreateJob(jobName));
        await Store.UpsertQueueAsync(new() { Name = queueName });

        // Create a coordinator and some children
        var coordinator = CreateRun(jobName);
        coordinator.BatchTotal = 2;
        coordinator.BatchCompleted = 0;
        coordinator.BatchFailed = 0;

        var child1 = CreateRun(jobName);
        child1.ParentRunId = coordinator.Id;
        child1.RootRunId = coordinator.Id;
        var child2 = CreateRun(jobName);
        child2.ParentRunId = coordinator.Id;
        child2.RootRunId = coordinator.Id;

        await Store.CreateRunsAsync([coordinator, child1, child2]);

        // Verify initial pending count: 2 children, not the coordinator
        var stats1 = await Store.GetQueueStatsAsync();
        Assert.True(stats1.ContainsKey(queueName));
        Assert.Equal(2, stats1[queueName].PendingCount);

        // Transition coordinator to Running (simulating batch start)
        coordinator.Status = JobStatus.Running;
        coordinator.Attempt = 1;
        coordinator.NodeName = "node1";
        coordinator.StartedAt = DateTimeOffset.UtcNow;
        coordinator.LastHeartbeatAt = DateTimeOffset.UtcNow;
        await Store.TryTransitionRunAsync(Transition(coordinator, JobStatus.Pending));

        // Pending count should still be 2 (children only)
        var stats2 = await Store.GetQueueStatsAsync();
        Assert.True(stats2.ContainsKey(queueName));
        Assert.Equal(2, stats2[queueName].PendingCount);

        // Transition coordinator to Completed
        coordinator.Status = JobStatus.Succeeded;
        coordinator.CompletedAt = DateTimeOffset.UtcNow;
        await Store.TryTransitionRunAsync(Transition(coordinator, JobStatus.Running));

        // Pending count should still be 2 — coordinator transition must not affect it
        var stats3 = await Store.GetQueueStatsAsync();
        Assert.True(stats3.ContainsKey(queueName));
        Assert.Equal(2, stats3[queueName].PendingCount);
    }

    [Fact]
    public async Task GetEvents_SinceId_WorksWithManyEvents()
    {
        var jobName = $"ManyEvents_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);

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
            await Store.AppendEventsAsync(events);
        }

        var all = await Store.GetEventsAsync(run.Id);
        Assert.Equal(200, all.Count);

        // Use a cursor from early in the list
        var earlyCursor = all[9].Id;
        var afterCursor = await Store.GetEventsAsync(run.Id, earlyCursor);

        Assert.Equal(190, afterCursor.Count);
        Assert.True(afterCursor.All(e => e.Id > earlyCursor));
    }

    [Fact]
    public async Task CancelExpiredRuns_MixedStatuses_CancelsOnlyEligible()
    {
        var jobName = $"MixedExpire_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));
        await Store.UpsertQueueAsync(new() { Name = "default" });

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var past = now.AddMinutes(-10);

        // Create 1 expired pending run
        var pending1 = CreateRun(jobName);
        pending1.NotAfter = past;
        await Store.CreateRunsAsync([pending1]);

        // Create a Running run with expired NotAfter (should NOT be cancelled)
        var running = CreateRun(jobName, JobStatus.Running);
        running.NotAfter = past;
        running.NodeName = "node1";
        running.Attempt = 1;
        running.StartedAt = now;
        running.LastHeartbeatAt = now;
        await Store.CreateRunsAsync([running]);

        // Create a Retrying run with expired NotAfter (SHOULD be cancelled)
        var retrying = CreateRun(jobName, JobStatus.Retrying);
        retrying.NotAfter = past;
        retrying.NodeName = "node1";
        retrying.Attempt = 1;
        retrying.StartedAt = now;
        retrying.LastHeartbeatAt = now;
        retrying.Error = "temp failure";
        await Store.CreateRunsAsync([retrying]);

        // 1 Pending + 1 Retrying = 2 should be cancelled
        var cancelled = await Store.CancelExpiredRunsAsync();
        Assert.Equal(2, cancelled);

        // The Running one should still be Running
        var loaded = await Store.GetRunAsync(running.Id);
        Assert.Equal(JobStatus.Running, loaded!.Status);
    }
}