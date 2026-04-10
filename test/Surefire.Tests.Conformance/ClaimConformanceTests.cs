using System.Collections.Concurrent;

namespace Surefire.Tests.Conformance;

public abstract class ClaimConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task Claim_ReturnsPendingRun()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);

        Assert.NotNull(claimed);
        Assert.Equal(run.Id, claimed.Id);
    }

    [Fact]
    public async Task Claim_SetsRunning_NodeName_StartedAt_Heartbeat()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);

        Assert.NotNull(claimed);
        Assert.Equal(JobStatus.Running, claimed.Status);
        Assert.Equal("node-1", claimed.NodeName);
        Assert.NotNull(claimed.StartedAt);
        Assert.NotNull(claimed.LastHeartbeatAt);
    }

    [Fact]
    public async Task Claim_IncrementsAttempt()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        Assert.Equal(0, run.Attempt);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);
        Assert.Equal(1, claimed.Attempt);

        claimed = claimed with { Status = JobStatus.Retrying };
        var ok = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));
        Assert.True(ok);

        claimed = claimed with { Status = JobStatus.Pending, NotBefore = DateTimeOffset.UtcNow.AddSeconds(-1) };
        ok = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Retrying));
        Assert.True(ok);

        var reclaimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(reclaimed);
        Assert.Equal(2, reclaimed.Attempt);
    }

    [Fact]
    public async Task Claim_ReturnsNull_WhenNoPendingRuns()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);

        Assert.Null(claimed);
    }

    [Fact]
    public async Task Claim_RespectsJobFilter()
    {
        var jobA = CreateJob("JobA");
        var jobB = CreateJob("JobB");
        await Store.UpsertJobAsync(jobA);
        await Store.UpsertJobAsync(jobB);

        var run = CreateRun("JobA");
        await Store.CreateRunsAsync([run]);

        var missed = await Store.ClaimRunAsync("node-1", ["JobB"], ["default"]);
        Assert.Null(missed);

        var claimed = await Store.ClaimRunAsync("node-1", ["JobA"], ["default"]);
        Assert.NotNull(claimed);
        Assert.Equal(run.Id, claimed.Id);
    }

    [Fact]
    public async Task Claim_RespectsQueueFilter()
    {
        var job = CreateJob();
        job.Queue = "fast";
        await Store.UpsertJobAsync(job);
        await Store.UpsertQueueAsync(new() { Name = "fast" });

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var missed = await Store.ClaimRunAsync("node-1", [job.Name], ["slow"]);
        Assert.Null(missed);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["fast"]);
        Assert.NotNull(claimed);
    }

    [Fact]
    public async Task Claim_SkipsPausedQueues()
    {
        var queueName = "pausable-" + Guid.CreateVersion7().ToString("N");
        var job = CreateJob();
        job.Queue = queueName;
        await Store.UpsertJobAsync(job);
        await Store.UpsertQueueAsync(new() { Name = queueName });

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        await Store.SetQueuePausedAsync(queueName, true);

        var missed = await Store.ClaimRunAsync("node-1", [job.Name], [queueName]);
        Assert.Null(missed);

        await Store.SetQueuePausedAsync(queueName, false);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], [queueName]);
        Assert.NotNull(claimed);
    }

    [Fact]
    public async Task Claim_RespectsNotBefore()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name) with { NotBefore = DateTimeOffset.UtcNow.AddHours(1) };
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);

        Assert.Null(claimed);
    }

    [Fact]
    public async Task Claim_RespectsJobMaxConcurrency()
    {
        var job = CreateJob();
        job.MaxConcurrency = 1;
        await Store.UpsertJobAsync(job);

        var run1 = CreateRun(job.Name);
        var run2 = CreateRun(job.Name);
        await Store.CreateRunsAsync([run1]);
        await Store.CreateRunsAsync([run2]);

        var claimed1 = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed1);

        var claimed2 = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.Null(claimed2);
    }

    [Fact]
    public async Task Claim_RespectsQueueMaxConcurrency()
    {
        var queueName = "limited-" + Guid.CreateVersion7().ToString("N");
        await Store.UpsertQueueAsync(new() { Name = queueName, MaxConcurrency = 1 });

        var jobA = CreateJob("QueueConcA_" + Guid.CreateVersion7().ToString("N"));
        jobA.Queue = queueName;
        var jobB = CreateJob("QueueConcB_" + Guid.CreateVersion7().ToString("N"));
        jobB.Queue = queueName;
        await Store.UpsertJobAsync(jobA);
        await Store.UpsertJobAsync(jobB);

        var run1 = CreateRun(jobA.Name);
        var run2 = CreateRun(jobB.Name);
        await Store.CreateRunsAsync([run1]);
        await Store.CreateRunsAsync([run2]);

        var claimed1 = await Store.ClaimRunAsync("node-1", [jobA.Name, jobB.Name], [queueName]);
        Assert.NotNull(claimed1);

        var claimed2 = await Store.ClaimRunAsync("node-1", [jobA.Name, jobB.Name], [queueName]);
        Assert.Null(claimed2);
    }

    [Fact]
    public async Task Claim_RespectsPriority_HigherFirst()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var lowPriority = CreateRun(job.Name) with { Priority = 0 };
        var highPriority = CreateRun(job.Name) with { Priority = 10 };

        await Store.CreateRunsAsync([lowPriority]);
        await Store.CreateRunsAsync([highPriority]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);
        Assert.Equal(highPriority.Id, claimed.Id);
    }

    [Fact]
    public async Task Claim_RespectsQueuePriority()
    {
        var highQueueName = "high-" + Guid.CreateVersion7().ToString("N");
        var lowQueueName = "low-" + Guid.CreateVersion7().ToString("N");
        await Store.UpsertQueueAsync(new() { Name = highQueueName, Priority = 10 });
        await Store.UpsertQueueAsync(new() { Name = lowQueueName, Priority = 0 });

        var jobHigh = CreateJob("JobHigh_" + Guid.CreateVersion7().ToString("N"));
        jobHigh.Queue = highQueueName;
        var jobLow = CreateJob("JobLow_" + Guid.CreateVersion7().ToString("N"));
        jobLow.Queue = lowQueueName;
        await Store.UpsertJobAsync(jobHigh);
        await Store.UpsertJobAsync(jobLow);

        var runLow = CreateRun(jobLow.Name);
        await Store.CreateRunsAsync([runLow]);

        var runHigh = CreateRun(jobHigh.Name);
        await Store.CreateRunsAsync([runHigh]);

        var claimed = await Store.ClaimRunAsync("node-1",
            [jobHigh.Name, jobLow.Name],
            [highQueueName, lowQueueName]);

        Assert.NotNull(claimed);
        Assert.Equal(runHigh.Id, claimed.Id);
    }

    [Fact]
    public async Task Claim_BatchChildRuns_AreClaimable()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var batchId = Guid.CreateVersion7().ToString("N");
        var child = CreateRun(job.Name) with { BatchId = batchId };
        await Store.CreateRunsAsync([child]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);

        Assert.NotNull(claimed);
        Assert.Equal(child.Id, claimed.Id);
    }

    [Fact]
    public async Task Claim_Concurrent_MaxConcurrency1_OnlyOneWins()
    {
        for (var trial = 0; trial < 10; trial++)
        {
            var jobName = "ConcurrentClaim1_" + Guid.CreateVersion7().ToString("N");
            var job = CreateJob(jobName);
            job.MaxConcurrency = 1;
            await Store.UpsertJobAsync(job);

            var run = CreateRun(jobName);
            await Store.CreateRunsAsync([run]);

            var results = new ConcurrentBag<JobRun?>();

            var tasks = Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
            {
                await Task.Delay(1);
                var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
                results.Add(claimed);
            }));

            await Task.WhenAll(tasks);

            Assert.Equal(1, results.Count(r => r is { }));
        }
    }

    [Fact]
    public async Task Claim_Concurrent_100Runs_10Threads()
    {
        var jobName = "ConcurrentClaim100_" + Guid.CreateVersion7().ToString("N");
        var job = CreateJob(jobName);
        await Store.UpsertJobAsync(job);

        var runIds = new HashSet<string>();
        for (var i = 0; i < 100; i++)
        {
            var run = CreateRun(jobName);
            runIds.Add(run.Id);
            await Store.CreateRunsAsync([run]);
        }

        var claimedIds = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, 10).Select(t => Task.Run(async () =>
        {
            while (true)
            {
                var claimed = await Store.ClaimRunAsync($"node-{t}", [jobName], ["default"]);
                if (claimed is null)
                {
                    break;
                }

                claimedIds.Add(claimed.Id);
            }
        }));

        await Task.WhenAll(tasks);

        Assert.Equal(100, claimedIds.Count);
        Assert.Equal(100, claimedIds.Distinct().Count());
        Assert.True(runIds.SetEquals(claimedIds));
    }

    [Fact]
    public async Task Claim_Concurrent_250Runs_20Threads()
    {
        var jobName = "ConcurrentClaim250_" + Guid.CreateVersion7().ToString("N");
        var job = CreateJob(jobName);
        await Store.UpsertJobAsync(job);

        var runIds = new HashSet<string>();
        for (var i = 0; i < 250; i++)
        {
            var run = CreateRun(jobName);
            runIds.Add(run.Id);
            await Store.CreateRunsAsync([run]);
        }

        var claimedIds = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, 20).Select(t => Task.Run(async () =>
        {
            while (true)
            {
                var claimed = await Store.ClaimRunAsync($"node-{t}", [jobName], ["default"]);
                if (claimed is null)
                {
                    break;
                }

                claimedIds.Add(claimed.Id);
            }
        }));

        await Task.WhenAll(tasks);

        Assert.Equal(250, claimedIds.Count);
        Assert.Equal(250, claimedIds.Distinct().Count());
        Assert.True(runIds.SetEquals(claimedIds));
    }

    [Fact]
    public async Task Claim_RespectsRateLimit_FixedWindow()
    {
        var rateLimitName = "fixed-" + Guid.CreateVersion7().ToString("N");
        await Store.UpsertRateLimitAsync(new()
        {
            Name = rateLimitName,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromMinutes(10)
        });

        var job = CreateJob("RateLimitFixed_" + Guid.CreateVersion7().ToString("N"));
        job.RateLimitName = rateLimitName;
        await Store.UpsertJobAsync(job);

        var run1 = CreateRun(job.Name);
        var run2 = CreateRun(job.Name);
        await Store.CreateRunsAsync([run1]);
        await Store.CreateRunsAsync([run2]);

        var claimed1 = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed1);

        var claimed2 = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.Null(claimed2);
    }

    [Fact]
    public async Task Claim_RespectsRateLimit_SlidingWindow()
    {
        var rateLimitName = "sliding-" + Guid.CreateVersion7().ToString("N");
        await Store.UpsertRateLimitAsync(new()
        {
            Name = rateLimitName,
            Type = RateLimitType.SlidingWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromMinutes(10)
        });

        var job = CreateJob("RateLimitSliding_" + Guid.CreateVersion7().ToString("N"));
        job.RateLimitName = rateLimitName;
        await Store.UpsertJobAsync(job);

        var run1 = CreateRun(job.Name);
        var run2 = CreateRun(job.Name);
        await Store.CreateRunsAsync([run1]);
        await Store.CreateRunsAsync([run2]);

        var claimed1 = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed1);

        var claimed2 = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.Null(claimed2);
    }

    [Fact]
    public async Task Claim_RespectsNotBeforeOrdering()
    {
        var jobName = $"NotBeforeOrder_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var baseTime = TruncateToMilliseconds(DateTimeOffset.UtcNow).AddMinutes(-5);
        var laterRun = CreateRun(jobName) with { NotBefore = baseTime.AddMinutes(2) };
        var earlierRun = CreateRun(jobName) with { NotBefore = baseTime.AddMinutes(1) };

        await Store.CreateRunsAsync([laterRun, earlierRun]);

        var first = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(first);
        Assert.Equal(earlierRun.Id, first.Id);

        var second = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(second);
        Assert.Equal(laterRun.Id, second.Id);
    }

    [Fact]
    public async Task Claim_RespectsQueueRateLimit()
    {
        var jobName = $"QueueRL_{Guid.CreateVersion7():N}";
        var rateLimitName = $"rl_{Guid.CreateVersion7():N}";
        var queueName = $"q_{Guid.CreateVersion7():N}";

        await Store.UpsertRateLimitAsync(new()
        {
            Name = rateLimitName,
            Type = RateLimitType.FixedWindow,
            MaxPermits = 1,
            Window = TimeSpan.FromHours(1)
        });
        await Store.UpsertQueueAsync(new()
        {
            Name = queueName,
            RateLimitName = rateLimitName
        });

        var job = CreateJob(jobName);
        job.Queue = queueName;
        await Store.UpsertJobAsync(job);

        var run1 = CreateRun(jobName);
        var run2 = CreateRun(jobName);
        await Store.CreateRunsAsync([run1, run2]);

        var claimed1 = await Store.ClaimRunAsync("node-1", [jobName], [queueName]);
        Assert.NotNull(claimed1);

        var claimed2 = await Store.ClaimRunAsync("node-1", [jobName], [queueName]);
        Assert.Null(claimed2);
    }

    [Fact]
    public async Task Claim_Concurrent_RateLimit1_OnlyOneWins()
    {
        for (var trial = 0; trial < 10; trial++)
        {
            var rateLimitName = "rl-" + Guid.CreateVersion7().ToString("N");
            await Store.UpsertRateLimitAsync(new()
            {
                Name = rateLimitName,
                Type = RateLimitType.FixedWindow,
                MaxPermits = 1,
                Window = TimeSpan.FromMinutes(1)
            });

            var jobName = "ConcurrentRL_" + Guid.CreateVersion7().ToString("N");
            var job = CreateJob(jobName);
            job.RateLimitName = rateLimitName;
            await Store.UpsertJobAsync(job);

            for (var i = 0; i < 5; i++)
            {
                var run = CreateRun(jobName);
                await Store.CreateRunsAsync([run]);
            }

            var results = new ConcurrentBag<JobRun?>();

            var tasks = Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
            {
                var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
                results.Add(claimed);
            }));

            await Task.WhenAll(tasks);

            Assert.Equal(1, results.Count(r => r is { }));
        }
    }

    [Fact]
    public async Task Claim_TiebreaksById_WhenPriorityAndNotBeforeEqual()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow).AddMinutes(-1);
        var run1 = CreateRun(job.Name) with { NotBefore = now };
        var run2 = CreateRun(job.Name) with { NotBefore = now };
        var run3 = CreateRun(job.Name) with { NotBefore = now };

        await Store.CreateRunsAsync([run1, run2, run3]);

        var expectedOrder = new[] { run1.Id, run2.Id, run3.Id }.Order().ToList();

        var claimed1 = await Store.ClaimRunAsync("node", [job.Name], ["default"]);
        var claimed2 = await Store.ClaimRunAsync("node", [job.Name], ["default"]);
        var claimed3 = await Store.ClaimRunAsync("node", [job.Name], ["default"]);

        Assert.NotNull(claimed1);
        Assert.NotNull(claimed2);
        Assert.NotNull(claimed3);
        Assert.Equal(expectedOrder[0], claimed1.Id);
        Assert.Equal(expectedOrder[1], claimed2.Id);
        Assert.Equal(expectedOrder[2], claimed3.Id);
    }

    [Fact]
    public async Task Claim_SkipsExpiredNotAfter()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name) with { NotAfter = DateTimeOffset.UtcNow.AddMinutes(-1) };
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);

        Assert.Null(claimed);
    }

    [Fact]
    public async Task Claim_AllowsFutureNotAfter()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name) with { NotAfter = DateTimeOffset.UtcNow.AddHours(1) };
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);

        Assert.NotNull(claimed);
        Assert.Equal(run.Id, claimed.Id);
    }

    [Fact]
    public async Task Claim_RespectsQueuePriorityChange_ExistingPendingRuns()
    {
        var queueA = "qa-" + Guid.CreateVersion7().ToString("N");
        var queueB = "qb-" + Guid.CreateVersion7().ToString("N");
        await Store.UpsertQueueAsync(new() { Name = queueA, Priority = 0 });
        await Store.UpsertQueueAsync(new() { Name = queueB, Priority = 10 });

        var jobA = CreateJob("JobA_" + Guid.CreateVersion7().ToString("N"));
        jobA.Queue = queueA;
        var jobB = CreateJob("JobB_" + Guid.CreateVersion7().ToString("N"));
        jobB.Queue = queueB;
        await Store.UpsertJobAsync(jobA);
        await Store.UpsertJobAsync(jobB);

        var runA = CreateRun(jobA.Name);
        var runB = CreateRun(jobB.Name);
        await Store.CreateRunsAsync([runA]);
        await Store.CreateRunsAsync([runB]);

        // Flip priorities: queueA becomes high, queueB becomes low
        await Store.UpsertQueueAsync(new() { Name = queueA, Priority = 20 });
        await Store.UpsertQueueAsync(new() { Name = queueB, Priority = 0 });

        var claimed = await Store.ClaimRunAsync("node-1",
            [jobA.Name, jobB.Name], [queueA, queueB]);

        Assert.NotNull(claimed);
        Assert.Equal(runA.Id, claimed.Id);
    }

    [Fact]
    public async Task Claim_EqualQueuePriority_GlobalRunPriorityOrdering()
    {
        var queueA = "eq-a-" + Guid.CreateVersion7().ToString("N");
        var queueB = "eq-b-" + Guid.CreateVersion7().ToString("N");
        await Store.UpsertQueueAsync(new() { Name = queueA, Priority = 10 });
        await Store.UpsertQueueAsync(new() { Name = queueB, Priority = 10 });

        var jobA = CreateJob("EqJobA_" + Guid.CreateVersion7().ToString("N"));
        jobA.Queue = queueA;
        var jobB = CreateJob("EqJobB_" + Guid.CreateVersion7().ToString("N"));
        jobB.Queue = queueB;
        await Store.UpsertJobAsync(jobA);
        await Store.UpsertJobAsync(jobB);

        var lowerPriority = CreateRun(jobA.Name) with { Priority = 1 };
        var higherPriority = CreateRun(jobB.Name) with { Priority = 100 };

        await Store.CreateRunsAsync([lowerPriority]);
        await Store.CreateRunsAsync([higherPriority]);

        // Intentionally pass queues in reverse lexical order to ensure queue input order
        // cannot influence cross-queue ordering when priorities are equal.
        var claimed = await Store.ClaimRunAsync("node-1", [jobA.Name, jobB.Name], [queueB, queueA]);

        Assert.NotNull(claimed);
        Assert.Equal(higherPriority.Id, claimed.Id);
    }
}