using System.Collections.Concurrent;

namespace Surefire.Tests.Conformance;

public abstract class CancelConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task TryCancelRun_Pending_Succeeds()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var result = await Store.TryCancelRunAsync(run.Id);

        Assert.True(result);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Cancelled, stored.Status);
        Assert.NotNull(stored.CancelledAt);
        Assert.NotNull(stored.CompletedAt);
    }

    [Fact]
    public async Task TryCancelRun_Running_Succeeds()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);

        var result = await Store.TryCancelRunAsync(run.Id);

        Assert.True(result);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Cancelled, stored.Status);
        Assert.NotNull(stored.CancelledAt);
        Assert.NotNull(stored.CompletedAt);
    }

    [Fact]
    public async Task TryCancelRun_Retrying_Succeeds()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.Retrying;
        var ok = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));
        Assert.True(ok);

        var result = await Store.TryCancelRunAsync(run.Id);

        Assert.True(result);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Cancelled, stored.Status);
        Assert.NotNull(stored.CancelledAt);
        Assert.NotNull(stored.CompletedAt);
    }

    [Fact]
    public async Task TryCancelRun_SetsCancelledAtAndCompletedAtToSameInstant()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var result = await Store.TryCancelRunAsync(run.Id);

        Assert.True(result);

        var stored = await Store.GetRunAsync(run.Id);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Cancelled, stored.Status);
        Assert.NotNull(stored.CancelledAt);
        Assert.NotNull(stored.CompletedAt);
        Assert.Equal(stored.CancelledAt, stored.CompletedAt);
    }

    [Fact]
    public async Task TryCancelRun_Terminal_ReturnsFalse()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.Succeeded;
        claimed.CompletedAt = DateTimeOffset.UtcNow;
        var ok = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));
        Assert.True(ok);

        var result = await Store.TryCancelRunAsync(run.Id);

        Assert.False(result);
    }

    [Fact]
    public async Task TryCancelRun_NonExistent_ReturnsFalse()
    {
        var result = await Store.TryCancelRunAsync("nonexistent-id");
        Assert.False(result);
    }

    [Fact]
    public async Task CancelChildRuns_CancelsAllNonTerminalChildren()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        // Create a batch: coordinator + 3 children
        var coordinator = CreateRun(job.Name);
        coordinator.Status = JobStatus.Running;
        coordinator.BatchTotal = 3;
        coordinator.BatchCompleted = 0;
        coordinator.BatchFailed = 0;

        var child1 = CreateRun(job.Name);
        child1.ParentRunId = coordinator.Id;

        var child2 = CreateRun(job.Name);
        child2.ParentRunId = coordinator.Id;

        var child3 = CreateRun(job.Name);
        child3.ParentRunId = coordinator.Id;

        await Store.CreateRunsAsync([coordinator, child1, child2, child3]);

        // Claim and complete one child so it's terminal (whichever the store picks first)
        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"]);
        Assert.NotNull(claimed);
        var completedChildId = claimed.Id;
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        Assert.True(await Store.TryTransitionRunAsync(RunStatusTransition.RunningToSucceeded(
            completedChildId, claimed.Attempt, now, claimed.NotBefore, "node-1", 1, null, null,
            claimed.StartedAt, now)));

        // Cancel all children of coordinator
        var cancelledIds = await Store.CancelChildRunsAsync(coordinator.Id);

        // The completed child should not be cancelled; the other 2 should be
        Assert.Equal(2, cancelledIds.Count);
        Assert.DoesNotContain(completedChildId, cancelledIds);

        // Verify the completed child remains completed
        var storedCompleted = await Store.GetRunAsync(completedChildId);
        Assert.Equal(JobStatus.Succeeded, storedCompleted!.Status);

        // Verify cancelled children
        foreach (var cancelledId in cancelledIds)
        {
            var stored = await Store.GetRunAsync(cancelledId);
            Assert.NotNull(stored);
            Assert.Equal(JobStatus.Cancelled, stored.Status);
            Assert.NotNull(stored.CancelledAt);
        }
    }

    [Fact]
    public async Task CancelChildRuns_NoChildren_ReturnsEmpty()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var cancelledIds = await Store.CancelChildRunsAsync(run.Id);

        Assert.Empty(cancelledIds);
    }

    [Fact]
    public async Task CancelChildRuns_AllTerminal_ReturnsEmpty()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);

        var coordinator = CreateRun(job.Name);
        coordinator.Status = JobStatus.Running;
        coordinator.BatchTotal = 1;
        coordinator.BatchCompleted = 0;
        coordinator.BatchFailed = 0;

        var child = CreateRun(job.Name);
        child.ParentRunId = coordinator.Id;

        await Store.CreateRunsAsync([coordinator, child]);

        // Cancel the child first
        Assert.True(await Store.TryCancelRunAsync(child.Id));

        // Now CancelChildRuns should find nothing to cancel
        var cancelledIds = await Store.CancelChildRunsAsync(coordinator.Id);
        Assert.Empty(cancelledIds);
    }

    [Fact]
    public async Task TryCancelRun_Concurrent_WithRetry_OneWins()
    {
        for (var trial = 0; trial < 10; trial++)
        {
            var jobName = "ConcurrentCancel_" + Guid.CreateVersion7().ToString("N");
            var job = CreateJob(jobName);
            await Store.UpsertJobAsync(job);

            var run = CreateRun(jobName);
            await Store.CreateRunsAsync([run]);

            var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
            Assert.NotNull(claimed);

            var cancelResults = new ConcurrentBag<bool>();
            var casResults = new ConcurrentBag<bool>();

            var tasks = Enumerable.Range(0, 10).Select(i => Task.Run(async () =>
            {
                await Task.Delay(1);

                if (i < 5)
                {
                    var ok = await Store.TryCancelRunAsync(run.Id);
                    cancelResults.Add(ok);
                }
                else
                {
                    var attempt = new RunRecord
                    {
                        Id = claimed.Id,
                        JobName = claimed.JobName,
                        Status = JobStatus.Retrying,
                        Attempt = claimed.Attempt,
                        CreatedAt = claimed.CreatedAt,
                        NotBefore = claimed.NotBefore
                    };
                    var ok = await Store.TryTransitionRunAsync(Transition(attempt, JobStatus.Running));
                    casResults.Add(ok);
                }
            }));

            await Task.WhenAll(tasks);

            // At most one CAS from Running succeeds
            Assert.True(casResults.Count(r => r) <= 1);

            // Run must end in a valid state. If CAS succeeded (Running->Retrying),
            // cancel can also succeed (Retrying->Cancelled) since Retrying is non-terminal.
            var stored = await Store.GetRunAsync(run.Id);
            Assert.NotNull(stored);
            Assert.True(stored.Status is JobStatus.Cancelled or JobStatus.Retrying);
        }
    }
}