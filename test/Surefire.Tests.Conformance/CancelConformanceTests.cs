using System.Collections.Concurrent;

namespace Surefire.Tests.Conformance;

public abstract class CancelConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task TryCancelRun_Pending_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var result = await Store.TryCancelRunAsync(run.Id, cancellationToken: ct);

        Assert.True(result.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Cancelled, stored.Status);
        Assert.NotNull(stored.CancelledAt);
        Assert.NotNull(stored.CompletedAt);
    }

    [Fact]
    public async Task TryCancelRun_Running_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"], ct);
        Assert.NotNull(claimed);

        var result = await Store.TryCancelRunAsync(run.Id, cancellationToken: ct);

        Assert.True(result.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Cancelled, stored.Status);
        Assert.NotNull(stored.CancelledAt);
        Assert.NotNull(stored.CompletedAt);
    }

    [Fact]
    public async Task TryCancelRun_SetsCancelledAtAndCompletedAtToSameInstant()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var result = await Store.TryCancelRunAsync(run.Id, cancellationToken: ct);

        Assert.True(result.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Cancelled, stored.Status);
        Assert.NotNull(stored.CancelledAt);
        Assert.NotNull(stored.CompletedAt);
        Assert.Equal(stored.CancelledAt, stored.CompletedAt);
    }

    [Fact]
    public async Task TryCancelRun_Terminal_ReturnsFalse()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"], ct);
        Assert.NotNull(claimed);

        claimed = claimed with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        var ok = await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running), ct);
        Assert.True(ok.Transitioned);

        var result = await Store.TryCancelRunAsync(run.Id, cancellationToken: ct);

        Assert.False(result.Transitioned);
    }

    [Fact]
    public async Task TryCancelRun_NonExistent_ReturnsFalse()
    {
        var ct = TestContext.Current.CancellationToken;
        var result = await Store.TryCancelRunAsync("nonexistent-id", cancellationToken: ct);
        Assert.False(result.Transitioned);
    }

    [Fact]
    public async Task CancelChildRuns_CancelsAllNonTerminalChildren()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);

        // Create a parent + 3 children (ParentRunId relationship)
        var parent = CreateRun(job.Name, JobStatus.Running) with
        {
            Attempt = 1,
            NodeName = "node1",
            StartedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow),
            LastHeartbeatAt = TruncateToMilliseconds(DateTimeOffset.UtcNow)
        };

        var child1 = CreateRun(job.Name) with { ParentRunId = parent.Id };
        var child2 = CreateRun(job.Name) with { ParentRunId = parent.Id };
        var child3 = CreateRun(job.Name) with { ParentRunId = parent.Id };

        await Store.CreateRunsAsync([parent, child1, child2, child3], cancellationToken: ct);

        // Claim and complete one child so it's terminal (whichever the store picks first)
        var claimed = await Store.ClaimRunAsync("node-1", [job.Name], ["default"], ct);
        Assert.NotNull(claimed);
        var completedChildId = claimed.Id;
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        Assert.True((await Store.TryTransitionRunAsync(RunStatusTransition.RunningToSucceeded(
            completedChildId, claimed.Attempt, now, claimed.NotBefore, "node-1", 1, null, null,
            claimed.StartedAt, now), ct)).Transitioned);

        // Cancel all children of coordinator
        var cancelledIds = await Store.CancelChildRunsAsync(parent.Id, cancellationToken: ct);

        // The completed child should not be cancelled; the other 2 should be
        Assert.Equal(2, cancelledIds.Count);
        Assert.DoesNotContain(completedChildId, cancelledIds);

        // Verify the completed child remains completed
        var storedCompleted = await Store.GetRunAsync(completedChildId, ct);
        Assert.Equal(JobStatus.Succeeded, storedCompleted!.Status);

        // Verify cancelled children
        foreach (var cancelledId in cancelledIds)
        {
            var stored = await Store.GetRunAsync(cancelledId, ct);
            Assert.NotNull(stored);
            Assert.Equal(JobStatus.Cancelled, stored.Status);
            Assert.NotNull(stored.CancelledAt);
        }
    }

    [Fact]
    public async Task CancelChildRuns_NoChildren_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var cancelledIds = await Store.CancelChildRunsAsync(run.Id, cancellationToken: ct);

        Assert.Empty(cancelledIds);
    }

    [Fact]
    public async Task CancelChildRuns_AllTerminal_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);

        var parent = CreateRun(job.Name, JobStatus.Running) with
        {
            Attempt = 1,
            NodeName = "node1",
            StartedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow),
            LastHeartbeatAt = TruncateToMilliseconds(DateTimeOffset.UtcNow)
        };

        var child = CreateRun(job.Name) with { ParentRunId = parent.Id };

        await Store.CreateRunsAsync([parent, child], cancellationToken: ct);

        // Cancel the child first
        Assert.True((await Store.TryCancelRunAsync(child.Id, cancellationToken: ct)).Transitioned);

        // Now CancelChildRuns should find nothing to cancel
        var cancelledIds = await Store.CancelChildRunsAsync(parent.Id, cancellationToken: ct);
        Assert.Empty(cancelledIds);
    }

    [Fact]
    public async Task TryCancelRun_Concurrent_WithRetry_OneWins()
    {
        var ct = TestContext.Current.CancellationToken;
        for (var trial = 0; trial < 10; trial++)
        {
            var jobName = "ConcurrentCancel_" + Guid.CreateVersion7().ToString("N");
            var job = CreateJob(jobName);
            await Store.UpsertJobAsync(job, ct);

            var run = CreateRun(jobName);
            await Store.CreateRunsAsync([run], cancellationToken: ct);

            var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
            Assert.NotNull(claimed);

            var cancelResults = new ConcurrentBag<bool>();
            var casResults = new ConcurrentBag<bool>();

            var tasks = Enumerable.Range(0, 10).Select(i => Task.Run(async () =>
            {
                await Task.Delay(1, ct);

                if (i < 5)
                {
                    var ok = await Store.TryCancelRunAsync(run.Id, cancellationToken: ct);
                    cancelResults.Add(ok.Transitioned);
                }
                else
                {
                    var attempt = new JobRun
                    {
                        Id = claimed.Id,
                        JobName = claimed.JobName,
                        Status = JobStatus.Pending,
                        Attempt = claimed.Attempt,
                        CreatedAt = claimed.CreatedAt,
                        NotBefore = claimed.NotBefore
                    };
                    var ok = await Store.TryTransitionRunAsync(Transition(attempt, JobStatus.Running), ct);
                    casResults.Add(ok.Transitioned);
                }
            }));

            await Task.WhenAll(tasks);

            // At most one CAS from Running->Pending succeeds. Zero is valid when all cancel
            // threads beat the CAS threads (Running->Cancelled happens first for all of them).
            Assert.True(casResults.Count(r => r) <= 1);

            // Run must end in a valid state.
            var stored = await Store.GetRunAsync(run.Id, ct);
            Assert.NotNull(stored);
            Assert.True(stored.Status is JobStatus.Cancelled or JobStatus.Pending);
        }
    }
}