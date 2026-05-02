using System.Collections.Concurrent;

namespace Surefire.Tests.Conformance;

public abstract class CancelConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task TryCancelRun_Pending_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var result = await Store.TryCancelRunAsync(run.Id, cancellationToken: ct);

        Assert.True(result.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Canceled, stored.Status);
        Assert.NotNull(stored.CanceledAt);
        Assert.NotNull(stored.CompletedAt);
    }

    [Fact]
    public async Task TryCancelRun_Running_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        var result = await Store.TryCancelRunAsync(run.Id, cancellationToken: ct);

        Assert.True(result.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Canceled, stored.Status);
        Assert.NotNull(stored.CanceledAt);
        Assert.NotNull(stored.CompletedAt);
    }

    [Fact]
    public async Task TryCancelRun_SetsCanceledAtAndCompletedAtToSameInstant()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var result = await Store.TryCancelRunAsync(run.Id, cancellationToken: ct);

        Assert.True(result.Transitioned);

        var stored = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Canceled, stored.Status);
        Assert.NotNull(stored.CanceledAt);
        Assert.NotNull(stored.CompletedAt);
        Assert.Equal(stored.CanceledAt, stored.CompletedAt);
    }

    [Fact]
    public async Task TryCancelRun_Terminal_ReturnsFalse()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
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
    public async Task CancelRunSubtree_DescendantsOnly_CancelsAllNonTerminalChildren()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

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
        var claimed = (await Store.ClaimRunsAsync("node-1", [job.Name], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);
        var completedChildId = claimed.Id;
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        Assert.True((await Store.TryTransitionRunAsync(RunStatusTransition.RunningToSucceeded(
            completedChildId, claimed.Attempt, now, claimed.NotBefore, "node-1", 1, null, null,
            claimed.StartedAt, now), ct)).Transitioned);

        var result = await Store.CancelRunSubtreeAsync(parent.Id, includeRoot: false, cancellationToken: ct);
        var canceledIds = result.Runs.Select(r => r.RunId).ToList();

        // The completed child should not be Canceled; the other 2 should be.
        // Parent must be untouched (includeRoot: false).
        Assert.Equal(2, canceledIds.Count);
        Assert.DoesNotContain(completedChildId, canceledIds);
        Assert.DoesNotContain(parent.Id, canceledIds);

        var storedCompleted = await Store.GetRunAsync(completedChildId, ct);
        Assert.Equal(JobStatus.Succeeded, storedCompleted!.Status);

        var storedParent = await Store.GetRunAsync(parent.Id, ct);
        Assert.Equal(JobStatus.Running, storedParent!.Status);

        foreach (var canceledId in canceledIds)
        {
            var stored = await Store.GetRunAsync(canceledId, ct);
            Assert.NotNull(stored);
            Assert.Equal(JobStatus.Canceled, stored.Status);
            Assert.NotNull(stored.CanceledAt);
        }
    }

    [Fact]
    public async Task CancelRunSubtree_DescendantsOnly_NoChildren_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var result = await Store.CancelRunSubtreeAsync(run.Id, includeRoot: false, cancellationToken: ct);

        Assert.Empty(result.Runs);
    }

    [Fact]
    public async Task CancelRunSubtree_DescendantsOnly_AllTerminal_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var parent = CreateRun(job.Name, JobStatus.Running) with
        {
            Attempt = 1,
            NodeName = "node1",
            StartedAt = TruncateToMilliseconds(DateTimeOffset.UtcNow),
            LastHeartbeatAt = TruncateToMilliseconds(DateTimeOffset.UtcNow)
        };

        var child = CreateRun(job.Name) with { ParentRunId = parent.Id };

        await Store.CreateRunsAsync([parent, child], cancellationToken: ct);

        Assert.True((await Store.TryCancelRunAsync(child.Id, cancellationToken: ct)).Transitioned);

        // Subtree walk must find nothing else to cancel.
        var result = await Store.CancelRunSubtreeAsync(parent.Id, includeRoot: false, cancellationToken: ct);
        Assert.Empty(result.Runs);
    }

    [Fact]
    public async Task CancelRunSubtree_IncludesRoot_CancelsRootAndDescendants()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var root = CreateRun(job.Name);
        var child = CreateRun(job.Name) with { ParentRunId = root.Id };
        var grandchild = CreateRun(job.Name) with { ParentRunId = child.Id };

        await Store.CreateRunsAsync([root, child, grandchild], cancellationToken: ct);

        var result = await Store.CancelRunSubtreeAsync(root.Id, cancellationToken: ct);
        var canceledIds = result.Runs.Select(r => r.RunId).ToHashSet();

        Assert.Equal(3, canceledIds.Count);
        Assert.Contains(root.Id, canceledIds);
        Assert.Contains(child.Id, canceledIds);
        Assert.Contains(grandchild.Id, canceledIds);

        foreach (var id in canceledIds)
        {
            var stored = await Store.GetRunAsync(id, ct);
            Assert.NotNull(stored);
            Assert.Equal(JobStatus.Canceled, stored.Status);
        }
    }

    [Fact]
    public async Task CancelRunSubtree_DeepChain_CancelsAllLevelsInOneCall()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        // Build a 5-level chain: r0 -> r1 -> r2 -> r3 -> r4.
        var runs = new List<JobRun>();
        string? parentId = null;
        for (var i = 0; i < 5; i++)
        {
            var r = parentId is null
                ? CreateRun(job.Name)
                : CreateRun(job.Name) with { ParentRunId = parentId };
            runs.Add(r);
            parentId = r.Id;
        }

        await Store.CreateRunsAsync(runs, cancellationToken: ct);

        var result = await Store.CancelRunSubtreeAsync(runs[0].Id, includeRoot: false, cancellationToken: ct);
        var canceledIds = result.Runs.Select(r => r.RunId).ToHashSet();

        // Every descendant (4 of them) cancels, root left alone.
        Assert.Equal(4, canceledIds.Count);
        Assert.DoesNotContain(runs[0].Id, canceledIds);
        for (var i = 1; i < runs.Count; i++)
        {
            Assert.Contains(runs[i].Id, canceledIds);
        }
    }

    [Fact]
    public async Task CancelRunSubtree_NonExistentRun_ReturnsNotFound()
    {
        var ct = TestContext.Current.CancellationToken;

        var result = await Store.CancelRunSubtreeAsync(
            Guid.CreateVersion7().ToString("N"), cancellationToken: ct);

        Assert.False(result.Found);
        Assert.Empty(result.Runs);
        Assert.Empty(result.CompletedBatches);
    }

    [Fact]
    public async Task CancelRunSubtree_AlreadyTerminalRoot_ReturnsFoundEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);
        Assert.True((await Store.TryCancelRunAsync(run.Id, cancellationToken: ct)).Transitioned);

        // Root exists but is already terminal and has no descendants — Found=true, Runs empty.
        var result = await Store.CancelRunSubtreeAsync(run.Id, cancellationToken: ct);

        Assert.True(result.Found);
        Assert.Empty(result.Runs);
    }

    [Fact]
    public async Task CancelBatchSubtree_NonExistentBatch_ReturnsNotFound()
    {
        var ct = TestContext.Current.CancellationToken;

        var result = await Store.CancelBatchSubtreeAsync(
            Guid.CreateVersion7().ToString("N"), cancellationToken: ct);

        Assert.False(result.Found);
        Assert.Empty(result.Runs);
        Assert.Empty(result.CompletedBatches);
    }

    [Fact]
    public async Task TryCancelRun_Concurrent_WithRetry_OneWins()
    {
        var ct = TestContext.Current.CancellationToken;
        for (var trial = 0; trial < 10; trial++)
        {
            var jobName = "ConcurrentCancel_" + Guid.CreateVersion7().ToString("N");
            var job = CreateJob(jobName);
            await Store.UpsertJobsAsync([job], ct);

            var run = CreateRun(jobName);
            await Store.CreateRunsAsync([run], cancellationToken: ct);

            var claimed = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
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
            // threads beat the CAS threads (Running->Canceled happens first for all of them).
            Assert.True(casResults.Count(r => r) <= 1);

            // Run must end in a valid state.
            var stored = await Store.GetRunAsync(run.Id, ct);
            Assert.NotNull(stored);
            Assert.True(stored.Status is JobStatus.Canceled or JobStatus.Pending);
        }
    }
}
