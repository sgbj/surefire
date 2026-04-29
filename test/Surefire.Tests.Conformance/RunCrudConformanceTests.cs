namespace Surefire.Tests.Conformance;

public abstract class RunCrudConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task CreateRuns_Persists()
    {
        var ct = TestContext.Current.CancellationToken;
        var run = CreateRun();
        run = run with { Arguments = """{"key":"value"}""", Priority = 5, DeduplicationId = "dedup-1" };

        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
        Assert.Equal(run.Id, loaded.Id);
        Assert.Equal(run.JobName, loaded.JobName);
        Assert.Equal(JobStatus.Pending, loaded.Status);
        Assert.Equal("""{"key":"value"}""", loaded.Arguments);
        Assert.Equal(5, loaded.Priority);
        Assert.Equal("dedup-1", loaded.DeduplicationId);
        Assert.Equal(run.CreatedAt, loaded.CreatedAt);
        Assert.Equal(run.NotBefore, loaded.NotBefore);
    }

    [Fact]
    public async Task CreateRuns_BatchAtomic()
    {
        var ct = TestContext.Current.CancellationToken;
        var runs = Enumerable.Range(0, 5)
            .Select(_ => CreateRun())
            .ToList();

        await Store.CreateRunsAsync(runs, cancellationToken: ct);

        foreach (var run in runs)
        {
            var loaded = await Store.GetRunAsync(run.Id, ct);
            Assert.NotNull(loaded);
            Assert.Equal(run.Id, loaded.Id);
        }
    }

    [Fact]
    public async Task CreateRuns_WithInitialEvents_PersistsAtomically()
    {
        var ct = TestContext.Current.CancellationToken;
        var run = CreateRun();
        var evt = new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.InputDeclared,
            Payload = "{\"arguments\":[\"input\"]}",
            CreatedAt = run.CreatedAt,
            Attempt = 0
        };

        await Store.CreateRunsAsync([run], [evt], ct);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);

        var events = await Store.GetEventsAsync(run.Id, cancellationToken: ct);
        Assert.Single(events);
        Assert.Equal(RunEventType.InputDeclared, events[0].EventType);
        Assert.Equal(evt.Payload, events[0].Payload);
    }

    [Fact]
    public async Task CreateRuns_BatchWithInitialEvents_PersistsForAll()
    {
        var ct = TestContext.Current.CancellationToken;
        var runA = CreateRun();
        var runB = CreateRun();
        var events = new List<RunEvent>
        {
            new()
            {
                RunId = runA.Id,
                EventType = RunEventType.InputDeclared,
                Payload = "{\"arguments\":[\"a\"]}",
                CreatedAt = runA.CreatedAt,
                Attempt = 0
            },
            new()
            {
                RunId = runB.Id,
                EventType = RunEventType.InputDeclared,
                Payload = "{\"arguments\":[\"b\"]}",
                CreatedAt = runB.CreatedAt,
                Attempt = 0
            }
        };

        await Store.CreateRunsAsync([runA, runB], events, ct);

        var eventsA = await Store.GetEventsAsync(runA.Id, cancellationToken: ct);
        var eventsB = await Store.GetEventsAsync(runB.Id, cancellationToken: ct);

        Assert.Single(eventsA);
        Assert.Single(eventsB);
        Assert.Equal("{\"arguments\":[\"a\"]}", eventsA[0].Payload);
        Assert.Equal("{\"arguments\":[\"b\"]}", eventsB[0].Payload);
    }

    [Fact]
    public async Task CreateRun_RerunOfRunId_RoundTrips()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"Rerun_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var originalRun = CreateRun(jobName);
        var rerun = CreateRun(jobName) with { RerunOfRunId = originalRun.Id };

        await Store.CreateRunsAsync([originalRun, rerun], cancellationToken: ct);

        var stored = await Store.GetRunAsync(rerun.Id, ct);
        Assert.NotNull(stored);
        Assert.Equal(originalRun.Id, stored.RerunOfRunId);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_RejectsDuplicate()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"Dedup_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");

        var run1 = CreateRun(jobName) with { DeduplicationId = dedupId };
        var result1 = await Store.TryCreateRunAsync(run1, cancellationToken: ct);
        Assert.True(result1);

        var run2 = CreateRun(jobName) with { DeduplicationId = dedupId };
        var result2 = await Store.TryCreateRunAsync(run2, cancellationToken: ct);
        Assert.False(result2);
    }

    [Fact]
    public async Task TryCreateRun_WithInitialEvents_PersistsOnSuccess()
    {
        var ct = TestContext.Current.CancellationToken;
        var run = CreateRun($"InitialEvents_{Guid.CreateVersion7():N}");
        var evt = new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.InputDeclared,
            Payload = "{\"arguments\":[\"values\"]}",
            CreatedAt = run.CreatedAt,
            Attempt = 0
        };

        var created = await Store.TryCreateRunAsync(run, initialEvents: [evt], cancellationToken: ct);
        Assert.True(created);

        var events = await Store.GetEventsAsync(run.Id, cancellationToken: ct);
        Assert.Single(events);
        Assert.Equal(RunEventType.InputDeclared, events[0].EventType);
    }

    [Fact]
    public async Task TryCreateRun_Rejected_DoesNotPersistInitialEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"InitialEventsReject_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");

        var run1 = CreateRun(jobName) with { DeduplicationId = dedupId };
        var ok = await Store.TryCreateRunAsync(run1, cancellationToken: ct);
        Assert.True(ok);

        var run2 = CreateRun(jobName) with { DeduplicationId = dedupId };
        var evt = new RunEvent
        {
            RunId = run2.Id,
            EventType = RunEventType.InputDeclared,
            Payload = "{\"arguments\":[\"values\"]}",
            CreatedAt = run2.CreatedAt,
            Attempt = 0
        };

        var created = await Store.TryCreateRunAsync(run2, initialEvents: [evt], cancellationToken: ct);
        Assert.False(created);

        var loaded = await Store.GetRunAsync(run2.Id, ct);
        Assert.Null(loaded);

        var events = await Store.GetEventsAsync(run2.Id, cancellationToken: ct);
        Assert.Empty(events);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_DifferentJobs_BothSucceed()
    {
        var ct = TestContext.Current.CancellationToken;
        var dedupId = Guid.CreateVersion7().ToString("N");

        var run1 = CreateRun($"JobA_{Guid.CreateVersion7():N}") with { DeduplicationId = dedupId };
        var result1 = await Store.TryCreateRunAsync(run1, cancellationToken: ct);
        Assert.True(result1);

        var run2 = CreateRun($"JobB_{Guid.CreateVersion7():N}") with { DeduplicationId = dedupId };
        var result2 = await Store.TryCreateRunAsync(run2, cancellationToken: ct);
        Assert.True(result2);
    }

    [Fact]
    public async Task TryCreateRun_NoDedup_AlwaysSucceeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"NoDedup_{Guid.CreateVersion7():N}";

        var run1 = CreateRun(jobName) with { DeduplicationId = null };
        var result1 = await Store.TryCreateRunAsync(run1, cancellationToken: ct);
        Assert.True(result1);

        var run2 = CreateRun(jobName) with { DeduplicationId = null };
        var result2 = await Store.TryCreateRunAsync(run2, cancellationToken: ct);
        Assert.True(result2);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_FreedOnTerminal()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DedupTerminal_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run1 = CreateRun(jobName) with { DeduplicationId = dedupId };
        var result1 = await Store.TryCreateRunAsync(run1, cancellationToken: ct);
        Assert.True(result1);

        var claimed = (await Store.ClaimRunsAsync("node1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        claimed = claimed with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow };
        await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running), ct);

        var run2 = CreateRun(jobName) with { DeduplicationId = dedupId };
        var result2 = await Store.TryCreateRunAsync(run2, cancellationToken: ct);
        Assert.True(result2);
    }

    [Fact]
    public async Task TryCreateRun_MaxActive_RespectsIsEnabled()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DisabledJob_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        await Store.UpsertJobsAsync([job], ct);
        await Store.SetJobEnabledAsync(jobName, false, ct);

        var run = CreateRun(jobName);
        var result = await Store.TryCreateRunAsync(run, 1, cancellationToken: ct);
        Assert.False(result);
    }

    [Fact]
    public async Task TryCreateRun_MaxActive_EnabledJob_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"EnabledJob_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        await Store.UpsertJobsAsync([job], ct);

        var run = CreateRun(jobName);
        var result = await Store.TryCreateRunAsync(run, 1, cancellationToken: ct);
        Assert.True(result);
    }

    [Fact]
    public async Task TryCreateRun_MaxActive_ExceedsLimit()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"MaxActive_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        await Store.UpsertJobsAsync([job], ct);

        var run1 = CreateRun(jobName);
        var result1 = await Store.TryCreateRunAsync(run1, 1, cancellationToken: ct);
        Assert.True(result1);

        var run2 = CreateRun(jobName);
        var result2 = await Store.TryCreateRunAsync(run2, 1, cancellationToken: ct);
        Assert.False(result2);
    }

    [Fact]
    public async Task TryCreateRun_NoMaxActive_IgnoresIsEnabled()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ManualTrigger_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        await Store.UpsertJobsAsync([job], ct);
        await Store.SetJobEnabledAsync(jobName, false, ct);

        var run = CreateRun(jobName);
        var result = await Store.TryCreateRunAsync(run, cancellationToken: ct);
        Assert.True(result);
    }

    [Fact]
    public async Task GetRun_ReturnsIsolatedCopy()
    {
        var ct = TestContext.Current.CancellationToken;
        var run = CreateRun() with { Arguments = """{"key":"value"}""" };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var copy1 = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(copy1);
        copy1 = copy1 with { Arguments = """{"mutated":true}""", Progress = 0.99 };

        var copy2 = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(copy2);
        Assert.Equal("""{"key":"value"}""", copy2.Arguments);
        Assert.Equal(0.0, copy2.Progress);
    }

    [Fact]
    public async Task GetRun_ReturnsNull_WhenNotFound()
    {
        var ct = TestContext.Current.CancellationToken;
        var result = await Store.GetRunAsync(Guid.CreateVersion7().ToString("N"), ct);
        Assert.Null(result);
    }

    [Fact]
    public async Task GetRuns_FilterByStatus()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StatusFilter_{Guid.CreateVersion7():N}";

        var pending = CreateRun(jobName);
        var completed = CreateRun(jobName, JobStatus.Succeeded) with { CompletedAt = DateTimeOffset.UtcNow };
        var cancelled = CreateRun(jobName, JobStatus.Cancelled) with
        {
            CompletedAt = DateTimeOffset.UtcNow, CancelledAt = DateTimeOffset.UtcNow
        };

        await Store.CreateRunsAsync([pending, completed, cancelled], cancellationToken: ct);

        var pendingResults = await Store.GetRunsAsync(new()
        {
            Status = JobStatus.Pending,
            JobName = jobName
        }, cancellationToken: ct);

        Assert.Single(pendingResults.Items);
        Assert.Equal(pending.Id, pendingResults.Items[0].Id);
    }

    [Fact]
    public async Task GetRuns_FilterByJobName_SubstringMatch()
    {
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.CreateVersion7().ToString("N");
        var run1 = CreateRun($"EmailSender_{suffix}");
        var run2 = CreateRun($"PreEmailTask_{suffix}");
        var run3 = CreateRun($"InvoiceGen_{suffix}");

        await Store.CreateRunsAsync([run1, run2, run3], cancellationToken: ct);

        var results = await Store.GetRunsAsync(new() { JobNameContains = "Email" }, cancellationToken: ct);

        Assert.Contains(results.Items, r => r.Id == run1.Id);
        Assert.Contains(results.Items, r => r.Id == run2.Id);
        Assert.DoesNotContain(results.Items, r => r.Id == run3.Id);
    }

    [Fact]
    public async Task GetRuns_FilterByParentRunId()
    {
        var ct = TestContext.Current.CancellationToken;
        var parentId = Guid.CreateVersion7().ToString("N");
        var parent = CreateRun(id: parentId);

        var child1 = CreateRun() with { ParentRunId = parentId, RootRunId = parentId };

        var child2 = CreateRun() with { ParentRunId = parentId, RootRunId = parentId };

        var unrelated = CreateRun();

        await Store.CreateRunsAsync([parent, child1, child2, unrelated], cancellationToken: ct);

        var results = await Store.GetRunsAsync(new() { ParentRunId = parentId }, cancellationToken: ct);

        Assert.Equal(2, results.TotalCount);
        Assert.Contains(results.Items, r => r.Id == child1.Id);
        Assert.Contains(results.Items, r => r.Id == child2.Id);
    }

    [Fact]
    public async Task GetRuns_FilterByRootRunId()
    {
        var ct = TestContext.Current.CancellationToken;
        var rootId = Guid.CreateVersion7().ToString("N");
        var root = CreateRun(id: rootId);

        var child = CreateRun() with { ParentRunId = rootId, RootRunId = rootId };

        var grandchild = CreateRun() with { ParentRunId = child.Id, RootRunId = rootId };

        var unrelated = CreateRun();

        await Store.CreateRunsAsync([root, child, grandchild, unrelated], cancellationToken: ct);

        var results = await Store.GetRunsAsync(new() { RootRunId = rootId }, cancellationToken: ct);

        Assert.Equal(2, results.TotalCount);
        Assert.Contains(results.Items, r => r.Id == child.Id);
        Assert.Contains(results.Items, r => r.Id == grandchild.Id);
    }

    [Fact]
    public async Task GetRuns_FilterByBatchId()
    {
        var ct = TestContext.Current.CancellationToken;
        var batchId = Guid.CreateVersion7().ToString("N");
        var batchRun = CreateRun() with { BatchId = batchId };

        var normal = CreateRun();

        await Store.CreateRunsAsync([batchRun, normal], cancellationToken: ct);

        var batchFiltered = await Store.GetRunsAsync(new() { BatchId = batchId }, cancellationToken: ct);

        Assert.Contains(batchFiltered.Items, r => r.Id == batchRun.Id);
        Assert.DoesNotContain(batchFiltered.Items, r => r.Id == normal.Id);
    }

    [Fact]
    public async Task GetRuns_FilterByIsTerminal()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"Terminal_{Guid.CreateVersion7():N}";

        var pending = CreateRun(jobName);
        var running = CreateRun(jobName, JobStatus.Running) with { StartedAt = DateTimeOffset.UtcNow };
        var completed = CreateRun(jobName, JobStatus.Succeeded) with { CompletedAt = DateTimeOffset.UtcNow };
        var deadLetter = CreateRun(jobName, JobStatus.Failed) with { CompletedAt = DateTimeOffset.UtcNow };

        await Store.CreateRunsAsync([pending, running, completed, deadLetter], cancellationToken: ct);

        var terminal = await Store.GetRunsAsync(new()
        {
            IsTerminal = true,
            JobName = jobName
        }, cancellationToken: ct);
        var nonTerminal = await Store.GetRunsAsync(new()
        {
            IsTerminal = false,
            JobName = jobName
        }, cancellationToken: ct);

        Assert.Equal(2, terminal.TotalCount);
        Assert.Contains(terminal.Items, r => r.Id == completed.Id);
        Assert.Contains(terminal.Items, r => r.Id == deadLetter.Id);

        Assert.Equal(2, nonTerminal.TotalCount);
        Assert.Contains(nonTerminal.Items, r => r.Id == pending.Id);
        Assert.Contains(nonTerminal.Items, r => r.Id == running.Id);
    }

    [Fact]
    public async Task GetRuns_Pagination_SkipTake()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"Paged_{Guid.CreateVersion7():N}";
        var runs = Enumerable.Range(0, 10)
            .Select(i => CreateRun(jobName) with { CreatedAt = DateTimeOffset.UtcNow.AddMinutes(i) })
            .ToList();

        await Store.CreateRunsAsync(runs, cancellationToken: ct);

        var page1 = await Store.GetRunsAsync(
            new() { JobName = jobName },
            0, 3, ct);
        var page2 = await Store.GetRunsAsync(
            new() { JobName = jobName },
            3, 3, ct);

        Assert.Equal(10, page1.TotalCount);
        Assert.Equal(3, page1.Items.Count);
        Assert.Equal(10, page2.TotalCount);
        Assert.Equal(3, page2.Items.Count);

        var page1Ids = page1.Items.Select(r => r.Id).ToHashSet();
        var page2Ids = page2.Items.Select(r => r.Id).ToHashSet();
        Assert.Empty(page1Ids.Intersect(page2Ids));
    }

    [Fact]
    public async Task GetRuns_Pagination_WithCombinedFilters_PreservesTotalCount()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"PagedFiltered_{Guid.CreateVersion7():N}";

        var pendingRuns = Enumerable.Range(0, 9)
            .Select(i =>
            {
                var createdAt = DateTimeOffset.UtcNow.AddMinutes(i);
                return CreateRun(jobName) with { CreatedAt = createdAt, NotBefore = createdAt };
            })
            .ToList();

        var nonMatching = Enumerable.Range(0, 3)
            .Select(_ => CreateRun(jobName, JobStatus.Running))
            .ToList();

        await Store.CreateRunsAsync([..pendingRuns, ..nonMatching], cancellationToken: ct);

        var filter = new RunFilter
        {
            JobName = jobName,
            Status = JobStatus.Pending
        };

        var page1 = await Store.GetRunsAsync(filter, 0, 4, ct);
        var page2 = await Store.GetRunsAsync(filter, 4, 4, ct);

        Assert.Equal(9, page1.TotalCount);
        Assert.Equal(9, page2.TotalCount);
        Assert.Equal(4, page1.Items.Count);
        Assert.Equal(4, page2.Items.Count);

        var page1Ids = page1.Items.Select(r => r.Id).ToHashSet();
        var page2Ids = page2.Items.Select(r => r.Id).ToHashSet();
        Assert.Empty(page1Ids.Intersect(page2Ids));
    }

    [Fact]
    public async Task GetRuns_PagingPastEnd_ReturnsEmptyItemsAndCorrectTotalCount()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"PagedPastEnd_{Guid.CreateVersion7():N}";
        var runs = Enumerable.Range(0, 10)
            .Select(_ => CreateRun(jobName))
            .ToList();
        await Store.CreateRunsAsync(runs, cancellationToken: ct);

        // Skipping past the end must still return the true TotalCount, not 0.
        var pastEnd = await Store.GetRunsAsync(
            new() { JobName = jobName },
            100, 10, ct);

        Assert.Equal(10, pastEnd.TotalCount);
        Assert.Empty(pastEnd.Items);
    }

    [Fact]
    public async Task GetRuns_NegativeSkip_ThrowsArgumentOutOfRange()
    {
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Store.GetRunsAsync(new(), -1, 10, ct));
    }

    [Fact]
    public async Task GetRuns_NonPositiveTake_ThrowsArgumentOutOfRange()
    {
        var ct = TestContext.Current.CancellationToken;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Store.GetRunsAsync(new(), 0, 0, ct));
    }

    [Fact]
    public async Task GetRuns_OrderBy_CreatedAt()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"Ordered_{Guid.CreateVersion7():N}";

        var early = CreateRun(jobName) with { CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-10) };
        var middle = CreateRun(jobName) with { CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-5) };
        var late = CreateRun(jobName) with { CreatedAt = DateTimeOffset.UtcNow };

        await Store.CreateRunsAsync([early, middle, late], cancellationToken: ct);

        var results = await Store.GetRunsAsync(new()
        {
            JobName = jobName,
            OrderBy = RunOrderBy.CreatedAt
        }, cancellationToken: ct);

        Assert.Equal(3, results.Items.Count);
        Assert.Equal(late.Id, results.Items[0].Id);
        Assert.Equal(middle.Id, results.Items[1].Id);
        Assert.Equal(early.Id, results.Items[2].Id);
    }

    [Fact]
    public async Task GetRuns_FilterByLastHeartbeatBefore()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"Heartbeat_{Guid.CreateVersion7():N}";
        var cutoff = DateTimeOffset.UtcNow;

        var stale = CreateRun(jobName) with { LastHeartbeatAt = cutoff.AddMinutes(-10) };

        var fresh = CreateRun(jobName) with { LastHeartbeatAt = cutoff.AddMinutes(10) };

        await Store.CreateRunsAsync([stale, fresh], cancellationToken: ct);

        var results = await Store.GetRunsAsync(new()
        {
            LastHeartbeatBefore = cutoff,
            JobName = jobName
        }, cancellationToken: ct);

        Assert.Single(results.Items);
        Assert.Equal(stale.Id, results.Items[0].Id);
    }

    [Fact]
    public async Task UpdateRun_PersistsFields()
    {
        var ct = TestContext.Current.CancellationToken;
        var run = CreateRun() with { NodeName = "node-1" };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var updated = CreateRun(id: run.Id) with
        {
            JobName = run.JobName,
            NodeName = "node-1",
            Progress = 0.75,
            TraceId = "trace-abc",
            SpanId = "span-xyz",
            Result = """{"output":42}""",
            Reason = "partial failure",
            LastHeartbeatAt = DateTimeOffset.UtcNow
        };

        await Store.UpdateRunAsync(updated, ct);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
        Assert.Equal(0.75, loaded.Progress);
        Assert.Equal("trace-abc", loaded.TraceId);
        Assert.Equal("span-xyz", loaded.SpanId);
        Assert.Equal("""{"output":42}""", loaded.Result);
        Assert.Equal("partial failure", loaded.Reason);
        Assert.NotNull(loaded.LastHeartbeatAt);
    }

    [Fact]
    public async Task UpdateRun_SkipsTerminalRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        var run = CreateRun(status: JobStatus.Succeeded) with
        {
            CompletedAt = DateTimeOffset.UtcNow, Progress = 1.0, NodeName = "node-1"
        };
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var updated = CreateRun(id: run.Id) with
        {
            JobName = run.JobName,
            NodeName = "node-1",
            Progress = 0.5,
            Reason = "should not persist"
        };

        await Store.UpdateRunAsync(updated, ct);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
        Assert.Equal(1.0, loaded.Progress);
        Assert.Null(loaded.Reason);
    }

    [Fact]
    public async Task GetRuns_FilterByCreatedRange()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"CreatedRange_{Guid.CreateVersion7():N}";

        var early = CreateRun(jobName) with { CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-30) };

        var middle = CreateRun(jobName) with { CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-15) };

        var late = CreateRun(jobName) with { CreatedAt = DateTimeOffset.UtcNow };

        await Store.CreateRunsAsync([early, middle, late], cancellationToken: ct);

        var results = await Store.GetRunsAsync(new()
        {
            JobName = jobName,
            CreatedAfter = DateTimeOffset.UtcNow.AddMinutes(-20),
            CreatedBefore = DateTimeOffset.UtcNow.AddMinutes(-10)
        }, cancellationToken: ct);

        Assert.Single(results.Items);
        Assert.Equal(middle.Id, results.Items[0].Id);
    }

    [Fact]
    public async Task GetRuns_FilterByStatusAndCreatedRange()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"StatusCreatedRange_{Guid.CreateVersion7():N}";

        var earlyCreatedAt = DateTimeOffset.UtcNow.AddMinutes(-30);
        var early = CreateRun(jobName, JobStatus.Succeeded) with
        {
            CreatedAt = earlyCreatedAt, CompletedAt = earlyCreatedAt
        };

        var middleCreatedAt = DateTimeOffset.UtcNow.AddMinutes(-15);
        var middle = CreateRun(jobName, JobStatus.Succeeded) with
        {
            CreatedAt = middleCreatedAt, CompletedAt = middleCreatedAt
        };

        var lateCreatedAt = DateTimeOffset.UtcNow;
        var late = CreateRun(jobName, JobStatus.Succeeded) with
        {
            CreatedAt = lateCreatedAt, CompletedAt = lateCreatedAt
        };

        await Store.CreateRunsAsync([early, middle, late], cancellationToken: ct);

        var results = await Store.GetRunsAsync(new()
        {
            Status = JobStatus.Succeeded,
            CreatedAfter = DateTimeOffset.UtcNow.AddMinutes(-20),
            CreatedBefore = DateTimeOffset.UtcNow.AddMinutes(-10)
        }, cancellationToken: ct);

        Assert.Single(results.Items);
        Assert.Equal(middle.Id, results.Items[0].Id);
    }

    [Fact]
    public async Task GetRuns_OrderBy_StartedAt()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"OrderStarted_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var baseTime = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run1 = CreateRun(jobName, JobStatus.Running) with { StartedAt = baseTime.AddMinutes(-3) };
        var run2 = CreateRun(jobName, JobStatus.Running) with { StartedAt = baseTime.AddMinutes(-2) };
        var run3 = CreateRun(jobName, JobStatus.Running) with { StartedAt = baseTime.AddMinutes(-1) };

        await Store.CreateRunsAsync([run1, run2, run3], cancellationToken: ct);

        var results = await Store.GetRunsAsync(new()
        {
            JobName = jobName,
            OrderBy = RunOrderBy.StartedAt
        }, cancellationToken: ct);

        Assert.Equal(3, results.Items.Count);
        Assert.Equal(run3.Id, results.Items[0].Id);
        Assert.Equal(run2.Id, results.Items[1].Id);
        Assert.Equal(run1.Id, results.Items[2].Id);
    }

    [Fact]
    public async Task GetRuns_OrderBy_CompletedAt()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"OrderCompleted_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var baseTime = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run1 = CreateRun(jobName, JobStatus.Succeeded) with { CompletedAt = baseTime.AddMinutes(-3) };
        var run2 = CreateRun(jobName, JobStatus.Succeeded) with { CompletedAt = baseTime.AddMinutes(-2) };
        var run3 = CreateRun(jobName, JobStatus.Succeeded) with { CompletedAt = baseTime.AddMinutes(-1) };

        await Store.CreateRunsAsync([run1, run2, run3], cancellationToken: ct);

        var results = await Store.GetRunsAsync(new()
        {
            JobName = jobName,
            OrderBy = RunOrderBy.CompletedAt
        }, cancellationToken: ct);

        Assert.Equal(3, results.Items.Count);
        Assert.Equal(run3.Id, results.Items[0].Id);
        Assert.Equal(run2.Id, results.Items[1].Id);
        Assert.Equal(run1.Id, results.Items[2].Id);
    }

    [Fact]
    public async Task GetRuns_FilteredResort_LargeSkipTake_ReturnsExactPageAndTotal()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"FilteredResortLarge_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var baseTime = TruncateToMilliseconds(DateTimeOffset.UtcNow.AddHours(-1));
        var completedRuns = new List<JobRun>(300);

        for (var i = 0; i < 300; i++)
        {
            var timestamp = baseTime.AddSeconds(i);
            var run = CreateRun(jobName, JobStatus.Succeeded) with
            {
                CreatedAt = timestamp,
                NotBefore = timestamp,
                StartedAt = timestamp,
                CompletedAt = timestamp,
                NodeName = "node-1",
                Attempt = 1,
                Progress = 1
            };
            completedRuns.Add(run);
        }

        await Store.CreateRunsAsync(completedRuns, cancellationToken: ct);

        const int skip = 150;
        const int take = 30;

        var page = await Store.GetRunsAsync(new()
        {
            JobName = jobName,
            Status = JobStatus.Succeeded,
            OrderBy = RunOrderBy.CompletedAt
        }, skip, take, ct);

        var expectedIds = completedRuns
            .OrderByDescending(r => r.CompletedAt)
            .ThenByDescending(r => r.Id)
            .Skip(skip)
            .Take(take)
            .Select(r => r.Id)
            .ToArray();

        Assert.Equal(300, page.TotalCount);
        Assert.Equal(take, page.Items.Count);
        Assert.Equal(expectedIds, page.Items.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task UpdateRun_SkipsWrongNode()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"WrongNode_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);

        claimed = claimed with { NodeName = "wrong-node", Progress = 0.5 };
        await Store.UpdateRunAsync(claimed, ct);

        var after = await Store.GetRunAsync(run.Id, ct);
        Assert.Equal(0.0, after!.Progress);
    }

    [Fact]
    public async Task GetRuns_FilterByNodeName()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"NodeFilter_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run1 = CreateRun(jobName, JobStatus.Running) with { NodeName = "node-a" };
        var run2 = CreateRun(jobName, JobStatus.Running) with { NodeName = "node-b" };

        await Store.CreateRunsAsync([run1, run2], cancellationToken: ct);

        var results = await Store.GetRunsAsync(new() { NodeName = "node-a" }, cancellationToken: ct);

        Assert.Single(results.Items);
        Assert.Equal(run1.Id, results.Items[0].Id);
    }

    [Fact]
    public async Task GetRuns_FilterByCompletedAfter()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"CompletedAfter_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var baseTime = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run1 = CreateRun(jobName, JobStatus.Succeeded) with { CompletedAt = baseTime.AddMinutes(-10) };
        var run2 = CreateRun(jobName, JobStatus.Succeeded) with { CompletedAt = baseTime.AddMinutes(-5) };
        var run3 = CreateRun(jobName, JobStatus.Succeeded) with { CompletedAt = baseTime.AddMinutes(-1) };

        await Store.CreateRunsAsync([run1, run2, run3], cancellationToken: ct);

        var results = await Store.GetRunsAsync(new()
        {
            JobName = jobName,
            CompletedAfter = baseTime.AddMinutes(-5)
        }, cancellationToken: ct);

        Assert.Single(results.Items);
        Assert.Equal(run3.Id, results.Items[0].Id);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_And_MaxActive_Combined()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DedupMaxActive_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.MaxConcurrency = 5;
        await Store.UpsertJobsAsync([job], ct);

        var run1 = CreateRun(jobName) with { DeduplicationId = "unique-1" };
        var created1 = await Store.TryCreateRunAsync(run1, 5, cancellationToken: ct);
        Assert.True(created1);

        var run2 = CreateRun(jobName) with { DeduplicationId = "unique-1" };
        var created2 = await Store.TryCreateRunAsync(run2, 5, cancellationToken: ct);
        Assert.False(created2);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_Concurrent_ExactlyOneWins()
    {
        var ct = TestContext.Current.CancellationToken;
        await Store.UpsertJobsAsync([CreateJob("TestJob")], ct);

        for (var trial = 0; trial < 20; trial++)
        {
            var dedupId = Guid.NewGuid().ToString("N");
            var results = await Task.WhenAll(
                Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
                {
                    await Task.Delay(1);
                    var run = CreateRun("TestJob") with { DeduplicationId = dedupId };
                    return await Store.TryCreateRunAsync(run);
                })));

            Assert.Equal(1, results.Count(r => r));
        }
    }

    [Fact]
    public async Task TryCreateRun_MaxActive_Concurrent_RespectsLimit()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob("TestJob");
        job.MaxConcurrency = 1;
        await Store.UpsertJobsAsync([job], ct);

        for (var trial = 0; trial < 20; trial++)
        {
            var results = await Task.WhenAll(
                Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
                {
                    await Task.Delay(1);
                    return await Store.TryCreateRunAsync(CreateRun("TestJob"), 1);
                })));

            Assert.Equal(1, results.Count(r => r));

            // Cleanup: complete the created run so next trial starts fresh
            var runs = await Store.GetRunsAsync(new() { JobName = "TestJob", IsTerminal = false },
                take: 10, cancellationToken: ct);
            foreach (var run in runs.Items)
            {
                await Store.TryCancelRunAsync(run.Id, cancellationToken: ct);
            }
        }
    }

    [Fact]
    public async Task UpdateRun_NonExistent_IsNoOp()
    {
        var ct = TestContext.Current.CancellationToken;
        var run = CreateRun("TestJob") with { Progress = 0.5 };
        await Store.UpdateRunAsync(run, ct);
    }

    [Fact]
    public async Task CreateRuns_DuplicateId_Throws()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"AtomicJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run1 = CreateRun(jobName);
        await Store.CreateRunsAsync([run1], cancellationToken: ct);

        var run2 = CreateRun(jobName);
        var duplicate = CreateRun(jobName, id: run1.Id);

        await Assert.ThrowsAnyAsync<Exception>(() => Store.CreateRunsAsync([run2, duplicate], cancellationToken: ct));

        var loaded = await Store.GetRunAsync(run2.Id, ct);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task TryCreateRun_DuplicateId_ReturnsFalse_NotException()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TryCreateDup_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run1 = CreateRun(jobName);
        Assert.True(await Store.TryCreateRunAsync(run1, cancellationToken: ct));

        var duplicate = CreateRun(jobName, id: run1.Id);
        var created = await Store.TryCreateRunAsync(duplicate, cancellationToken: ct);

        Assert.False(created);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_AllowedAfterPurge()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DedupPurge_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var createdAt1 = DateTimeOffset.UtcNow.AddDays(-30);
        var run1 = CreateRun(jobName) with
        {
            DeduplicationId = dedupId, CreatedAt = createdAt1, NotBefore = createdAt1
        };
        Assert.True(await Store.TryCreateRunAsync(run1, cancellationToken: ct));

        var claimed = (await Store.ClaimRunsAsync("node1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);
        claimed = claimed with { Status = JobStatus.Succeeded, CompletedAt = DateTimeOffset.UtcNow.AddDays(-30) };
        await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running), ct);

        await Store.PurgeAsync(DateTimeOffset.UtcNow.AddDays(-7), ct);

        Assert.Null(await Store.GetRunAsync(run1.Id, ct));

        var run2 = CreateRun(jobName) with { DeduplicationId = dedupId };
        Assert.True(await Store.TryCreateRunAsync(run2, cancellationToken: ct));
    }

    [Fact]
    public async Task GetRuns_MultipleFilters_IntersectsCorrectly()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"MultiFilter_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        var oldPendingCreatedAt = now.AddHours(-2);
        var oldPending = CreateRun(jobName) with { CreatedAt = oldPendingCreatedAt, NotBefore = oldPendingCreatedAt };

        var recentPendingCreatedAt = now.AddMinutes(-5);
        var recentPending = CreateRun(jobName) with
        {
            CreatedAt = recentPendingCreatedAt, NotBefore = recentPendingCreatedAt
        };

        await Store.CreateRunsAsync([oldPending, recentPending], cancellationToken: ct);

        var claimed = (await Store.ClaimRunsAsync("node1", [jobName], ["default"], 1, ct)).FirstOrDefault();
        Assert.NotNull(claimed);
        claimed = claimed with { Status = JobStatus.Succeeded, CompletedAt = now };
        await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running), ct);

        var result = await Store.GetRunsAsync(new()
        {
            Status = JobStatus.Pending,
            JobName = jobName,
            CreatedAfter = now.AddHours(-1)
        }, take: 10, cancellationToken: ct);

        Assert.Equal(1, result.TotalCount);
        Assert.Equal(recentPending.Id, result.Items[0].Id);
    }

    [Fact]
    public async Task GetRun_NonBatch_BatchIdIsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        var run = CreateRun();
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
        Assert.Null(loaded.BatchId);
    }

    [Fact]
    public async Task GetRuns_IsTerminal_Pagination()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TermPage_{Guid.CreateVersion7():N}";

        var runs = new List<JobRun>();
        for (var i = 0; i < 5; i++)
        {
            var r = CreateRun(jobName, JobStatus.Succeeded) with { CompletedAt = DateTimeOffset.UtcNow };
            runs.Add(r);
        }

        var pending = CreateRun(jobName);
        runs.Add(pending);
        await Store.CreateRunsAsync(runs, cancellationToken: ct);

        var page = await Store.GetRunsAsync(new()
        {
            IsTerminal = true,
            JobName = jobName
        }, 0, 3, ct);

        Assert.Equal(5, page.TotalCount);
        Assert.Equal(3, page.Items.Count);
        Assert.All(page.Items, r => Assert.True(r.Status.IsTerminal));
    }

    [Fact]
    public async Task UpdateRun_NullNodeName_Succeeds()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"NullNode_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        // Update progress on a run that has no node assigned
        run = run with { Progress = 0.5 };
        await Store.UpdateRunAsync(run, ct);

        var loaded = await Store.GetRunAsync(run.Id, ct);
        Assert.NotNull(loaded);
        Assert.Null(loaded.NodeName);
        Assert.Equal(0.5, loaded.Progress);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_FreedOnCancel()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DedupCancel_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run1 = CreateRun(jobName) with { DeduplicationId = dedupId };
        Assert.True(await Store.TryCreateRunAsync(run1, cancellationToken: ct));

        Assert.True((await Store.TryCancelRunAsync(run1.Id, cancellationToken: ct)).Transitioned);

        var run2 = CreateRun(jobName) with { DeduplicationId = dedupId };
        Assert.True(await Store.TryCreateRunAsync(run2, cancellationToken: ct));
    }

    [Fact]
    public async Task TryCreateRun_Dedup_BlocksWhileActive()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"DedupActive_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run1 = CreateRun(jobName) with { DeduplicationId = dedupId };
        Assert.True(await Store.TryCreateRunAsync(run1, cancellationToken: ct));

        var run2 = CreateRun(jobName) with { DeduplicationId = dedupId };
        Assert.False(await Store.TryCreateRunAsync(run2, cancellationToken: ct));
    }

    [Fact]
    public async Task TryCreateRun_LastCronFireAt_UpdatedAtomically()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"CronAtomic_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var fireAt = DateTimeOffset.UtcNow;
        var run = CreateRun(jobName);
        Assert.True(await Store.TryCreateRunAsync(run, lastCronFireAt: fireAt, cancellationToken: ct));

        var job = await Store.GetJobAsync(jobName, ct);
        Assert.NotNull(job);
        Assert.NotNull(job.LastCronFireAt);
        // Truncate both to milliseconds for cross-store comparison
        Assert.Equal(
            fireAt.ToUnixTimeMilliseconds(),
            job.LastCronFireAt.Value.ToUnixTimeMilliseconds());
    }

    [Fact]
    public async Task TryCreateRun_LastCronFireAt_NotUpdatedOnRejection()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"CronReject_{Guid.CreateVersion7():N}";
        await Store.UpsertJobsAsync([CreateJob(jobName)], ct);

        var run1 = CreateRun(jobName) with { DeduplicationId = "same" };
        Assert.True(await Store.TryCreateRunAsync(run1, lastCronFireAt: DateTimeOffset.UtcNow.AddMinutes(-5),
            cancellationToken: ct));

        var run2 = CreateRun(jobName) with { DeduplicationId = "same" };
        var laterFireAt = DateTimeOffset.UtcNow;
        Assert.False(await Store.TryCreateRunAsync(run2, lastCronFireAt: laterFireAt, cancellationToken: ct));

        var job = await Store.GetJobAsync(jobName, ct);
        Assert.NotNull(job);
        // LastCronFireAt should still be the original value, not the rejected one
        Assert.NotEqual(
            laterFireAt.ToUnixTimeMilliseconds(),
            job!.LastCronFireAt!.Value.ToUnixTimeMilliseconds());
    }
}
