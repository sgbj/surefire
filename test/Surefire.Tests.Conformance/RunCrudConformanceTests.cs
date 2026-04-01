namespace Surefire.Tests.Conformance;

public abstract class RunCrudConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task CreateRuns_Persists()
    {
        var run = CreateRun();
        run.Arguments = """{"key":"value"}""";
        run.Priority = 5;
        run.DeduplicationId = "dedup-1";

        await Store.CreateRunsAsync([run]);

        var loaded = await Store.GetRunAsync(run.Id);
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
        var runs = Enumerable.Range(0, 5)
            .Select(_ => CreateRun())
            .ToList();

        await Store.CreateRunsAsync(runs);

        foreach (var run in runs)
        {
            var loaded = await Store.GetRunAsync(run.Id);
            Assert.NotNull(loaded);
            Assert.Equal(run.Id, loaded.Id);
        }
    }

    [Fact]
    public async Task CreateRuns_WithInitialEvents_PersistsAtomically()
    {
        var run = CreateRun();
        var evt = new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.InputDeclared,
            Payload = "{\"arguments\":[\"input\"]}",
            CreatedAt = run.CreatedAt,
            Attempt = 0
        };

        await Store.CreateRunsAsync([run], [evt]);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);

        var events = await Store.GetEventsAsync(run.Id);
        Assert.Single(events);
        Assert.Equal(RunEventType.InputDeclared, events[0].EventType);
        Assert.Equal(evt.Payload, events[0].Payload);
    }

    [Fact]
    public async Task CreateRuns_BatchWithInitialEvents_PersistsForAll()
    {
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

        await Store.CreateRunsAsync([runA, runB], events);

        var eventsA = await Store.GetEventsAsync(runA.Id);
        var eventsB = await Store.GetEventsAsync(runB.Id);

        Assert.Single(eventsA);
        Assert.Single(eventsB);
        Assert.Equal("{\"arguments\":[\"a\"]}", eventsA[0].Payload);
        Assert.Equal("{\"arguments\":[\"b\"]}", eventsB[0].Payload);
    }

    [Fact]
    public async Task CreateRun_RerunOfRunId_RoundTrips()
    {
        var jobName = $"Rerun_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var originalRun = CreateRun(jobName);
        var rerun = CreateRun(jobName);
        rerun.RerunOfRunId = originalRun.Id;

        await Store.CreateRunsAsync([originalRun, rerun]);

        var stored = await Store.GetRunAsync(rerun.Id);
        Assert.NotNull(stored);
        Assert.Equal(originalRun.Id, stored.RerunOfRunId);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_RejectsDuplicate()
    {
        var jobName = $"Dedup_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");

        var run1 = CreateRun(jobName);
        run1.DeduplicationId = dedupId;
        var result1 = await Store.TryCreateRunAsync(run1);
        Assert.True(result1);

        var run2 = CreateRun(jobName);
        run2.DeduplicationId = dedupId;
        var result2 = await Store.TryCreateRunAsync(run2);
        Assert.False(result2);
    }

    [Fact]
    public async Task TryCreateRun_WithInitialEvents_PersistsOnSuccess()
    {
        var run = CreateRun($"InitialEvents_{Guid.CreateVersion7():N}");
        var evt = new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.InputDeclared,
            Payload = "{\"arguments\":[\"values\"]}",
            CreatedAt = run.CreatedAt,
            Attempt = 0
        };

        var created = await Store.TryCreateRunAsync(run, initialEvents: [evt]);
        Assert.True(created);

        var events = await Store.GetEventsAsync(run.Id);
        Assert.Single(events);
        Assert.Equal(RunEventType.InputDeclared, events[0].EventType);
    }

    [Fact]
    public async Task TryCreateRun_Rejected_DoesNotPersistInitialEvents()
    {
        var jobName = $"InitialEventsReject_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");

        var run1 = CreateRun(jobName);
        run1.DeduplicationId = dedupId;
        var ok = await Store.TryCreateRunAsync(run1);
        Assert.True(ok);

        var run2 = CreateRun(jobName);
        run2.DeduplicationId = dedupId;
        var evt = new RunEvent
        {
            RunId = run2.Id,
            EventType = RunEventType.InputDeclared,
            Payload = "{\"arguments\":[\"values\"]}",
            CreatedAt = run2.CreatedAt,
            Attempt = 0
        };

        var created = await Store.TryCreateRunAsync(run2, initialEvents: [evt]);
        Assert.False(created);

        var loaded = await Store.GetRunAsync(run2.Id);
        Assert.Null(loaded);

        var events = await Store.GetEventsAsync(run2.Id);
        Assert.Empty(events);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_DifferentJobs_BothSucceed()
    {
        var dedupId = Guid.CreateVersion7().ToString("N");

        var run1 = CreateRun($"JobA_{Guid.CreateVersion7():N}");
        run1.DeduplicationId = dedupId;
        var result1 = await Store.TryCreateRunAsync(run1);
        Assert.True(result1);

        var run2 = CreateRun($"JobB_{Guid.CreateVersion7():N}");
        run2.DeduplicationId = dedupId;
        var result2 = await Store.TryCreateRunAsync(run2);
        Assert.True(result2);
    }

    [Fact]
    public async Task TryCreateRun_NoDedup_AlwaysSucceeds()
    {
        var jobName = $"NoDedup_{Guid.CreateVersion7():N}";

        var run1 = CreateRun(jobName);
        run1.DeduplicationId = null;
        var result1 = await Store.TryCreateRunAsync(run1);
        Assert.True(result1);

        var run2 = CreateRun(jobName);
        run2.DeduplicationId = null;
        var result2 = await Store.TryCreateRunAsync(run2);
        Assert.True(result2);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_FreedOnTerminal()
    {
        var jobName = $"DedupTerminal_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run1 = CreateRun(jobName);
        run1.DeduplicationId = dedupId;
        var result1 = await Store.TryCreateRunAsync(run1);
        Assert.True(result1);

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"]);
        Assert.NotNull(claimed);

        claimed.Status = JobStatus.Completed;
        claimed.CompletedAt = DateTimeOffset.UtcNow;
        await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));

        var run2 = CreateRun(jobName);
        run2.DeduplicationId = dedupId;
        var result2 = await Store.TryCreateRunAsync(run2);
        Assert.True(result2);
    }

    [Fact]
    public async Task TryCreateRun_MaxActive_RespectsIsEnabled()
    {
        var jobName = $"DisabledJob_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        await Store.UpsertJobAsync(job);
        await Store.SetJobEnabledAsync(jobName, false);

        var run = CreateRun(jobName);
        var result = await Store.TryCreateRunAsync(run, 1);
        Assert.False(result);
    }

    [Fact]
    public async Task TryCreateRun_MaxActive_EnabledJob_Succeeds()
    {
        var jobName = $"EnabledJob_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        await Store.UpsertJobAsync(job);

        var run = CreateRun(jobName);
        var result = await Store.TryCreateRunAsync(run, 1);
        Assert.True(result);
    }

    [Fact]
    public async Task TryCreateRun_MaxActive_ExceedsLimit()
    {
        var jobName = $"MaxActive_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        await Store.UpsertJobAsync(job);

        var run1 = CreateRun(jobName);
        var result1 = await Store.TryCreateRunAsync(run1, 1);
        Assert.True(result1);

        var run2 = CreateRun(jobName);
        var result2 = await Store.TryCreateRunAsync(run2, 1);
        Assert.False(result2);
    }

    [Fact]
    public async Task TryCreateRun_NoMaxActive_IgnoresIsEnabled()
    {
        var jobName = $"ManualTrigger_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        await Store.UpsertJobAsync(job);
        await Store.SetJobEnabledAsync(jobName, false);

        var run = CreateRun(jobName);
        var result = await Store.TryCreateRunAsync(run);
        Assert.True(result);
    }

    [Fact]
    public async Task GetRun_ReturnsIsolatedCopy()
    {
        var run = CreateRun();
        run.Arguments = """{"key":"value"}""";
        await Store.CreateRunsAsync([run]);

        var copy1 = await Store.GetRunAsync(run.Id);
        Assert.NotNull(copy1);
        copy1.Arguments = """{"mutated":true}""";
        copy1.Progress = 0.99;

        var copy2 = await Store.GetRunAsync(run.Id);
        Assert.NotNull(copy2);
        Assert.Equal("""{"key":"value"}""", copy2.Arguments);
        Assert.Equal(0.0, copy2.Progress);
    }

    [Fact]
    public async Task GetRun_ReturnsNull_WhenNotFound()
    {
        var result = await Store.GetRunAsync(Guid.CreateVersion7().ToString("N"));
        Assert.Null(result);
    }

    [Fact]
    public async Task GetRuns_FilterByStatus()
    {
        var jobName = $"StatusFilter_{Guid.CreateVersion7():N}";

        var pending = CreateRun(jobName);
        var completed = CreateRun(jobName, JobStatus.Completed);
        completed.CompletedAt = DateTimeOffset.UtcNow;
        var cancelled = CreateRun(jobName, JobStatus.Cancelled);
        cancelled.CompletedAt = DateTimeOffset.UtcNow;
        cancelled.CancelledAt = DateTimeOffset.UtcNow;

        await Store.CreateRunsAsync([pending, completed, cancelled]);

        var pendingResults = await Store.GetRunsAsync(new()
        {
            Status = JobStatus.Pending,
            JobName = jobName,
            ExactJobName = true
        });

        Assert.Single(pendingResults.Items);
        Assert.Equal(pending.Id, pendingResults.Items[0].Id);
    }

    [Fact]
    public async Task GetRuns_FilterByJobName_SubstringMatch()
    {
        var suffix = Guid.CreateVersion7().ToString("N");
        var run1 = CreateRun($"EmailSender_{suffix}");
        var run2 = CreateRun($"PreEmailTask_{suffix}");
        var run3 = CreateRun($"InvoiceGen_{suffix}");

        await Store.CreateRunsAsync([run1, run2, run3]);

        var results = await Store.GetRunsAsync(new() { JobName = "Email" });

        Assert.Contains(results.Items, r => r.Id == run1.Id);
        Assert.Contains(results.Items, r => r.Id == run2.Id);
        Assert.DoesNotContain(results.Items, r => r.Id == run3.Id);
    }

    [Fact]
    public async Task GetRuns_FilterByParentRunId()
    {
        var parentId = Guid.CreateVersion7().ToString("N");
        var parent = CreateRun(id: parentId);

        var child1 = CreateRun();
        child1.ParentRunId = parentId;
        child1.RootRunId = parentId;

        var child2 = CreateRun();
        child2.ParentRunId = parentId;
        child2.RootRunId = parentId;

        var unrelated = CreateRun();

        await Store.CreateRunsAsync([parent, child1, child2, unrelated]);

        var results = await Store.GetRunsAsync(new() { ParentRunId = parentId });

        Assert.Equal(2, results.TotalCount);
        Assert.Contains(results.Items, r => r.Id == child1.Id);
        Assert.Contains(results.Items, r => r.Id == child2.Id);
    }

    [Fact]
    public async Task GetRuns_FilterByRootRunId()
    {
        var rootId = Guid.CreateVersion7().ToString("N");
        var root = CreateRun(id: rootId);

        var child = CreateRun();
        child.ParentRunId = rootId;
        child.RootRunId = rootId;

        var grandchild = CreateRun();
        grandchild.ParentRunId = child.Id;
        grandchild.RootRunId = rootId;

        var unrelated = CreateRun();

        await Store.CreateRunsAsync([root, child, grandchild, unrelated]);

        var results = await Store.GetRunsAsync(new() { RootRunId = rootId });

        Assert.Equal(2, results.TotalCount);
        Assert.Contains(results.Items, r => r.Id == child.Id);
        Assert.Contains(results.Items, r => r.Id == grandchild.Id);
    }

    [Fact]
    public async Task GetRuns_FilterByIsBatchCoordinator()
    {
        var coordinator = CreateRun();
        coordinator.BatchTotal = 10;
        coordinator.BatchCompleted = 0;
        coordinator.BatchFailed = 0;

        var normal = CreateRun();

        await Store.CreateRunsAsync([coordinator, normal]);

        var coordinators = await Store.GetRunsAsync(new() { IsBatchCoordinator = true });
        var nonCoordinators = await Store.GetRunsAsync(new() { IsBatchCoordinator = false });

        Assert.Contains(coordinators.Items, r => r.Id == coordinator.Id);
        Assert.DoesNotContain(coordinators.Items, r => r.Id == normal.Id);

        Assert.Contains(nonCoordinators.Items, r => r.Id == normal.Id);
        Assert.DoesNotContain(nonCoordinators.Items, r => r.Id == coordinator.Id);
    }

    [Fact]
    public async Task GetRuns_FilterByIsTerminal()
    {
        var jobName = $"Terminal_{Guid.CreateVersion7():N}";

        var pending = CreateRun(jobName);
        var running = CreateRun(jobName, JobStatus.Running);
        running.StartedAt = DateTimeOffset.UtcNow;
        var completed = CreateRun(jobName, JobStatus.Completed);
        completed.CompletedAt = DateTimeOffset.UtcNow;
        var deadLetter = CreateRun(jobName, JobStatus.DeadLetter);
        deadLetter.CompletedAt = DateTimeOffset.UtcNow;

        await Store.CreateRunsAsync([pending, running, completed, deadLetter]);

        var terminal = await Store.GetRunsAsync(new()
        {
            IsTerminal = true,
            JobName = jobName,
            ExactJobName = true
        });
        var nonTerminal = await Store.GetRunsAsync(new()
        {
            IsTerminal = false,
            JobName = jobName,
            ExactJobName = true
        });

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
        var jobName = $"Paged_{Guid.CreateVersion7():N}";
        var runs = Enumerable.Range(0, 10)
            .Select(i =>
            {
                var run = CreateRun(jobName);
                run.CreatedAt = DateTimeOffset.UtcNow.AddMinutes(i);
                return run;
            })
            .ToList();

        await Store.CreateRunsAsync(runs);

        var page1 = await Store.GetRunsAsync(
            new() { JobName = jobName, ExactJobName = true },
            0, 3);
        var page2 = await Store.GetRunsAsync(
            new() { JobName = jobName, ExactJobName = true },
            3, 3);

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
        var jobName = $"PagedFiltered_{Guid.CreateVersion7():N}";

        var pendingRuns = Enumerable.Range(0, 9)
            .Select(i =>
            {
                var run = CreateRun(jobName, JobStatus.Pending);
                run.CreatedAt = DateTimeOffset.UtcNow.AddMinutes(i);
                run.NotBefore = run.CreatedAt;
                return run;
            })
            .ToList();

        var nonMatching = Enumerable.Range(0, 3)
            .Select(_ => CreateRun(jobName, JobStatus.Running))
            .ToList();

        await Store.CreateRunsAsync([..pendingRuns, ..nonMatching]);

        var filter = new RunFilter
        {
            JobName = jobName,
            ExactJobName = true,
            Status = JobStatus.Pending
        };

        var page1 = await Store.GetRunsAsync(filter, 0, 4);
        var page2 = await Store.GetRunsAsync(filter, 4, 4);

        Assert.Equal(9, page1.TotalCount);
        Assert.Equal(9, page2.TotalCount);
        Assert.Equal(4, page1.Items.Count);
        Assert.Equal(4, page2.Items.Count);

        var page1Ids = page1.Items.Select(r => r.Id).ToHashSet();
        var page2Ids = page2.Items.Select(r => r.Id).ToHashSet();
        Assert.Empty(page1Ids.Intersect(page2Ids));
    }

    [Fact]
    public async Task GetRuns_NegativeSkip_ThrowsArgumentOutOfRange()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Store.GetRunsAsync(new(), -1, 10));
    }

    [Fact]
    public async Task GetRuns_NonPositiveTake_ThrowsArgumentOutOfRange()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            Store.GetRunsAsync(new(), 0, 0));
    }

    [Fact]
    public async Task GetRuns_OrderBy_CreatedAt()
    {
        var jobName = $"Ordered_{Guid.CreateVersion7():N}";

        var early = CreateRun(jobName);
        early.CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-10);

        var middle = CreateRun(jobName);
        middle.CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-5);

        var late = CreateRun(jobName);
        late.CreatedAt = DateTimeOffset.UtcNow;

        await Store.CreateRunsAsync([early, middle, late]);

        var results = await Store.GetRunsAsync(new()
        {
            JobName = jobName,
            ExactJobName = true,
            OrderBy = RunOrderBy.CreatedAt
        });

        Assert.Equal(3, results.Items.Count);
        Assert.Equal(late.Id, results.Items[0].Id);
        Assert.Equal(middle.Id, results.Items[1].Id);
        Assert.Equal(early.Id, results.Items[2].Id);
    }

    [Fact]
    public async Task GetRuns_FilterByLastHeartbeatBefore()
    {
        var jobName = $"Heartbeat_{Guid.CreateVersion7():N}";
        var cutoff = DateTimeOffset.UtcNow;

        var stale = CreateRun(jobName);
        stale.LastHeartbeatAt = cutoff.AddMinutes(-10);

        var fresh = CreateRun(jobName);
        fresh.LastHeartbeatAt = cutoff.AddMinutes(10);

        await Store.CreateRunsAsync([stale, fresh]);

        var results = await Store.GetRunsAsync(new()
        {
            LastHeartbeatBefore = cutoff,
            JobName = jobName,
            ExactJobName = true
        });

        Assert.Single(results.Items);
        Assert.Equal(stale.Id, results.Items[0].Id);
    }

    [Fact]
    public async Task UpdateRun_PersistsFields()
    {
        var run = CreateRun();
        run.NodeName = "node-1";
        await Store.CreateRunsAsync([run]);

        var updated = CreateRun(id: run.Id);
        updated.JobName = run.JobName;
        updated.NodeName = "node-1";
        updated.Progress = 0.75;
        updated.TraceId = "trace-abc";
        updated.SpanId = "span-xyz";
        updated.Result = """{"output":42}""";
        updated.Error = "partial failure";
        updated.LastHeartbeatAt = DateTimeOffset.UtcNow;

        await Store.UpdateRunAsync(updated);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Equal(0.75, loaded.Progress);
        Assert.Equal("trace-abc", loaded.TraceId);
        Assert.Equal("span-xyz", loaded.SpanId);
        Assert.Equal("""{"output":42}""", loaded.Result);
        Assert.Equal("partial failure", loaded.Error);
        Assert.NotNull(loaded.LastHeartbeatAt);
    }

    [Fact]
    public async Task UpdateRun_SkipsTerminalRuns()
    {
        var run = CreateRun(status: JobStatus.Completed);
        run.CompletedAt = DateTimeOffset.UtcNow;
        run.Progress = 1.0;
        run.NodeName = "node-1";
        await Store.CreateRunsAsync([run]);

        var updated = CreateRun(id: run.Id);
        updated.JobName = run.JobName;
        updated.NodeName = "node-1";
        updated.Progress = 0.5;
        updated.Error = "should not persist";

        await Store.UpdateRunAsync(updated);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Equal(1.0, loaded.Progress);
        Assert.Null(loaded.Error);
    }

    [Fact]
    public async Task GetRuns_FilterByCreatedRange()
    {
        var jobName = $"CreatedRange_{Guid.CreateVersion7():N}";

        var early = CreateRun(jobName);
        early.CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-30);

        var middle = CreateRun(jobName);
        middle.CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-15);

        var late = CreateRun(jobName);
        late.CreatedAt = DateTimeOffset.UtcNow;

        await Store.CreateRunsAsync([early, middle, late]);

        var results = await Store.GetRunsAsync(new()
        {
            JobName = jobName,
            ExactJobName = true,
            CreatedAfter = DateTimeOffset.UtcNow.AddMinutes(-20),
            CreatedBefore = DateTimeOffset.UtcNow.AddMinutes(-10)
        });

        Assert.Single(results.Items);
        Assert.Equal(middle.Id, results.Items[0].Id);
    }

    [Fact]
    public async Task GetRuns_OrderBy_StartedAt()
    {
        var jobName = $"OrderStarted_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var baseTime = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run1 = CreateRun(jobName, JobStatus.Running);
        run1.StartedAt = baseTime.AddMinutes(-3);
        var run2 = CreateRun(jobName, JobStatus.Running);
        run2.StartedAt = baseTime.AddMinutes(-2);
        var run3 = CreateRun(jobName, JobStatus.Running);
        run3.StartedAt = baseTime.AddMinutes(-1);

        await Store.CreateRunsAsync([run1, run2, run3]);

        var results = await Store.GetRunsAsync(new()
        {
            JobName = jobName,
            ExactJobName = true,
            OrderBy = RunOrderBy.StartedAt
        });

        Assert.Equal(3, results.Items.Count);
        Assert.Equal(run3.Id, results.Items[0].Id);
        Assert.Equal(run2.Id, results.Items[1].Id);
        Assert.Equal(run1.Id, results.Items[2].Id);
    }

    [Fact]
    public async Task GetRuns_OrderBy_CompletedAt()
    {
        var jobName = $"OrderCompleted_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var baseTime = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run1 = CreateRun(jobName, JobStatus.Completed);
        run1.CompletedAt = baseTime.AddMinutes(-3);
        var run2 = CreateRun(jobName, JobStatus.Completed);
        run2.CompletedAt = baseTime.AddMinutes(-2);
        var run3 = CreateRun(jobName, JobStatus.Completed);
        run3.CompletedAt = baseTime.AddMinutes(-1);

        await Store.CreateRunsAsync([run1, run2, run3]);

        var results = await Store.GetRunsAsync(new()
        {
            JobName = jobName,
            ExactJobName = true,
            OrderBy = RunOrderBy.CompletedAt
        });

        Assert.Equal(3, results.Items.Count);
        Assert.Equal(run3.Id, results.Items[0].Id);
        Assert.Equal(run2.Id, results.Items[1].Id);
        Assert.Equal(run1.Id, results.Items[2].Id);
    }

    [Fact]
    public async Task UpdateRun_SkipsWrongNode()
    {
        var jobName = $"WrongNode_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);

        var claimed = await Store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(claimed);

        claimed.NodeName = "wrong-node";
        claimed.Progress = 0.5;
        await Store.UpdateRunAsync(claimed);

        var after = await Store.GetRunAsync(run.Id);
        Assert.Equal(0.0, after!.Progress);
    }

    [Fact]
    public async Task GetRuns_FilterByNodeName()
    {
        var jobName = $"NodeFilter_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run1 = CreateRun(jobName, JobStatus.Running);
        run1.NodeName = "node-a";
        var run2 = CreateRun(jobName, JobStatus.Running);
        run2.NodeName = "node-b";

        await Store.CreateRunsAsync([run1, run2]);

        var results = await Store.GetRunsAsync(new() { NodeName = "node-a" });

        Assert.Single(results.Items);
        Assert.Equal(run1.Id, results.Items[0].Id);
    }

    [Fact]
    public async Task GetRuns_FilterByCompletedAfter()
    {
        var jobName = $"CompletedAfter_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var baseTime = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var run1 = CreateRun(jobName, JobStatus.Completed);
        run1.CompletedAt = baseTime.AddMinutes(-10);
        var run2 = CreateRun(jobName, JobStatus.Completed);
        run2.CompletedAt = baseTime.AddMinutes(-1);

        await Store.CreateRunsAsync([run1, run2]);

        var results = await Store.GetRunsAsync(new()
        {
            JobName = jobName,
            ExactJobName = true,
            CompletedAfter = baseTime.AddMinutes(-5)
        });

        Assert.Single(results.Items);
        Assert.Equal(run2.Id, results.Items[0].Id);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_And_MaxActive_Combined()
    {
        var jobName = $"DedupMaxActive_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        job.MaxConcurrency = 5;
        await Store.UpsertJobAsync(job);

        var run1 = CreateRun(jobName);
        run1.DeduplicationId = "unique-1";
        var created1 = await Store.TryCreateRunAsync(run1, 5);
        Assert.True(created1);

        var run2 = CreateRun(jobName);
        run2.DeduplicationId = "unique-1";
        var created2 = await Store.TryCreateRunAsync(run2, 5);
        Assert.False(created2);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_Concurrent_ExactlyOneWins()
    {
        await Store.UpsertJobAsync(CreateJob("TestJob"));

        for (var trial = 0; trial < 20; trial++)
        {
            var dedupId = Guid.NewGuid().ToString("N");
            var results = await Task.WhenAll(
                Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
                {
                    await Task.Delay(1);
                    var run = CreateRun("TestJob");
                    run.DeduplicationId = dedupId;
                    return await Store.TryCreateRunAsync(run);
                })));

            Assert.Equal(1, results.Count(r => r));
        }
    }

    [Fact]
    public async Task TryCreateRun_MaxActive_Concurrent_RespectsLimit()
    {
        var job = CreateJob("TestJob");
        job.MaxConcurrency = 1;
        await Store.UpsertJobAsync(job);

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
            var runs = await Store.GetRunsAsync(new() { JobName = "TestJob", ExactJobName = true, IsTerminal = false },
                take: 10);
            foreach (var run in runs.Items)
            {
                await Store.TryCancelRunAsync(run.Id);
            }
        }
    }

    [Fact]
    public async Task UpdateRun_NonExistent_IsNoOp()
    {
        var run = CreateRun("TestJob");
        run.Progress = 0.5;
        // Should not throw
        await Store.UpdateRunAsync(run);
    }

    [Fact]
    public async Task CreateRuns_DuplicateId_Throws()
    {
        var jobName = $"AtomicJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run1 = CreateRun(jobName);
        await Store.CreateRunsAsync([run1]);

        var run2 = CreateRun(jobName);
        var duplicate = CreateRun(jobName, id: run1.Id);

        await Assert.ThrowsAnyAsync<Exception>(() => Store.CreateRunsAsync([run2, duplicate]));

        var loaded = await Store.GetRunAsync(run2.Id);
        Assert.Null(loaded);
    }

    [Fact]
    public async Task TryCreateRun_DuplicateId_ReturnsFalse_NotException()
    {
        var jobName = $"TryCreateDup_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run1 = CreateRun(jobName);
        Assert.True(await Store.TryCreateRunAsync(run1));

        var duplicate = CreateRun(jobName, id: run1.Id);
        var created = await Store.TryCreateRunAsync(duplicate);

        Assert.False(created);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_AllowedAfterPurge()
    {
        var jobName = $"DedupPurge_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run1 = CreateRun(jobName);
        run1.DeduplicationId = dedupId;
        run1.CreatedAt = DateTimeOffset.UtcNow.AddDays(-30);
        run1.NotBefore = run1.CreatedAt;
        Assert.True(await Store.TryCreateRunAsync(run1));

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"]);
        Assert.NotNull(claimed);
        claimed.Status = JobStatus.Completed;
        claimed.CompletedAt = DateTimeOffset.UtcNow.AddDays(-30);
        await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));

        await Store.PurgeAsync(DateTimeOffset.UtcNow.AddDays(-7));

        Assert.Null(await Store.GetRunAsync(run1.Id));

        var run2 = CreateRun(jobName);
        run2.DeduplicationId = dedupId;
        Assert.True(await Store.TryCreateRunAsync(run2));
    }

    [Fact]
    public async Task GetRuns_MultipleFilters_IntersectsCorrectly()
    {
        var jobName = $"MultiFilter_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);

        var oldPending = CreateRun(jobName);
        oldPending.CreatedAt = now.AddHours(-2);
        oldPending.NotBefore = oldPending.CreatedAt;

        var recentPending = CreateRun(jobName);
        recentPending.CreatedAt = now.AddMinutes(-5);
        recentPending.NotBefore = recentPending.CreatedAt;

        await Store.CreateRunsAsync([oldPending, recentPending]);

        var claimed = await Store.ClaimRunAsync("node1", [jobName], ["default"]);
        Assert.NotNull(claimed);
        claimed.Status = JobStatus.Completed;
        claimed.CompletedAt = now;
        await Store.TryTransitionRunAsync(Transition(claimed, JobStatus.Running));

        var result = await Store.GetRunsAsync(new()
        {
            Status = JobStatus.Pending,
            JobName = jobName,
            ExactJobName = true,
            CreatedAfter = now.AddHours(-1)
        }, take: 10);

        Assert.Equal(1, result.TotalCount);
        Assert.Equal(recentPending.Id, result.Items[0].Id);
    }

    [Fact]
    public async Task GetRun_NonBatch_BatchFieldsAreNull()
    {
        var run = CreateRun();
        await Store.CreateRunsAsync([run]);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Null(loaded.BatchTotal);
        Assert.Null(loaded.BatchCompleted);
        Assert.Null(loaded.BatchFailed);
    }

    [Fact]
    public async Task GetRuns_IsTerminal_Pagination()
    {
        var jobName = $"TermPage_{Guid.CreateVersion7():N}";

        var runs = new List<JobRun>();
        for (var i = 0; i < 5; i++)
        {
            var r = CreateRun(jobName, JobStatus.Completed);
            r.CompletedAt = DateTimeOffset.UtcNow;
            runs.Add(r);
        }

        var pending = CreateRun(jobName);
        runs.Add(pending);
        await Store.CreateRunsAsync(runs);

        var page = await Store.GetRunsAsync(new()
        {
            IsTerminal = true,
            JobName = jobName,
            ExactJobName = true
        }, 0, 3);

        Assert.Equal(5, page.TotalCount);
        Assert.Equal(3, page.Items.Count);
        Assert.All(page.Items, r => Assert.True(r.Status.IsTerminal));
    }

    [Fact]
    public async Task UpdateRun_NullNodeName_Succeeds()
    {
        var jobName = $"NullNode_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);

        // Update progress on a run that has no node assigned
        run.Progress = 0.5;
        await Store.UpdateRunAsync(run);

        var loaded = await Store.GetRunAsync(run.Id);
        Assert.NotNull(loaded);
        Assert.Null(loaded.NodeName);
        Assert.Equal(0.5, loaded.Progress);
    }

    [Fact]
    public async Task TryCreateRun_Dedup_FreedOnCancel()
    {
        var jobName = $"DedupCancel_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run1 = CreateRun(jobName);
        run1.DeduplicationId = dedupId;
        Assert.True(await Store.TryCreateRunAsync(run1));

        Assert.True(await Store.TryCancelRunAsync(run1.Id));

        var run2 = CreateRun(jobName);
        run2.DeduplicationId = dedupId;
        Assert.True(await Store.TryCreateRunAsync(run2));
    }

    [Fact]
    public async Task TryCreateRun_Dedup_BlocksWhileActive()
    {
        var jobName = $"DedupActive_{Guid.CreateVersion7():N}";
        var dedupId = Guid.CreateVersion7().ToString("N");
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run1 = CreateRun(jobName);
        run1.DeduplicationId = dedupId;
        Assert.True(await Store.TryCreateRunAsync(run1));

        var run2 = CreateRun(jobName);
        run2.DeduplicationId = dedupId;
        Assert.False(await Store.TryCreateRunAsync(run2));
    }

    [Fact]
    public async Task TryCreateRun_LastCronFireAt_UpdatedAtomically()
    {
        var jobName = $"CronAtomic_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var fireAt = DateTimeOffset.UtcNow;
        var run = CreateRun(jobName);
        Assert.True(await Store.TryCreateRunAsync(run, lastCronFireAt: fireAt));

        var job = await Store.GetJobAsync(jobName);
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
        var jobName = $"CronReject_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run1 = CreateRun(jobName);
        run1.DeduplicationId = "same";
        Assert.True(await Store.TryCreateRunAsync(run1, lastCronFireAt: DateTimeOffset.UtcNow.AddMinutes(-5)));

        var run2 = CreateRun(jobName);
        run2.DeduplicationId = "same";
        var laterFireAt = DateTimeOffset.UtcNow;
        Assert.False(await Store.TryCreateRunAsync(run2, lastCronFireAt: laterFireAt));

        var job = await Store.GetJobAsync(jobName);
        Assert.NotNull(job);
        // LastCronFireAt should still be the original value, not the rejected one
        Assert.NotEqual(
            laterFireAt.ToUnixTimeMilliseconds(),
            job!.LastCronFireAt!.Value.ToUnixTimeMilliseconds());
    }
}