namespace Surefire.Tests.Conformance;

public abstract class EventConformanceTests : StoreConformanceBase
{
    private async Task<string> CreateRunForEventsAsync(CancellationToken ct)
    {
        var jobName = $"EventJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run], cancellationToken: ct);
        return run.Id;
    }

    private async Task<(string BatchId, JobRun ChildA, JobRun ChildB)> CreateBatchForEventsAsync(CancellationToken ct)
    {
        var jobName = $"BatchEventJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName), ct);

        var batchId = Guid.CreateVersion7().ToString("N");
        var createdAt = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        var batch = new JobBatch
        {
            Id = batchId,
            CreatedAt = createdAt,
            Total = 2,
            Status = JobStatus.Pending
        };

        var childA = CreateRun(jobName) with { BatchId = batchId, RootRunId = batchId };
        var childB = CreateRun(jobName) with { BatchId = batchId, RootRunId = batchId };
        await Store.CreateBatchAsync(batch, [childA, childB], cancellationToken: ct);
        return (batchId, childA, childB);
    }

    [Fact]
    public async Task AppendEvents_AssignsSequentialIds()
    {
        var ct = TestContext.Current.CancellationToken;
        var runId = await CreateRunForEventsAsync(ct);

        var events = new List<RunEvent>
        {
            new() { RunId = runId, EventType = RunEventType.Log, Payload = "first", CreatedAt = DateTimeOffset.UtcNow },
            new()
            {
                RunId = runId, EventType = RunEventType.Log, Payload = "second", CreatedAt = DateTimeOffset.UtcNow
            },
            new() { RunId = runId, EventType = RunEventType.Log, Payload = "third", CreatedAt = DateTimeOffset.UtcNow }
        };

        await Store.AppendEventsAsync(events, ct);

        var loaded = await Store.GetEventsAsync(runId, cancellationToken: ct);
        Assert.Equal(3, loaded.Count);
        Assert.True(loaded[0].Id < loaded[1].Id);
        Assert.True(loaded[1].Id < loaded[2].Id);
    }

    [Fact]
    public async Task AppendEvents_SingleEvent_Works()
    {
        var ct = TestContext.Current.CancellationToken;
        var runId = await CreateRunForEventsAsync(ct);

        await Store.AppendEventsAsync([
            new() { RunId = runId, EventType = RunEventType.Log, Payload = "solo", CreatedAt = DateTimeOffset.UtcNow }
        ], ct);

        var loaded = await Store.GetEventsAsync(runId, cancellationToken: ct);
        Assert.Single(loaded);
        Assert.Equal("solo", loaded[0].Payload);
    }

    [Fact]
    public async Task GetEvents_SinceId_ReturnsOnlyNewer()
    {
        var ct = TestContext.Current.CancellationToken;
        var runId = await CreateRunForEventsAsync(ct);

        var events = Enumerable.Range(1, 5)
            .Select(i => new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Log,
                Payload = $"event-{i}",
                CreatedAt = DateTimeOffset.UtcNow
            })
            .ToList();

        await Store.AppendEventsAsync(events, ct);

        var all = await Store.GetEventsAsync(runId, cancellationToken: ct);
        Assert.Equal(5, all.Count);

        var sinceId = all[1].Id;
        var newer = await Store.GetEventsAsync(runId, sinceId, cancellationToken: ct);

        Assert.Equal(3, newer.Count);
        Assert.True(newer.All(e => e.Id > sinceId));
    }

    [Fact]
    public async Task GetEvents_FilterByType()
    {
        var ct = TestContext.Current.CancellationToken;
        var runId = await CreateRunForEventsAsync(ct);

        await Store.AppendEventsAsync([
            new() { RunId = runId, EventType = RunEventType.Log, Payload = "log-1", CreatedAt = DateTimeOffset.UtcNow },
            new()
            {
                RunId = runId, EventType = RunEventType.Progress, Payload = "0.5", CreatedAt = DateTimeOffset.UtcNow
            },
            new() { RunId = runId, EventType = RunEventType.Log, Payload = "log-2", CreatedAt = DateTimeOffset.UtcNow },
            new() { RunId = runId, EventType = RunEventType.Output, Payload = "out", CreatedAt = DateTimeOffset.UtcNow }
        ], ct);

        var logs = await Store.GetEventsAsync(runId, types: [RunEventType.Log], cancellationToken: ct);

        Assert.Equal(2, logs.Count);
        Assert.All(logs, e => Assert.Equal(RunEventType.Log, e.EventType));
    }

    [Fact]
    public async Task GetEvents_FilterByAttempt()
    {
        var ct = TestContext.Current.CancellationToken;
        var runId = await CreateRunForEventsAsync(ct);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = runId, EventType = RunEventType.Status, Payload = "created", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 0
            },
            new()
            {
                RunId = runId, EventType = RunEventType.Log, Payload = "attempt-1-log",
                CreatedAt = DateTimeOffset.UtcNow, Attempt = 1
            },
            new()
            {
                RunId = runId, EventType = RunEventType.Log, Payload = "attempt-2-log",
                CreatedAt = DateTimeOffset.UtcNow, Attempt = 2
            }
        ], ct);

        var attempt1 = await Store.GetEventsAsync(runId, attempt: 1, cancellationToken: ct);

        Assert.Equal(2, attempt1.Count);
        Assert.Contains(attempt1, e => e.Attempt == 0);
        Assert.Contains(attempt1, e => e.Attempt == 1);
        Assert.DoesNotContain(attempt1, e => e.Attempt == 2);
    }

    [Fact]
    public async Task GetEvents_ReturnsIsolatedCopies()
    {
        var ct = TestContext.Current.CancellationToken;
        var runId = await CreateRunForEventsAsync(ct);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = runId, EventType = RunEventType.Log, Payload = "original", CreatedAt = DateTimeOffset.UtcNow
            }
        ], ct);

        var events1 = await Store.GetEventsAsync(runId, cancellationToken: ct);
        Assert.Single(events1);
        var mutated = events1[0] with { Payload = "mutated" };

        var events2 = await Store.GetEventsAsync(runId, cancellationToken: ct);
        Assert.Single(events2);
        Assert.Equal("original", events2[0].Payload);
    }

    [Fact]
    public async Task GetEvents_EmptyRun_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        var runId = await CreateRunForEventsAsync(ct);

        var events = await Store.GetEventsAsync(runId, cancellationToken: ct);

        Assert.Empty(events);
    }

    [Fact]
    public async Task AppendEvents_LargeBatch_500Events()
    {
        var ct = TestContext.Current.CancellationToken;
        var runId = await CreateRunForEventsAsync(ct);

        var events = Enumerable.Range(1, 500)
            .Select(i => new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Log,
                Payload = $"event-{i}",
                CreatedAt = DateTimeOffset.UtcNow
            })
            .ToList();

        await Store.AppendEventsAsync(events, ct);

        var loaded = await Store.GetEventsAsync(runId, cancellationToken: ct);
        Assert.Equal(500, loaded.Count);
    }

    [Fact]
    public async Task GetEvents_NoAttemptFilter_ReturnsAll()
    {
        var ct = TestContext.Current.CancellationToken;
        var runId = await CreateRunForEventsAsync(ct);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = runId, EventType = RunEventType.Status, Payload = "created", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 0
            },
            new()
            {
                RunId = runId, EventType = RunEventType.Log, Payload = "attempt-1-log",
                CreatedAt = DateTimeOffset.UtcNow, Attempt = 1
            },
            new()
            {
                RunId = runId, EventType = RunEventType.Log, Payload = "attempt-2-log",
                CreatedAt = DateTimeOffset.UtcNow, Attempt = 2
            }
        ], ct);

        var all = await Store.GetEventsAsync(runId, attempt: null, cancellationToken: ct);

        Assert.Equal(3, all.Count);
        Assert.Contains(all, e => e.Attempt == 0);
        Assert.Contains(all, e => e.Attempt == 1);
        Assert.Contains(all, e => e.Attempt == 2);
    }

    [Fact]
    public async Task GetEvents_FilterByTypeAndAttempt_IncludesAttemptZeroEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        var runId = await CreateRunForEventsAsync(ct);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = "shared-output",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 0
            },
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = "attempt-2-output",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 2
            },
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = "attempt-3-output",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 3
            },
            new()
            {
                RunId = runId,
                EventType = RunEventType.Log,
                Payload = "attempt-2-log",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 2
            }
        ], ct);

        var filtered =
            await Store.GetEventsAsync(runId, types: [RunEventType.Output], attempt: 2, cancellationToken: ct);

        Assert.Equal(2, filtered.Count);
        Assert.All(filtered, e => Assert.Equal(RunEventType.Output, e.EventType));
        Assert.Contains(filtered, e => e.Attempt == 0 && e.Payload == "shared-output");
        Assert.Contains(filtered, e => e.Attempt == 2 && e.Payload == "attempt-2-output");
        Assert.DoesNotContain(filtered, e => e.Attempt == 3);
    }

    [Fact]
    public async Task GetEvents_WithTake_LimitsResults()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);
        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var events = Enumerable.Range(1, 10).Select(i => new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.Log,
            Payload = $"log-{i}",
            CreatedAt = DateTimeOffset.UtcNow,
            Attempt = 1
        }).ToList();
        await Store.AppendEventsAsync(events, ct);

        var limited = await Store.GetEventsAsync(run.Id, take: 3, cancellationToken: ct);

        Assert.Equal(3, limited.Count);
        Assert.Equal("log-1", limited[0].Payload);
        Assert.Equal("log-2", limited[1].Payload);
        Assert.Equal("log-3", limited[2].Payload);
    }

    [Fact]
    public async Task GetEvents_WithTake_AndSinceId_ReturnsCorrectSlice()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);
        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var events = Enumerable.Range(1, 10).Select(i => new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.Log,
            Payload = $"log-{i}",
            CreatedAt = DateTimeOffset.UtcNow,
            Attempt = 1
        }).ToList();
        await Store.AppendEventsAsync(events, ct);

        var all = await Store.GetEventsAsync(run.Id, cancellationToken: ct);
        var cursorId = all[2].Id; // after 3rd event

        var slice = await Store.GetEventsAsync(run.Id, cursorId, take: 2, cancellationToken: ct);

        Assert.Equal(2, slice.Count);
        Assert.Equal("log-4", slice[0].Payload);
        Assert.Equal("log-5", slice[1].Payload);
    }

    [Fact]
    public async Task GetEvents_WithTake_AndTypeFilter_LimitsCorrectly()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);
        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = run.Id, EventType = RunEventType.Log, Payload = "log-1", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id, EventType = RunEventType.Progress, Payload = "0.5", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id, EventType = RunEventType.Log, Payload = "log-2", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id, EventType = RunEventType.Log, Payload = "log-3", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id, EventType = RunEventType.Progress, Payload = "1.0", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            }
        ], ct);

        var logs = await Store.GetEventsAsync(run.Id, types: [RunEventType.Log], take: 2, cancellationToken: ct);

        Assert.Equal(2, logs.Count);
        Assert.All(logs, e => Assert.Equal(RunEventType.Log, e.EventType));
        Assert.Equal("log-1", logs[0].Payload);
        Assert.Equal("log-2", logs[1].Payload);
    }

    [Fact]
    public async Task GetEvents_WithTakeNull_ReturnsAll()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);
        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        var events = Enumerable.Range(1, 10).Select(i => new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.Log,
            Payload = $"log-{i}",
            CreatedAt = DateTimeOffset.UtcNow,
            Attempt = 1
        }).ToList();
        await Store.AppendEventsAsync(events, ct);

        var all = await Store.GetEventsAsync(run.Id, take: null, cancellationToken: ct);

        Assert.Equal(10, all.Count);
    }

    [Fact]
    public async Task GetEvents_WithTake_LargerThanTotal_ReturnsAll()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        await Store.UpsertJobAsync(job, ct);
        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run], cancellationToken: ct);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = run.Id, EventType = RunEventType.Log, Payload = "a", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id, EventType = RunEventType.Log, Payload = "b", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            new()
            {
                RunId = run.Id, EventType = RunEventType.Log, Payload = "c", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            }
        ], ct);

        var result = await Store.GetEventsAsync(run.Id, take: 100, cancellationToken: ct);

        Assert.Equal(3, result.Count);
    }

    [Fact]
    public async Task GetBatchEvents_ReturnsGlobalOrderAcrossChildren()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batchId, childA, childB) = await CreateBatchForEventsAsync(ct);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = childB.Id, EventType = RunEventType.Progress, Payload = "0.5",
                CreatedAt = DateTimeOffset.UtcNow, Attempt = 1
            },
            new()
            {
                RunId = childA.Id, EventType = RunEventType.Output, Payload = "a-1", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            new()
            {
                RunId = childB.Id, EventType = RunEventType.Output, Payload = "b-1", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            }
        ], ct);

        var events = await Store.GetBatchEventsAsync(batchId, cancellationToken: ct);

        Assert.Equal(3, events.Count);
        Assert.True(events[0].Id < events[1].Id && events[1].Id < events[2].Id);
        Assert.Equal([childB.Id, childA.Id, childB.Id], events.Select(e => e.RunId).ToArray());
    }

    [Fact]
    public async Task GetBatchOutputEvents_FiltersAndResumesBatchWide()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batchId, childA, childB) = await CreateBatchForEventsAsync(ct);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = childA.Id, EventType = RunEventType.Log, Payload = "log-a", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            new()
            {
                RunId = childA.Id, EventType = RunEventType.Output, Payload = "a-1", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            new()
            {
                RunId = childB.Id, EventType = RunEventType.Output, Payload = "b-1", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            new()
            {
                RunId = childB.Id, EventType = RunEventType.OutputComplete, Payload = "null",
                CreatedAt = DateTimeOffset.UtcNow, Attempt = 1
            }
        ], ct);

        var firstPage = await Store.GetBatchOutputEventsAsync(batchId, take: 1, cancellationToken: ct);
        Assert.Single(firstPage);
        Assert.Equal(RunEventType.Output, firstPage[0].EventType);

        var resumed = await Store.GetBatchOutputEventsAsync(batchId, firstPage[0].Id, cancellationToken: ct);

        Assert.Single(resumed);
        Assert.All(resumed, e => Assert.Equal(RunEventType.Output, e.EventType));
        Assert.DoesNotContain(resumed, e => e.Id <= firstPage[0].Id);
        Assert.Equal([childB.Id], resumed.Select(e => e.RunId).ToArray());
    }

    [Fact]
    public async Task GetBatchEvents_ResumeCursor_IncludesEventsAppendedLater()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batchId, childA, childB) = await CreateBatchForEventsAsync(ct);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = childA.Id, EventType = RunEventType.Log, Payload = "a-1", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            }
        ], ct);

        var firstPage = await Store.GetBatchEventsAsync(batchId, take: 1, cancellationToken: ct);
        Assert.Single(firstPage);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = childB.Id, EventType = RunEventType.Progress, Payload = "0.5",
                CreatedAt = DateTimeOffset.UtcNow, Attempt = 1
            },
            new()
            {
                RunId = childA.Id, EventType = RunEventType.Output, Payload = "a-2", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            }
        ], ct);

        var resumed = await Store.GetBatchEventsAsync(batchId, firstPage[0].Id, cancellationToken: ct);

        Assert.Equal(2, resumed.Count);
        Assert.All(resumed, e => Assert.True(e.Id > firstPage[0].Id));
        Assert.Equal([childB.Id, childA.Id], resumed.Select(e => e.RunId).ToArray());
    }

    [Fact]
    public async Task GetBatchOutputEvents_ResumeCursor_IncludesOutputAppendedLater()
    {
        var ct = TestContext.Current.CancellationToken;
        var (batchId, childA, childB) = await CreateBatchForEventsAsync(ct);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = childA.Id, EventType = RunEventType.Output, Payload = "a-1", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            }
        ], ct);

        var firstPage = await Store.GetBatchOutputEventsAsync(batchId, take: 1, cancellationToken: ct);
        Assert.Single(firstPage);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = childA.Id, EventType = RunEventType.Log, Payload = "ignore-me",
                CreatedAt = DateTimeOffset.UtcNow, Attempt = 1
            },
            new()
            {
                RunId = childB.Id, EventType = RunEventType.Output, Payload = "b-1", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            },
            new()
            {
                RunId = childA.Id, EventType = RunEventType.Output, Payload = "a-2", CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            }
        ], ct);

        var resumed = await Store.GetBatchOutputEventsAsync(batchId, firstPage[0].Id, cancellationToken: ct);

        Assert.Equal(2, resumed.Count);
        Assert.All(resumed, e =>
        {
            Assert.True(e.Id > firstPage[0].Id);
            Assert.Equal(RunEventType.Output, e.EventType);
        });
        Assert.Equal([childB.Id, childA.Id], resumed.Select(e => e.RunId).ToArray());
    }
}