namespace Surefire.Tests.Conformance;

public abstract class EventConformanceTests : StoreConformanceBase
{
    private async Task<string> CreateRunForEventsAsync()
    {
        var jobName = $"EventJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);
        return run.Id;
    }

    [Fact]
    public async Task AppendEvents_AssignsSequentialIds()
    {
        var runId = await CreateRunForEventsAsync();

        var events = new List<RunEvent>
        {
            new() { RunId = runId, EventType = RunEventType.Log, Payload = "first", CreatedAt = DateTimeOffset.UtcNow },
            new()
            {
                RunId = runId, EventType = RunEventType.Log, Payload = "second", CreatedAt = DateTimeOffset.UtcNow
            },
            new() { RunId = runId, EventType = RunEventType.Log, Payload = "third", CreatedAt = DateTimeOffset.UtcNow }
        };

        await Store.AppendEventsAsync(events);

        var loaded = await Store.GetEventsAsync(runId);
        Assert.Equal(3, loaded.Count);
        Assert.True(loaded[0].Id < loaded[1].Id);
        Assert.True(loaded[1].Id < loaded[2].Id);
    }

    [Fact]
    public async Task AppendEvents_SingleEvent_Works()
    {
        var runId = await CreateRunForEventsAsync();

        await Store.AppendEventsAsync([
            new() { RunId = runId, EventType = RunEventType.Log, Payload = "solo", CreatedAt = DateTimeOffset.UtcNow }
        ]);

        var loaded = await Store.GetEventsAsync(runId);
        Assert.Single(loaded);
        Assert.Equal("solo", loaded[0].Payload);
    }

    [Fact]
    public async Task GetEvents_SinceId_ReturnsOnlyNewer()
    {
        var runId = await CreateRunForEventsAsync();

        var events = Enumerable.Range(1, 5)
            .Select(i => new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Log,
                Payload = $"event-{i}",
                CreatedAt = DateTimeOffset.UtcNow
            })
            .ToList();

        await Store.AppendEventsAsync(events);

        var all = await Store.GetEventsAsync(runId);
        Assert.Equal(5, all.Count);

        var sinceId = all[1].Id;
        var newer = await Store.GetEventsAsync(runId, sinceId);

        Assert.Equal(3, newer.Count);
        Assert.True(newer.All(e => e.Id > sinceId));
    }

    [Fact]
    public async Task GetEvents_FilterByType()
    {
        var runId = await CreateRunForEventsAsync();

        await Store.AppendEventsAsync([
            new() { RunId = runId, EventType = RunEventType.Log, Payload = "log-1", CreatedAt = DateTimeOffset.UtcNow },
            new()
            {
                RunId = runId, EventType = RunEventType.Progress, Payload = "0.5", CreatedAt = DateTimeOffset.UtcNow
            },
            new() { RunId = runId, EventType = RunEventType.Log, Payload = "log-2", CreatedAt = DateTimeOffset.UtcNow },
            new() { RunId = runId, EventType = RunEventType.Output, Payload = "out", CreatedAt = DateTimeOffset.UtcNow }
        ]);

        var logs = await Store.GetEventsAsync(runId, types: [RunEventType.Log]);

        Assert.Equal(2, logs.Count);
        Assert.All(logs, e => Assert.Equal(RunEventType.Log, e.EventType));
    }

    [Fact]
    public async Task GetEvents_FilterByAttempt()
    {
        var runId = await CreateRunForEventsAsync();

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
        ]);

        var attempt1 = await Store.GetEventsAsync(runId, attempt: 1);

        Assert.Equal(2, attempt1.Count);
        Assert.Contains(attempt1, e => e.Attempt == 0);
        Assert.Contains(attempt1, e => e.Attempt == 1);
        Assert.DoesNotContain(attempt1, e => e.Attempt == 2);
    }

    [Fact]
    public async Task GetEvents_ReturnsIsolatedCopies()
    {
        var runId = await CreateRunForEventsAsync();

        await Store.AppendEventsAsync([
            new()
            {
                RunId = runId, EventType = RunEventType.Log, Payload = "original", CreatedAt = DateTimeOffset.UtcNow
            }
        ]);

        var events1 = await Store.GetEventsAsync(runId);
        Assert.Single(events1);
        events1[0].Payload = "mutated";

        var events2 = await Store.GetEventsAsync(runId);
        Assert.Single(events2);
        Assert.Equal("original", events2[0].Payload);
    }

    [Fact]
    public async Task GetEvents_EmptyRun_ReturnsEmpty()
    {
        var runId = await CreateRunForEventsAsync();

        var events = await Store.GetEventsAsync(runId);

        Assert.Empty(events);
    }

    [Fact]
    public async Task AppendEvents_LargeBatch_500Events()
    {
        var runId = await CreateRunForEventsAsync();

        var events = Enumerable.Range(1, 500)
            .Select(i => new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Log,
                Payload = $"event-{i}",
                CreatedAt = DateTimeOffset.UtcNow
            })
            .ToList();

        await Store.AppendEventsAsync(events);

        var loaded = await Store.GetEventsAsync(runId);
        Assert.Equal(500, loaded.Count);
    }

    [Fact]
    public async Task GetEvents_NoAttemptFilter_ReturnsAll()
    {
        var runId = await CreateRunForEventsAsync();

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
        ]);

        var all = await Store.GetEventsAsync(runId, attempt: null);

        Assert.Equal(3, all.Count);
        Assert.Contains(all, e => e.Attempt == 0);
        Assert.Contains(all, e => e.Attempt == 1);
        Assert.Contains(all, e => e.Attempt == 2);
    }

    [Fact]
    public async Task GetEvents_FilterByTypeAndAttempt_IncludesAttemptZeroEvents()
    {
        var runId = await CreateRunForEventsAsync();

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
        ]);

        var filtered = await Store.GetEventsAsync(runId, types: [RunEventType.Output], attempt: 2);

        Assert.Equal(2, filtered.Count);
        Assert.All(filtered, e => Assert.Equal(RunEventType.Output, e.EventType));
        Assert.Contains(filtered, e => e.Attempt == 0 && e.Payload == "shared-output");
        Assert.Contains(filtered, e => e.Attempt == 2 && e.Payload == "attempt-2-output");
        Assert.DoesNotContain(filtered, e => e.Attempt == 3);
    }

    [Fact]
    public async Task GetEvents_WithTake_LimitsResults()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);
        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var events = Enumerable.Range(1, 10).Select(i => new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.Log,
            Payload = $"log-{i}",
            CreatedAt = DateTimeOffset.UtcNow,
            Attempt = 1
        }).ToList();
        await Store.AppendEventsAsync(events);

        var limited = await Store.GetEventsAsync(run.Id, take: 3);

        Assert.Equal(3, limited.Count);
        Assert.Equal("log-1", limited[0].Payload);
        Assert.Equal("log-2", limited[1].Payload);
        Assert.Equal("log-3", limited[2].Payload);
    }

    [Fact]
    public async Task GetEvents_WithTake_AndSinceId_ReturnsCorrectSlice()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);
        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var events = Enumerable.Range(1, 10).Select(i => new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.Log,
            Payload = $"log-{i}",
            CreatedAt = DateTimeOffset.UtcNow,
            Attempt = 1
        }).ToList();
        await Store.AppendEventsAsync(events);

        var all = await Store.GetEventsAsync(run.Id);
        var cursorId = all[2].Id; // after 3rd event

        var slice = await Store.GetEventsAsync(run.Id, sinceId: cursorId, take: 2);

        Assert.Equal(2, slice.Count);
        Assert.Equal("log-4", slice[0].Payload);
        Assert.Equal("log-5", slice[1].Payload);
    }

    [Fact]
    public async Task GetEvents_WithTake_AndTypeFilter_LimitsCorrectly()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);
        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        await Store.AppendEventsAsync([
            new() { RunId = run.Id, EventType = RunEventType.Log, Payload = "log-1", CreatedAt = DateTimeOffset.UtcNow, Attempt = 1 },
            new() { RunId = run.Id, EventType = RunEventType.Progress, Payload = "0.5", CreatedAt = DateTimeOffset.UtcNow, Attempt = 1 },
            new() { RunId = run.Id, EventType = RunEventType.Log, Payload = "log-2", CreatedAt = DateTimeOffset.UtcNow, Attempt = 1 },
            new() { RunId = run.Id, EventType = RunEventType.Log, Payload = "log-3", CreatedAt = DateTimeOffset.UtcNow, Attempt = 1 },
            new() { RunId = run.Id, EventType = RunEventType.Progress, Payload = "1.0", CreatedAt = DateTimeOffset.UtcNow, Attempt = 1 }
        ]);

        var logs = await Store.GetEventsAsync(run.Id, types: [RunEventType.Log], take: 2);

        Assert.Equal(2, logs.Count);
        Assert.All(logs, e => Assert.Equal(RunEventType.Log, e.EventType));
        Assert.Equal("log-1", logs[0].Payload);
        Assert.Equal("log-2", logs[1].Payload);
    }

    [Fact]
    public async Task GetEvents_WithTakeNull_ReturnsAll()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);
        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        var events = Enumerable.Range(1, 10).Select(i => new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.Log,
            Payload = $"log-{i}",
            CreatedAt = DateTimeOffset.UtcNow,
            Attempt = 1
        }).ToList();
        await Store.AppendEventsAsync(events);

        var all = await Store.GetEventsAsync(run.Id, take: null);

        Assert.Equal(10, all.Count);
    }

    [Fact]
    public async Task GetEvents_WithTake_LargerThanTotal_ReturnsAll()
    {
        var job = CreateJob();
        await Store.UpsertJobAsync(job);
        var run = CreateRun(job.Name);
        await Store.CreateRunsAsync([run]);

        await Store.AppendEventsAsync([
            new() { RunId = run.Id, EventType = RunEventType.Log, Payload = "a", CreatedAt = DateTimeOffset.UtcNow, Attempt = 1 },
            new() { RunId = run.Id, EventType = RunEventType.Log, Payload = "b", CreatedAt = DateTimeOffset.UtcNow, Attempt = 1 },
            new() { RunId = run.Id, EventType = RunEventType.Log, Payload = "c", CreatedAt = DateTimeOffset.UtcNow, Attempt = 1 }
        ]);

        var result = await Store.GetEventsAsync(run.Id, take: 100);

        Assert.Equal(3, result.Count);
    }
}