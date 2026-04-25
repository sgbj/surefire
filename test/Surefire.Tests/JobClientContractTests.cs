using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Surefire.Tests.Testing;

namespace Surefire.Tests;

public sealed class JobClientContractTests
{
    [Fact]
    public async Task TriggerAsync_UsesJobPriority_WhenRunPriorityNotProvided()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, eventWriter, time, new(), logger);

        var jobName = "PriorityDefault_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Priority = 17 }], ct);

        var triggered = await client.TriggerAsync(jobName, null, new(), ct);
        var runId = triggered.Id;

        var run = await store.GetRunAsync(runId, ct);
        Assert.NotNull(run);
        Assert.Equal(17, run.Priority);
        Assert.Empty(logger.Warnings);
    }

    [Fact]
    public async Task TriggerAsync_UsesRunPriorityOverride_WhenProvided()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, eventWriter, time, new(), logger);

        var jobName = "PriorityOverride_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Priority = 3 }], ct);

        var triggered = await client.TriggerAsync(jobName, null, new() { Priority = 99 }, ct);

        var runId = triggered.Id;

        var run = await store.GetRunAsync(runId, ct);
        Assert.NotNull(run);
        Assert.Equal(99, run.Priority);
        Assert.Empty(logger.Warnings);
    }

    [Fact]
    public async Task TriggerBatchAsync_PreservesPerItemRunOptions()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        // Regression: the heterogeneous TriggerBatchAsync(IEnumerable<BatchItem>, RunOptions?, CT)
        // overload silently overwrote each BatchItem's Options with the global one. The footgun
        // is fixed by removing the global parameter; this test pins the per-item behavior.
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var client = new JobClient(store, notifications, eventWriter, time, new(),
            new CollectingLogger<JobClient>());

        var jobName = "BatchPerItemOptions_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName }], ct);

        var batch = await client.TriggerBatchAsync(
            new[]
            {
                new BatchItem(jobName, new { x = 1 }, new() { Priority = 11 }),
                new BatchItem(jobName, new { x = 2 }, new() { Priority = 22 })
            },
            ct);

        var children = new List<JobRun>();
        await foreach (var child in client.GetRunsAsync(new() { BatchId = batch.Id }, ct))
        {
            children.Add(child);
        }

        Assert.Equal(2, children.Count);
        Assert.Contains(children, c => c.Priority == 11);
        Assert.Contains(children, c => c.Priority == 22);
    }

    [Fact]
    public async Task TriggerAsync_UnknownJob_LogsWarning_AndUsesDefaultPriority()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, eventWriter, time, new(), logger);

        var jobName = "UnknownJob_" + Guid.CreateVersion7().ToString("N");

        var triggered = await client.TriggerAsync(jobName, null, new(), ct);
        var runId = triggered.Id;

        var run = await store.GetRunAsync(runId, ct);
        Assert.NotNull(run);
        Assert.Equal(0, run.Priority);
        Assert.Single(logger.Warnings);
        Assert.Contains(jobName, logger.Warnings[0], StringComparison.Ordinal);
    }

    [Fact]
    public async Task TriggerAllAsync_WithDeduplicationId_ThrowsArgumentException()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, eventWriter, time, new(), logger);

        var jobName = "BatchDedup_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName }], ct);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.TriggerAllAsync(jobName, [1, 2], new() { DeduplicationId = "dup" }, ct));
    }

    [Fact]
    public async Task TriggerAsync_Rejected_ThrowsRunConflictException()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, eventWriter, time, new(), logger);

        var jobName = "ConflictType_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName }], ct);

        // Create first run with dedup id
        await client.TriggerAsync(jobName, null, new() { DeduplicationId = "unique-1" }, ct);

        // Second trigger with same dedup id should throw RunConflictException specifically
        await Assert.ThrowsAsync<RunConflictException>(() =>
            client.TriggerAsync(jobName, null, new() { DeduplicationId = "unique-1" }, ct));
    }

    [Fact]
    public async Task RunAsync_Cancellation_PropagatesToOwnedRun()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(25) },
            logger);

        var jobName = "CancelOwned_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName }], ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var runTask = client.RunAsync(jobName, cancellationToken: cts.Token);

        _ = await TestWait.PollUntilAsync(
            async pollCt =>
            {
                var runs = await store.GetRunsAsync(new() { JobName = jobName }, 0, 5, pollCt);
                return runs.Items.SingleOrDefault();
            },
            run => run is { },
            TimeSpan.FromSeconds(3),
            TimeSpan.FromMilliseconds(20),
            "Expected RunAsync to create an owned run before cancellation.",
            ct);

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => runTask);

        var cancelledRun = await TestWait.PollUntilAsync(
            async pollCt =>
            {
                var runs = await store.GetRunsAsync(new() { JobName = jobName }, 0, 5, pollCt);
                return runs.Items.SingleOrDefault();
            },
            run => run.Status == JobStatus.Cancelled,
            TimeSpan.FromSeconds(3),
            TimeSpan.FromMilliseconds(20),
            "Expected RunAsync cancellation to cancel the owned run.",
            ct);

        Assert.NotNull(cancelledRun.CompletedAt);
        Assert.NotNull(cancelledRun.CancelledAt);
    }

    [Fact]
    public async Task WaitAsync_Cancellation_CancelsWaitOnly()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(25) },
            logger);

        var jobName = "WaitOnly_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName }], ct);
        var triggered = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId = triggered.Id;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromMilliseconds(80));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.WaitAsync(runId, cts.Token));

        var run = await store.GetRunAsync(runId, ct);
        Assert.NotNull(run);
        Assert.Equal(JobStatus.Pending, run.Status);
        Assert.Null(run.CancelledAt);
    }

    [Fact]
    public async Task WaitAsync_RunCancelledViaBatch_CompletesWithoutPollingDelay()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMinutes(1) },
            logger);

        var jobName = "WaitBatchCancel_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var batch = await client.TriggerBatchAsync(jobName, [1], cancellationToken: ct);
        var child = (await store.GetRunsAsync(new() { BatchId = batch.Id }, 0, 10, ct)).Items.Single();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(2));
        var waitTask = client.WaitAsync(child.Id, cts.Token);

        await client.CancelBatchAsync(batch.Id, cts.Token);

        var cancelled = await waitTask;
        Assert.Equal(JobStatus.Cancelled, cancelled.Status);
    }

    [Fact]
    public async Task ObserveRunEventsAsync_EmitsEvents_AndTerminatesOnTerminalStatus()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "ObserveRun_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var triggered = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId = triggered.Id;
        var observedTask = Task.Run(async () =>
        {
            var events = new List<RunEvent>();
            await foreach (var @event in client.ObserveRunEventsAsync(runId, 0, ct))
            {
                events.Add(@event);
            }

            return events;
        }, ct);

        var claimed = await WaitForClaimAsync(store, jobName, ct);
        var now = time.GetUtcNow();
        await store.AppendEventsAsync([
            new()
            {
                RunId = runId,
                EventType = RunEventType.Log,
                Payload = JsonSerializer.Serialize("hello"),
                CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ], ct);
        await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, ct);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            time.GetUtcNow(),
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(7),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True((await store.TryTransitionRunAsync(completed, ct)).Transitioned);
        await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, ct);

        var events = await observedTask;
        Assert.Contains(events, e => e.EventType == RunEventType.Log);
        Assert.Contains(events, RunStatusEvents.IsTerminal);

        var finalRun = await client.GetRunAsync(runId, ct);
        Assert.NotNull(finalRun);
        Assert.Equal(JobStatus.Succeeded, finalRun.Status);
    }

    [Fact]
    public async Task ObserveRunEventsAsync_DrainsPreTerminalOutput_BeforeCompleting()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "ObserveLate_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var triggered = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId = triggered.Id;
        var observedTask = Task.Run(async () =>
        {
            var events = new List<RunEvent>();
            await foreach (var @event in client.ObserveRunEventsAsync(runId, 0, ct))
            {
                events.Add(@event);
            }

            return events;
        }, ct);

        var claimed = await WaitForClaimAsync(store, jobName, ct);
        await store.AppendEventsAsync([
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = "7",
                CreatedAt = time.GetUtcNow(),
                Attempt = claimed.Attempt
            }
        ], ct);
        await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, ct);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            time.GetUtcNow(),
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(7),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True((await store.TryTransitionRunAsync(completed, ct)).Transitioned);
        await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, ct);

        var events = await observedTask;
        Assert.Contains(events, e => e.EventType == RunEventType.Output && e.Payload == "7");
        Assert.Contains(events, RunStatusEvents.IsTerminal);

        var finalRun = await client.GetRunAsync(runId, ct);
        Assert.NotNull(finalRun);
        Assert.Equal(JobStatus.Succeeded, finalRun.Status);
    }

    [Fact]
    public async Task ObserveRunEventsAsync_WaitsForCancelledRun()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "ObserveCancelled_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var triggered = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId = triggered.Id;
        var claimed = await WaitForClaimAsync(store, jobName, ct);

        var observeTask = Task.Run(async () =>
        {
            var events = new List<RunEvent>();
            await foreach (var @event in client.ObserveRunEventsAsync(runId, 0, ct))
            {
                events.Add(@event);
            }

            return events;
        }, ct);

        var cancelResult = await store.TryCancelRunAsync(runId, cancellationToken: ct);
        Assert.True(cancelResult.Transitioned);
        await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, ct);
        await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, ct);

        var events = await observeTask.WaitAsync(TimeSpan.FromSeconds(5), ct);
        Assert.Contains(events, RunStatusEvents.IsTerminal);

        var finalRun = await client.GetRunAsync(runId, ct);
        Assert.NotNull(finalRun);
        Assert.Equal(JobStatus.Cancelled, finalRun.Status);

        var statusEvents =
            await store.GetEventsAsync(runId, 0, [RunEventType.Status], claimed.Attempt, cancellationToken: ct);
        Assert.Contains(statusEvents, RunStatusEvents.IsTerminal);
    }

    [Fact]
    public async Task ObserveRunEventsAsync_MissingRun_ThrowsRunNotFoundException()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        await Assert.ThrowsAsync<RunNotFoundException>(async () =>
        {
            await foreach (var _ in client.ObserveRunEventsAsync(Guid.CreateVersion7().ToString("N"), 0, ct))
            {
            }
        });
    }

    [Fact]
    public async Task CancelAsync_TerminalRun_IsIdempotentNoOp()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "CancelNoOp_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var triggered = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId = triggered.Id;
        var claimed = await WaitForClaimAsync(store, jobName, ct);
        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            time.GetUtcNow(),
            claimed.NotBefore,
            claimed.NodeName,
            1,
            "{}",
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True((await store.TryTransitionRunAsync(completed, ct)).Transitioned);

        await client.CancelAsync(runId, ct);

        var run = await store.GetRunAsync(runId, ct);
        Assert.NotNull(run);
        Assert.Equal(JobStatus.Succeeded, run.Status);
    }

    [Fact]
    public async Task StreamAsync_StreamsOutputFromExistingRun()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "WaitStream_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var triggered = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId = triggered.Id;
        var collectTask = Task.Run(async () =>
        {
            var values = new List<int>();
            await foreach (var value in client.WaitStreamAsync<int>(runId, ct))
            {
                values.Add(value);
            }

            return values;
        }, ct);

        var claimed = await WaitForClaimAsync(store, jobName, ct);
        var now = time.GetUtcNow();
        await store.AppendEventsAsync([
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(3),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(4),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new()
            {
                RunId = runId,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ], ct);
        await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, ct);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            time.GetUtcNow(),
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(new[] { 3, 4 }),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True((await store.TryTransitionRunAsync(completed, ct)).Transitioned);
        await store.AppendEventsAsync(
            [RunStatusEvents.Create(runId, claimed.Attempt, JobStatus.Succeeded, time.GetUtcNow())], ct);
        await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, ct);

        var values = await collectTask;
        Assert.Equal([3, 4], values);
    }

    [Fact]
    public async Task StreamAsync_DrainsAllOutputEvents_WhenRunAlreadyTerminal()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "WaitStreamTerminal_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var triggered = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId = triggered.Id;
        var claimed = await WaitForClaimAsync(store, jobName, ct);

        var now = time.GetUtcNow();
        await store.AppendEventsAsync([
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(1),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(2),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new()
            {
                RunId = runId,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ], ct);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            time.GetUtcNow(),
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(new[] { 1, 2 }),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True((await store.TryTransitionRunAsync(completed, ct)).Transitioned);
        await store.AppendEventsAsync(
            [RunStatusEvents.Create(runId, claimed.Attempt, JobStatus.Succeeded, time.GetUtcNow())], ct);

        var values = new List<int>();
        await foreach (var value in client.WaitStreamAsync<int>(runId, ct))
        {
            values.Add(value);
        }

        Assert.Equal([1, 2], values);
    }

    [Fact]
    public async Task ObserveRunEventsAsync_CanResumeFromCursor()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "ObserveCursor_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var triggered = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId = triggered.Id;
        var claimed = await WaitForClaimAsync(store, jobName, ct);

        var now = time.GetUtcNow();
        await store.AppendEventsAsync([
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(11),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(22),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new()
            {
                RunId = runId,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ], ct);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            time.GetUtcNow(),
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(new[] { 11, 22 }),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True((await store.TryTransitionRunAsync(completed, ct)).Transitioned);
        await store.AppendEventsAsync(
            [RunStatusEvents.Create(runId, claimed.Attempt, JobStatus.Succeeded, time.GetUtcNow())], ct);

        var firstOutputEvent =
            (await store.GetEventsAsync(runId, 0, [RunEventType.Output], cancellationToken: ct)).First();

        var resumed = new List<RunEvent>();
        await foreach (var @event in client.ObserveRunEventsAsync(runId, firstOutputEvent.Id, ct))
        {
            resumed.Add(@event);
        }

        var outputValues = resumed
            .Where(e => e.EventType == RunEventType.Output)
            .Select(e => JsonSerializer.Deserialize<int>(e.Payload))
            .ToArray();

        Assert.Equal([22], outputValues);
        Assert.Contains(resumed, RunStatusEvents.IsTerminal);
    }

    [Fact]
    public async Task StreamAsync_SkipsMalformedOutputPayload_AndContinues()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "WaitStreamMalformed_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var triggered = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId = triggered.Id;
        var claimed = await WaitForClaimAsync(store, jobName, ct);

        var now = time.GetUtcNow();
        await store.AppendEventsAsync([
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = "{ this is not valid json",
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new()
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(42),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new()
            {
                RunId = runId,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ], ct);
        await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, ct);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            time.GetUtcNow(),
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(new[] { 42 }),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True((await store.TryTransitionRunAsync(completed, ct)).Transitioned);
        await store.AppendEventsAsync(
            [RunStatusEvents.Create(runId, claimed.Attempt, JobStatus.Succeeded, time.GetUtcNow())], ct);
        await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, ct);

        var values = new List<int>();
        await foreach (var value in client.WaitStreamAsync<int>(runId, ct))
        {
            values.Add(value);
        }

        Assert.Equal([42], values);
        Assert.NotEmpty(logger.Warnings);
    }

    [Fact]
    public async Task WaitEachAsync_EqualCompletionTimes_YieldsAllChildren()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var now = time.GetUtcNow();
        var jobName = "WaitEachOrdering_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);
        await store.UpsertQueuesAsync([new() { Name = "default" }], ct);

        var batchId = await client.TriggerAllAsync(jobName,
            [new { v = 1 }, new { v = 2 }, new { v = 3 }], ct);

        // Claim and complete all three children at the exact same CompletedAt.
        var completedAt = now.AddSeconds(1);
        var claimed = new List<JobRun>();
        for (var i = 0; i < 3; i++)
        {
            var run = (await store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, ct)).FirstOrDefault();
            Assert.NotNull(run);
            claimed.Add(run);
        }

        foreach (var run in claimed)
        {
            var complete = RunStatusTransition.RunningToSucceeded(
                run.Id, run.Attempt, completedAt, run.NotBefore, run.NodeName,
                1, $"{{\"v\":{run.Id}}}", null, run.StartedAt, run.LastHeartbeatAt);
            var result = await store.TryTransitionRunAsync(complete, ct);
            Assert.True(result.Transitioned);
            if (result.BatchCompletion is { } bc)
            {
                await notifications.PublishAsync(NotificationChannels.BatchTerminated(bc.BatchId), bc.BatchId, ct);
            }
        }

        var orderedIds = new List<string>();
        await foreach (var result in client.WaitEachAsync(batchId, ct))
        {
            orderedIds.Add(result.Id);
        }

        // All children yielded exactly once, in event commit order.
        Assert.Equal(3, orderedIds.Count);
        Assert.Equal(3, orderedIds.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task ObserveBatchEventsAsync_ResumesFromCursor_YieldsNewerEventsOnly()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(15) },
            logger);

        var jobName = "BatchCursor_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var batchId = await client.TriggerAllAsync(jobName, [new { value = 1 }, new { value = 2 }], ct);
        var childA = await WaitForClaimAsync(store, jobName, ct);
        var childB = await WaitForClaimAsync(store, jobName, ct);

        await store.AppendEventsAsync([
            new()
            {
                RunId = childA.Id,
                EventType = RunEventType.Log,
                Payload = "\"a-log\"",
                CreatedAt = time.GetUtcNow(),
                Attempt = childA.Attempt
            },
            new()
            {
                RunId = childB.Id,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(21),
                CreatedAt = time.GetUtcNow(),
                Attempt = childB.Attempt
            },
            new()
            {
                RunId = childA.Id,
                EventType = RunEventType.Progress,
                Payload = "0.5",
                CreatedAt = time.GetUtcNow(),
                Attempt = childA.Attempt
            }
        ], ct);

        var batchEvents = await store.GetBatchEventsAsync(batchId, cancellationToken: ct);
        Assert.Equal(5, batchEvents.Count);

        var completedAt = time.GetUtcNow();
        Assert.True((await store.TryTransitionRunAsync(
            RunStatusTransition.RunningToSucceeded(
                childA.Id,
                childA.Attempt,
                completedAt,
                childA.NotBefore,
                childA.NodeName,
                1,
                null,
                null,
                childA.StartedAt,
                childA.LastHeartbeatAt), ct)).Transitioned);
        var resultB = await store.TryTransitionRunAsync(
            RunStatusTransition.RunningToSucceeded(
                childB.Id,
                childB.Attempt,
                completedAt,
                childB.NotBefore,
                childB.NodeName,
                1,
                JsonSerializer.Serialize(21),
                null,
                childB.StartedAt,
                childB.LastHeartbeatAt), ct);
        Assert.True(resultB.Transitioned);
        Assert.NotNull(resultB.BatchCompletion);
        await notifications.PublishAsync(NotificationChannels.BatchTerminated(resultB.BatchCompletion.BatchId),
            resultB.BatchCompletion.BatchId, ct);

        var resumed = new List<RunEvent>();
        await foreach (var @event in client.ObserveBatchEventsAsync(batchId, batchEvents[0].Id, ct))
        {
            resumed.Add(@event);
        }

        Assert.All(resumed, @event => Assert.True(@event.Id > batchEvents[0].Id));
        Assert.Contains(resumed, @event => @event.RunId == childA.Id && @event.EventType == RunEventType.Log);
        Assert.Contains(resumed, @event => @event.RunId == childB.Id && @event.EventType == RunEventType.Output);
        Assert.Contains(resumed, @event => @event.RunId == childA.Id && @event.EventType == RunEventType.Progress);
        Assert.Contains(resumed, @event =>
            @event.RunId == childA.Id
            && RunStatusEvents.TryGetStatus(@event, out var statusA)
            && statusA == JobStatus.Succeeded);
        Assert.Contains(resumed, @event =>
            @event.RunId == childB.Id
            && RunStatusEvents.TryGetStatus(@event, out var statusB)
            && statusB == JobStatus.Succeeded);
    }

    [Fact]
    public async Task StreamBatchAsync_CapturesOutputBeforeBatchTerminal()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(50) },
            logger);

        var jobName = "BatchLateOutput_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var now = time.GetUtcNow();
        var batchId = Guid.CreateVersion7().ToString("N");
        var childId = Guid.CreateVersion7().ToString("N");

        var batch = new JobBatch
        {
            Id = batchId,
            Status = JobStatus.Running,
            Total = 1,
            CreatedAt = now
        };

        var childRun = new JobRun
        {
            Id = childId,
            BatchId = batchId,
            RootRunId = batchId,
            JobName = jobName,
            Status = JobStatus.Running,
            CreatedAt = now,
            NotBefore = now,
            StartedAt = now,
            Attempt = 1,
            Progress = 0
        };

        await store.CreateBatchAsync(batch, [childRun], cancellationToken: ct);

        var streamTask = Task.Run(async () =>
        {
            var items = new List<int>();
            await foreach (var item in client.WaitEachAsync<int>(batchId, ct))
            {
                items.Add(item);
            }

            return items;
        }, ct);

        await Task.Yield();

        await store.AppendEventsAsync([
            new()
            {
                RunId = childId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(42),
                CreatedAt = time.GetUtcNow(),
                Attempt = 1
            }
        ], ct);
        var result = await store.TryTransitionRunAsync(
            RunStatusTransition.RunningToSucceeded(
                childId,
                1,
                time.GetUtcNow(),
                now,
                null,
                1,
                "42",
                null,
                now,
                now), ct);
        Assert.True(result.Transitioned);
        Assert.NotNull(result.BatchCompletion);
        await notifications.PublishAsync(NotificationChannels.BatchTerminated(result.BatchCompletion.BatchId),
            result.BatchCompletion.BatchId, ct);

        var streamed = await streamTask.WaitAsync(TimeSpan.FromSeconds(5), ct);
        Assert.Single(streamed);
        Assert.Equal(42, streamed[0]);
    }

    [Fact]
    public async Task StreamBatchAsync_CollectionT_HydratesFromOutputEvents_BeforeBatchTerminal()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(50) },
            logger);

        var jobName = "BatchLateOutputAfterEarlier_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var now = time.GetUtcNow();
        var batchId = Guid.CreateVersion7().ToString("N");
        var childId = Guid.CreateVersion7().ToString("N");

        var batch = new JobBatch
        {
            Id = batchId,
            Status = JobStatus.Running,
            Total = 1,
            CreatedAt = now
        };

        var childRun = new JobRun
        {
            Id = childId,
            BatchId = batchId,
            RootRunId = batchId,
            JobName = jobName,
            Status = JobStatus.Running,
            CreatedAt = now,
            NotBefore = now,
            StartedAt = now,
            Attempt = 1,
            Progress = 0
        };

        await store.CreateBatchAsync(
            batch,
            [childRun],
            [
                new()
                {
                    RunId = childId,
                    EventType = RunEventType.Output,
                    Payload = JsonSerializer.Serialize(1),
                    CreatedAt = now,
                    Attempt = 1
                }
            ],
            ct);

        var streamTask = Task.Run(async () =>
        {
            var items = new List<List<int>>();
            await foreach (var item in client.WaitEachAsync<List<int>>(batchId, ct))
            {
                items.Add(item);
            }

            return items;
        }, ct);

        await Task.Yield();

        await store.AppendEventsAsync([
            new()
            {
                RunId = childId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(2),
                CreatedAt = time.GetUtcNow(),
                Attempt = 1
            },
            new()
            {
                RunId = childId,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = time.GetUtcNow(),
                Attempt = 1
            }
        ], ct);
        // NOTE: no final run.Result — the child's collection must hydrate from Output events.
        var result = await store.TryTransitionRunAsync(
            RunStatusTransition.RunningToSucceeded(
                childId,
                1,
                time.GetUtcNow(),
                now,
                null,
                1,
                null,
                null,
                now,
                now), ct);
        Assert.True(result.Transitioned);
        Assert.NotNull(result.BatchCompletion);
        await notifications.PublishAsync(NotificationChannels.BatchTerminated(result.BatchCompletion.BatchId),
            result.BatchCompletion.BatchId, ct);

        var streamed = await streamTask.WaitAsync(TimeSpan.FromSeconds(5), ct);
        Assert.Single(streamed);
        Assert.Equal([1, 2], streamed[0]);
    }

    [Fact]
    public async Task GetRunsAsync_UsesStableUpperBound_ExcludesRunsCreatedAfterEnumerationStarts()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "StableGetRuns_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var baseline = time.GetUtcNow().AddSeconds(-2);
        var initialRuns = Enumerable.Range(0, 3)
            .Select(i => new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = jobName,
                Status = JobStatus.Pending,
                CreatedAt = baseline.AddMilliseconds(i),
                NotBefore = baseline.AddMilliseconds(i),
                Progress = 0
            })
            .ToList();

        await store.CreateRunsAsync(initialRuns, cancellationToken: ct);

        string? lateRunId = null;
        var observed = new List<string>();

        await foreach (var run in client.GetRunsAsync(new()
                       {
                           JobName = jobName
                       }, ct))
        {
            observed.Add(run.Id);

            if (observed.Count == 1)
            {
                var lateRun = new JobRun
                {
                    Id = Guid.CreateVersion7().ToString("N"),
                    JobName = jobName,
                    Status = JobStatus.Pending,
                    CreatedAt = time.GetUtcNow(),
                    NotBefore = time.GetUtcNow(),
                    Progress = 0
                };

                lateRunId = lateRun.Id;
                await store.CreateRunsAsync([lateRun], cancellationToken: ct);
            }
        }

        Assert.NotNull(lateRunId);
        Assert.DoesNotContain(lateRunId, observed);

        foreach (var run in initialRuns)
        {
            Assert.Contains(run.Id, observed);
        }
    }

    [Fact]
    public async Task InMemoryNotificationProvider_PublishAsync_WaitsForSlowSubscriber()
    {
        var ct = TestContext.Current.CancellationToken;
        var provider = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var fastHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await provider.SubscribeAsync("surefire:run:created", async _ => { await Task.Delay(1000); }, ct);

        await provider.SubscribeAsync("surefire:run:created", _ =>
        {
            fastHandler.TrySetResult();
            return Task.CompletedTask;
        }, ct);

        await provider.PublishAsync("surefire:run:created", "msg", ct);

        await fastHandler.Task.WaitAsync(TimeSpan.FromSeconds(1), ct);
    }

    [Fact]
    public async Task TriggerAsync_StreamInput_ContinuesAfterCallerCancellation()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(15) },
            logger);

        var jobName = "StreamAcceptsThenCallerCancels_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var triggered = await client.TriggerAsync(jobName, new
        {
            values = ProduceStream()
        }, cancellationToken: cts.Token);
        var runId = triggered.Id;

        cts.Cancel();

        await TestWait.PollUntilConditionAsync(
            async pollCt =>
            {
                var inputEvents = await store.GetEventsAsync(
                    runId,
                    0,
                    [RunEventType.InputDeclared, RunEventType.Input, RunEventType.InputComplete],
                    0,
                    cancellationToken: pollCt);

                var declaredCount = inputEvents.Count(e => e.EventType == RunEventType.InputDeclared);
                var inputCount = inputEvents.Count(e => e.EventType == RunEventType.Input);
                var completedCount = inputEvents.Count(e => e.EventType == RunEventType.InputComplete);
                return declaredCount == 1 && inputCount == 2 && completedCount == 1;
            },
            TimeSpan.FromSeconds(3),
            TimeSpan.FromMilliseconds(20),
            "Expected both stream items and a single completion envelope after caller cancellation.",
            ct);

        static async IAsyncEnumerable<int> ProduceStream()
        {
            yield return 1;
            await Task.Delay(100);
            yield return 2;
        }
    }

    [Fact]
    public async Task TriggerAsync_StreamOnlyObjectArguments_PersistAsNullArguments()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            eventWriter,
            time,
            new() { PollingInterval = TimeSpan.FromMilliseconds(15) },
            logger);

        var jobName = "StreamOnlyArgs_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName }], ct);

        var triggered = await client.TriggerAsync(jobName, new
        {
            values = ProduceStream()
        }, cancellationToken: ct);
        var runId = triggered.Id;

        var run = await store.GetRunAsync(runId, ct);
        Assert.NotNull(run);
        Assert.Null(run.Arguments);

        static async IAsyncEnumerable<int> ProduceStream()
        {
            yield return 1;
            await Task.Delay(20);
            yield return 2;
        }
    }

    [Fact]
    public async Task TriggerBatchAsync_PopulatesRunIds_OnReturnedBatch()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, eventWriter, time, new(), logger);

        var jobName = "RunIdsBatch_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName }], ct);

        var batch = await client.TriggerBatchAsync(jobName, [new { x = 1 }, new { x = 2 }, new { x = 3 }],
            cancellationToken: ct);

        var runsPage = await store.GetRunsAsync(new() { BatchId = batch.Id }, cancellationToken: ct);
        Assert.Equal(3, runsPage.TotalCount);
        Assert.All(runsPage.Items, r => Assert.False(string.IsNullOrEmpty(r.Id)));
        Assert.Equal(runsPage.Items.Select(r => r.Id).Distinct().Count(), runsPage.Items.Count);
    }

    [Fact]
    public async Task GetBatchAsync_PopulatesRunIds_OnReturnedBatch()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, eventWriter, time, new(), logger);

        var jobName = "RunIdsBatchGet_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName }], ct);

        var triggered = await client.TriggerBatchAsync(jobName, [new { x = 1 }, new { x = 2 }], cancellationToken: ct);
        var fetched = await client.GetBatchAsync(triggered.Id, ct);

        Assert.NotNull(fetched);
        Assert.Equal(2, fetched.Total);
        var runsPage = await store.GetRunsAsync(new() { BatchId = triggered.Id }, cancellationToken: ct);
        Assert.Equal(2, runsPage.TotalCount);
    }

    [Fact]
    public async Task WaitAsync_ReturnsArrayResult_WhenOutputEventsEmittedAndNoJsonResult()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = CreateTime();
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, eventWriter, time, new(), logger);

        var jobName = "ArrayResult_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobsAsync([new() { Name = jobName, Queue = "default" }], ct);

        var triggered = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId = triggered.Id;

        var claimed = await WaitForClaimAsync(store, jobName, ct);
        var now = time.GetUtcNow();
        await store.AppendEventsAsync([
            new()
            {
                RunId = runId, EventType = RunEventType.Output, Payload = JsonSerializer.Serialize(10), CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new()
            {
                RunId = runId, EventType = RunEventType.Output, Payload = JsonSerializer.Serialize(20), CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new()
            {
                RunId = runId, EventType = RunEventType.Output, Payload = JsonSerializer.Serialize(30), CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new()
            {
                RunId = runId, EventType = RunEventType.OutputComplete, Payload = "{}", CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ], ct);

        // Succeed the run with null result — output events must be used to reconstruct the result.
        var succeeded = RunStatusTransition.RunningToSucceeded(
            runId, claimed.Attempt, time.GetUtcNow(), claimed.NotBefore,
            claimed.NodeName, 1, null, null,
            claimed.StartedAt, claimed.LastHeartbeatAt);
        Assert.True((await store.TryTransitionRunAsync(succeeded, ct)).Transitioned);
        await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, ct);

        var result = await client.WaitAsync<int[]>(runId, ct);

        Assert.IsType<int[]>(result);
        Assert.Equal([10, 20, 30], result);
    }

    private static FakeTimeProvider CreateTime() => new(new(2025, 6, 15, 10, 0, 0, TimeSpan.Zero));

    private static async Task<JobRun> WaitForClaimAsync(InMemoryJobStore store, string jobName, CancellationToken ct)
    {
        return await TestWait.PollUntilAsync(
            async pollCt => (await store.ClaimRunsAsync("node-1", [jobName], ["default"], 1, pollCt)).FirstOrDefault(),
            _ => true,
            TimeSpan.FromSeconds(2),
            TimeSpan.FromMilliseconds(10),
            "Run was not claimable in time.",
            ct);
    }

    private sealed class CollectingLogger<T> : ILogger<T>
    {
        public List<string> Warnings { get; } = [];

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel != LogLevel.Warning)
            {
                return;
            }

            Warnings.Add(formatter(state, exception));
        }

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();

            public void Dispose()
            {
            }
        }
    }
}