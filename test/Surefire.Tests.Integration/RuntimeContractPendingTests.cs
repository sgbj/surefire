using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Surefire.Tests.Testing;
using Xunit.Sdk;

namespace Surefire.Tests.Integration;

public sealed class RuntimeContractPendingTests
{
    [Fact]
    public async Task TriggerAsync_DoesNotApplyJobMaxConcurrencyAtCreateTime()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "TriggerMaxConcurrency_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new()
        {
            Name = jobName,
            Queue = "default",
            MaxConcurrency = 1
        }, ct);

        var runId1 = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId2 = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId3 = await client.TriggerAsync(jobName, cancellationToken: ct);

        Assert.NotEqual(runId1, runId2);
        Assert.NotEqual(runId2, runId3);
        Assert.NotEqual(runId1, runId3);

        var page = await store.GetRunsAsync(new() { JobName = jobName, ExactJobName = true }, 0, 10, ct);
        Assert.Equal(3, page.TotalCount);
        Assert.All(page.Items, r => Assert.Equal(JobStatus.Pending, r.Status));
    }

    [Fact]
    public async Task StreamAsync_RetryBoundary_IsAtLeastOnce_WithAttemptReset()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "StreamRetry_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var marker = Guid.CreateVersion7().ToString("N");
        var streamTask = Task.Run(async () =>
        {
            var items = new List<int>();
            await foreach (var value in client.StreamAsync<int>(jobName, new { marker }))
            {
                items.Add(value);
            }

            return items;
        }, ct);

        var run = await WaitForRunAsync(store, jobName, marker, ct);

        var claimed1 = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimed1);
        Assert.Equal(run.Id, claimed1.Id);

        await store.AppendEventsAsync([
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "1",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = claimed1.Attempt
            }
        ], ct);
        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), cancellationToken: ct);

        var pending = RunStatusTransition.RunningToPending(
            run.Id,
            claimed1.Attempt,
            DateTimeOffset.UtcNow.AddMilliseconds(-1),
            "attempt 1 failed",
            claimed1.Result);
        Assert.True((await store.TryTransitionRunAsync(pending, ct)).Transitioned);

        var claimed2 = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimed2);
        Assert.Equal(2, claimed2.Attempt);

        await store.AppendEventsAsync([
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = "2",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = claimed2.Attempt
            },
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = claimed2.Attempt
            }
        ], ct);

        var completed = RunStatusTransition.RunningToSucceeded(
            run.Id,
            claimed2.Attempt,
            DateTimeOffset.UtcNow,
            claimed2.NotBefore,
            claimed2.NodeName,
            1,
            "2",
            null,
            claimed2.StartedAt,
            claimed2.LastHeartbeatAt);
        Assert.True((await store.TryTransitionRunAsync(completed, ct)).Transitioned);

        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), cancellationToken: ct);
        await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), cancellationToken: ct);

        var streamed = await streamTask;
        Assert.Equal([1, 2], streamed);
    }

    [Fact]
    public async Task WaitEachAsync_ProgressesWithMissedNotifications_ViaPollingFallback()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "BatchWait_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }], ct);

        var waitTask = Task.Run(async () =>
        {
            var ids = new List<string>();
            await foreach (var result in client.WaitEachAsync(batchId))
            {
                ids.Add(result.Id);
            }

            return ids;
        }, ct);

        await Task.Delay(50, ct);

        var claimedA = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimedA);
        var completeA = RunStatusTransition.RunningToSucceeded(
            claimedA.Id,
            claimedA.Attempt,
            DateTimeOffset.UtcNow,
            claimedA.NotBefore,
            claimedA.NodeName,
            1,
            "{\"value\":1}",
            null,
            claimedA.StartedAt,
            claimedA.LastHeartbeatAt);
        Assert.True((await store.TryTransitionRunAsync(completeA, ct)).Transitioned);

        var claimedB = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimedB);
        var completeB = RunStatusTransition.RunningToSucceeded(
            claimedB.Id,
            claimedB.Attempt,
            DateTimeOffset.UtcNow,
            claimedB.NotBefore,
            claimedB.NodeName,
            1,
            "{\"value\":2}",
            null,
            claimedB.StartedAt,
            claimedB.LastHeartbeatAt);
        var resultB = await store.TryTransitionRunAsync(completeB, ct);
        Assert.True(resultB.Transitioned);
        if (resultB.BatchCompletion is { } bcB)
        {
            await notifications.PublishAsync(NotificationChannels.BatchTerminated(bcB.BatchId), bcB.BatchId, ct);
        }

        var completedIds = await waitTask;
        Assert.Equal(2, completedIds.Count);
        Assert.Equal(2, completedIds.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task ObserveAsync_WaitsForTerminalStatusEvent_ViaPollingFallback()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "ObserveFallback_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var run = await client.TriggerAsync(jobName, cancellationToken: ct);
        var runId = run.Id;
        var observeTask = Task.Run(async () =>
        {
            var events = new List<RunEvent>();
            await foreach (var @event in client.ObserveRunEventsAsync(runId, 0, ct))
            {
                events.Add(@event);
            }

            var finalRun = await client.GetRunAsync(runId, ct)
                           ?? throw new InvalidOperationException("Run missing after observation.");
            return (finalRun, events);
        }, ct);

        var claimed = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimed);

        await store.AppendEventsAsync(
        [
            new()
            {
                RunId = runId,
                EventType = RunEventType.Log,
                Payload = JsonSerializer.Serialize(new { Message = "late-final-log" }),
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = claimed.Attempt
            }
        ], ct);

        Assert.True((await store.TryTransitionRunAsync(
            RunStatusTransition.RunningToSucceeded(
                runId,
                claimed.Attempt,
                DateTimeOffset.UtcNow,
                claimed.NotBefore,
                claimed.NodeName,
                1,
                JsonSerializer.Serialize(42),
                null,
                claimed.StartedAt,
                claimed.LastHeartbeatAt), ct)).Transitioned);

        var (finalRun, events) = await observeTask;
        Assert.Equal(JobStatus.Succeeded, finalRun.Status);
        Assert.Contains(events,
            e => e.EventType == RunEventType.Log && e.Payload.Contains("late-final-log", StringComparison.Ordinal));
        Assert.Contains(events, RunStatusEvents.IsTerminal);
    }

    [Fact]
    public async Task TriggerAllAsync_EmptyBatch_CompletesCoordinatorImmediately()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "EmptyBatch_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var batchId = await client.TriggerAllAsync(jobName, Array.Empty<object?>(), ct);
        var batch = await store.GetBatchAsync(batchId, ct);

        Assert.NotNull(batch);
        Assert.Equal(JobStatus.Succeeded, batch.Status);
        Assert.Equal(0, batch.Total);
        Assert.NotNull(batch.CompletedAt);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(3));
        var observed = new List<JobRun>();
        await foreach (var result in client.WaitEachAsync(batchId, cts.Token))
        {
            observed.Add(result);
        }

        Assert.Empty(observed);
    }

    [Fact]
    public async Task TriggerManyAsync_EmptyBatch_CompletesImmediately()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "EmptyBatch_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var newBatch = await client.TriggerBatchAsync(jobName, Array.Empty<object?>(), cancellationToken: ct);
        var batchId = newBatch.Id;
        var batch = await store.GetBatchAsync(batchId, ct);

        Assert.NotNull(batch);
        Assert.Equal(JobStatus.Succeeded, batch.Status);
        Assert.Equal(0, batch.Total);
        Assert.NotNull(batch.CompletedAt);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(3));
        var observed = new List<JobRun>();
        await foreach (var result in client.WaitEachAsync(batchId, cts.Token))
        {
            observed.Add(result);
        }

        Assert.Empty(observed);
    }

    [Fact]
    public async Task WaitEachAsync_DoesNotMissLargeBatchCompletions_WhenNotificationsAreMissed()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "BatchWaitLarge_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var args = Enumerable.Range(1, 260).Select(i => (object?)new { x = i }).ToArray();
        var batchId = await client.TriggerAllAsync(jobName, args, ct);

        var waitTask = Task.Run(async () =>
        {
            var ids = new List<string>();
            await foreach (var result in client.WaitEachAsync(batchId))
            {
                ids.Add(result.Id);
            }

            return ids;
        }, ct);

        await Task.Delay(50, ct);

        for (var i = 0; i < args.Length; i++)
        {
            var claimed = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
            Assert.NotNull(claimed);

            var complete = RunStatusTransition.RunningToSucceeded(
                claimed.Id,
                claimed.Attempt,
                DateTimeOffset.UtcNow,
                claimed.NotBefore,
                claimed.NodeName,
                1,
                "{}",
                null,
                claimed.StartedAt,
                claimed.LastHeartbeatAt);
            var result = await store.TryTransitionRunAsync(complete, ct);
            Assert.True(result.Transitioned);
            if (result.BatchCompletion is { } bc)
            {
                await notifications.PublishAsync(NotificationChannels.BatchTerminated(bc.BatchId), bc.BatchId, ct);
            }
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(10));
        var completedIds = await waitTask.WaitAsync(cts.Token);
        Assert.Equal(args.Length, completedIds.Count);
        Assert.Equal(args.Length, completedIds.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task ObserveBatchEventsAsync_PreservesGlobalOutputOrder()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "BatchStreamOrder_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }], ct);
        var childA = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        var childB = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(childA);
        Assert.NotNull(childB);

        var streamTask = Task.Run(async () =>
        {
            var values = new List<int>();
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(10));
            await foreach (var @event in client.ObserveBatchEventsAsync(batchId, 0, cts.Token))
            {
                if (@event.EventType != RunEventType.Output)
                {
                    continue;
                }

                values.Add(int.Parse(@event.Payload));
                if (values.Count == 4)
                {
                    break;
                }
            }

            return values;
        }, ct);

        await store.AppendEventsAsync([
            new()
            {
                RunId = childB.Id,
                EventType = RunEventType.Output,
                Payload = "200",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childB.Attempt
            },
            new()
            {
                RunId = childA.Id,
                EventType = RunEventType.Output,
                Payload = "100",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childA.Attempt
            },
            new()
            {
                RunId = childB.Id,
                EventType = RunEventType.Output,
                Payload = "201",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childB.Attempt
            },
            new()
            {
                RunId = childA.Id,
                EventType = RunEventType.Output,
                Payload = "101",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childA.Attempt
            }
        ], ct);

        await notifications.PublishAsync(NotificationChannels.RunEvent(batchId), cancellationToken: ct);

        var values = await streamTask;
        Assert.Equal([200, 100, 201, 101], values);
    }

    [Fact]
    public async Task ObserveBatchEventsAsync_PreservesGlobalBatchEventOrder()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "BatchEventOrder_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }], ct);
        var childA = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        var childB = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(childA);
        Assert.NotNull(childB);

        var streamTask = Task.Run(async () =>
        {
            var events = new List<(string RunId, RunEventType EventType, string Payload)>();
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(10));
            await foreach (var @event in client.ObserveBatchEventsAsync(batchId, 0, cts.Token))
            {
                events.Add((@event.RunId, @event.EventType, @event.Payload));
                if (events.Count == 4)
                {
                    break;
                }
            }

            return events;
        }, ct);

        await store.AppendEventsAsync([
            new()
            {
                RunId = childB!.Id,
                EventType = RunEventType.Output,
                Payload = "200",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childB.Attempt
            },
            new()
            {
                RunId = childA!.Id,
                EventType = RunEventType.Log,
                Payload = "\"a-log\"",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childA.Attempt
            },
            new()
            {
                RunId = childB.Id,
                EventType = RunEventType.OutputComplete,
                Payload = "null",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childB.Attempt
            },
            new()
            {
                RunId = childA.Id,
                EventType = RunEventType.Progress,
                Payload = "0.5",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childA.Attempt
            }
        ], ct);

        await notifications.PublishAsync(NotificationChannels.RunEvent(batchId), cancellationToken: ct);

        var events = await streamTask;
        Assert.Equal(
            [
                (childA.Id, RunEventType.Status, ((int)JobStatus.Running).ToString()),
                (childB.Id, RunEventType.Status, ((int)JobStatus.Running).ToString()),
                (childB.Id, RunEventType.Output, "200"),
                (childA.Id, RunEventType.Log, "\"a-log\"")
            ],
            events);
    }

    [Fact]
    public async Task ObserveBatchEventsAsync_LargeBatch_HighVolume_DoesNotMissOrDuplicateEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "BatchEventStress_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        const int childCount = 96;
        const int eventsPerChild = 4;
        var batchId = await client.TriggerAllAsync(jobName,
            Enumerable.Range(0, childCount).Select(i => new { x = i }).ToArray(), ct);

        var claimed = new List<JobRun>(childCount);
        for (var i = 0; i < childCount; i++)
        {
            var run = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
            Assert.NotNull(run);
            claimed.Add(run);
        }

        var expected = new List<(string RunId, string Payload)>(childCount * eventsPerChild);
        var streamTask = Task.Run(async () =>
        {
            var received = new List<(string RunId, string Payload)>(childCount * eventsPerChild);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(10));
            await foreach (var @event in client.ObserveBatchEventsAsync(batchId, 0, cts.Token))
            {
                if (@event.EventType != RunEventType.Output)
                {
                    continue;
                }

                received.Add((@event.RunId, @event.Payload));
                if (received.Count == childCount * eventsPerChild)
                {
                    break;
                }
            }

            return received;
        }, ct);

        var appended = new List<RunEvent>(childCount * eventsPerChild);
        for (var round = 0; round < eventsPerChild; round++)
        {
            for (var i = 0; i < claimed.Count; i++)
            {
                var payload = $"{round:D2}-{i:D3}";
                appended.Add(new()
                {
                    RunId = claimed[i].Id,
                    EventType = RunEventType.Output,
                    Payload = payload,
                    CreatedAt = DateTimeOffset.UtcNow,
                    Attempt = claimed[i].Attempt
                });
                expected.Add((claimed[i].Id, payload));
            }
        }

        await store.AppendEventsAsync(appended, ct);
        await notifications.PublishAsync(NotificationChannels.RunEvent(batchId), batchId, ct);

        var received = await streamTask;
        Assert.Equal(expected.Count, received.Count);
        Assert.Equal(expected, received);
        Assert.Equal(expected.Count, received.Distinct().Count());
    }

    [Fact]
    public async Task ObserveBatchEventsAsync_CapturesLateEventsBeforeCoordinatorTerminal()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(25) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "BatchEventGrace_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }], ct);
        var child = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(child);

        var streamTask = Task.Run(async () =>
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(10));
            await foreach (var @event in client.ObserveBatchEventsAsync(batchId, 0, cts.Token))
            {
                if (@event.EventType != RunEventType.Status)
                {
                    return @event;
                }
            }

            throw new XunitException("Expected a late batch event.");
        }, ct);

        await store.AppendEventsAsync([
            new()
            {
                RunId = child.Id,
                EventType = RunEventType.Output,
                Payload = "123",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = child.Attempt
            }
        ], ct);

        var completedAt = DateTimeOffset.UtcNow;
        var transition = RunStatusTransition.RunningToSucceeded(
            child!.Id,
            child.Attempt,
            completedAt,
            child.NotBefore,
            "node-1",
            startedAt: child.StartedAt ?? completedAt,
            lastHeartbeatAt: child.LastHeartbeatAt);
        var result = await store.TryTransitionRunAsync(transition, ct);
        Assert.True(result.Transitioned);
        if (result.BatchCompletion is { } bc)
        {
            await notifications.PublishAsync(NotificationChannels.BatchTerminated(bc.BatchId), bc.BatchId, ct);
        }

        var @event = await streamTask;
        Assert.Equal(child.Id, @event.RunId);
        Assert.Equal(RunEventType.Output, @event.EventType);
        Assert.Equal("123", @event.Payload);
    }

    [Fact]
    public async Task ObserveBatchEventsAsync_ResumesFromCursor_YieldsOnlyNewerEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "BatchStreamResume_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }], ct);
        var childA = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        var childB = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(childA);
        Assert.NotNull(childB);

        await store.AppendEventsAsync([
            new()
            {
                RunId = childA.Id,
                EventType = RunEventType.Output,
                Payload = "11",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childA.Attempt
            },
            new()
            {
                RunId = childA.Id,
                EventType = RunEventType.Output,
                Payload = "12",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childA.Attempt
            },
            new()
            {
                RunId = childB.Id,
                EventType = RunEventType.Output,
                Payload = "21",
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childB.Attempt
            }
        ], ct);

        var childAEvents =
            await store.GetEventsAsync(childA.Id, 0, [RunEventType.Output], childA.Attempt, cancellationToken: ct);
        Assert.Equal(2, childAEvents.Count);
        var cursorId = childAEvents.Max(e => e.Id);

        var completeA = RunStatusTransition.RunningToSucceeded(
            childA.Id,
            childA.Attempt,
            DateTimeOffset.UtcNow,
            childA.NotBefore,
            childA.NodeName,
            1,
            "12",
            null,
            childA.StartedAt,
            childA.LastHeartbeatAt);
        var completeB = RunStatusTransition.RunningToSucceeded(
            childB.Id,
            childB.Attempt,
            DateTimeOffset.UtcNow,
            childB.NotBefore,
            childB.NodeName,
            1,
            "21",
            null,
            childB.StartedAt,
            childB.LastHeartbeatAt);

        Assert.True((await store.TryTransitionRunAsync(completeA, ct)).Transitioned);
        var resultB = await store.TryTransitionRunAsync(completeB, ct);
        Assert.True(resultB.Transitioned);
        if (resultB.BatchCompletion is { } bc)
        {
            await notifications.PublishAsync(NotificationChannels.BatchTerminated(bc.BatchId), bc.BatchId, ct);
        }

        // Resume at the cursor — should see only events with Id > cursorId, including childB's Output
        // and both children's terminal Status events. Events pre-cursor are skipped.
        var resumedOutputs = new List<(string RunId, string Payload)>();
        await foreach (var @event in client.ObserveBatchEventsAsync(batchId, cursorId, ct))
        {
            if (@event.EventType == RunEventType.Output)
            {
                resumedOutputs.Add((@event.RunId, @event.Payload));
            }
        }

        Assert.Single(resumedOutputs);
        Assert.Equal(childB.Id, resumedOutputs[0].RunId);
        Assert.Equal("21", resumedOutputs[0].Payload);
    }

    [Fact]
    public async Task WaitEachAsync_YieldsChildrenInEventCommitOrder()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "BatchWaitOrder_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }, new { x = 3 }], ct);

        var claimed = new List<JobRun>();
        for (var i = 0; i < 3; i++)
        {
            var run = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
            Assert.NotNull(run);
            claimed.Add(run);
        }

        // Complete in a specific order: child[2], child[0], child[1].
        // Event commit order (which determines yield order) matches this sequence.
        var commitOrder = new[] { claimed[2], claimed[0], claimed[1] };
        var baseTime = DateTimeOffset.UtcNow;

        foreach (var run in commitOrder)
        {
            var complete = RunStatusTransition.RunningToSucceeded(
                run.Id,
                run.Attempt,
                baseTime,
                run.NotBefore,
                run.NodeName,
                1,
                "{}",
                null,
                run.StartedAt,
                run.LastHeartbeatAt);
            var result = await store.TryTransitionRunAsync(complete, ct);
            Assert.True(result.Transitioned);
            if (result.BatchCompletion is { } bc)
            {
                await notifications.PublishAsync(NotificationChannels.BatchTerminated(bc.BatchId), bc.BatchId, ct);
            }
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(10));
        var completedIds = new List<string>();
        await foreach (var result in client.WaitEachAsync(batchId, cts.Token))
        {
            completedIds.Add(result.Id);
        }

        var expected = commitOrder.Select(x => x.Id).ToArray();
        Assert.Equal(expected, completedIds);
    }

    [Fact]
    public async Task WaitEachAsync_FastPath_Terminates_WithoutPollingDelay_WhenAllRunsAlreadyComplete()
    {
        var ct = TestContext.Current.CancellationToken;
        // Verifies the cleaned-up termination logic: once batch is terminal AND we've observed
        // all child runs in a single drain pass, we exit immediately — no extra polling delay.
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromSeconds(30) }, // intentionally large
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" }, ct);
        var jobName = "BatchWaitFastPath_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" }, ct);

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }], ct);

        // Complete BOTH runs and the batch BEFORE WaitEachAsync starts.
        var claimedA = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        var claimedB = await store.ClaimRunAsync("node-1", [jobName], ["default"], ct);
        Assert.NotNull(claimedA);
        Assert.NotNull(claimedB);

        var completedAt = DateTimeOffset.UtcNow;
        foreach (var c in new[] { claimedA, claimedB })
        {
            var transition = RunStatusTransition.RunningToSucceeded(
                c.Id, c.Attempt, completedAt, c.NotBefore, c.NodeName, 1, "{}", null,
                c.StartedAt, c.LastHeartbeatAt);
            Assert.True((await store.TryTransitionRunAsync(transition, ct)).Transitioned);
        }

        // The fast path should not need to wait on the 30s polling interval.
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(2));
        var results = new List<JobRun>();
        var startedAt = Stopwatch.GetTimestamp();
        await foreach (var item in client.WaitEachAsync(batchId, cts.Token))
        {
            results.Add(item);
        }

        var elapsed = Stopwatch.GetElapsedTime(startedAt);

        Assert.Equal(2, results.Count);
        Assert.True(elapsed < TimeSpan.FromSeconds(1),
            $"WaitEachAsync should exit on fast path; took {elapsed.TotalMilliseconds}ms.");
    }

    private static async Task<JobRun> WaitForRunAsync(IJobStore store, string jobName, string marker,
        CancellationToken ct = default)
    {
        return await TestWait.PollUntilAsync(
            async _ =>
            {
                var page = await store.GetRunsAsync(new() { JobName = jobName, ExactJobName = true },
                    cancellationToken: ct);
                return page.Items.FirstOrDefault(r =>
                    r.Arguments is { } args && args.Contains(marker, StringComparison.Ordinal));
            },
            _ => true,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(10),
            "Timed out waiting for run creation.",
            ct);
    }
}