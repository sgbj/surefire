using Microsoft.Extensions.Logging.Abstractions;
using Surefire.Tests.Testing;

namespace Surefire.Tests.Integration;

public sealed class RuntimeContractPendingTests
{
    [Fact]
    public async Task TriggerAsync_DoesNotApplyJobMaxConcurrencyAtCreateTime()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" });
        var jobName = "TriggerMaxConcurrency_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new()
        {
            Name = jobName,
            Queue = "default",
            MaxConcurrency = 1
        });

        var runId1 = await client.TriggerAsync(jobName);
        var runId2 = await client.TriggerAsync(jobName);
        var runId3 = await client.TriggerAsync(jobName);

        Assert.NotEqual(runId1, runId2);
        Assert.NotEqual(runId2, runId3);
        Assert.NotEqual(runId1, runId3);

        var page = await store.GetRunsAsync(new() { JobName = jobName, ExactJobName = true }, 0, 10);
        Assert.Equal(3, page.TotalCount);
        Assert.All(page.Items, r => Assert.Equal(JobStatus.Pending, r.Status));
    }

    [Fact]
    public async Task StreamAsync_RetryBoundary_IsAtLeastOnce_WithAttemptReset()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" });
        var jobName = "StreamRetry_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" });

        var marker = Guid.CreateVersion7().ToString("N");
        var streamTask = Task.Run(async () =>
        {
            var items = new List<int>();
            await foreach (var value in client.StreamAsync<int>(jobName, new { marker }))
            {
                items.Add(value);
            }

            return items;
        });

        var run = await WaitForRunAsync(store, jobName, marker);

        var claimed1 = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
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
        ]);
        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id));

        var retrying = RunStatusTransition.RunningToRetrying(
            run.Id,
            claimed1.Attempt,
            DateTimeOffset.UtcNow,
            claimed1.NodeName,
            claimed1.Progress,
            "attempt 1 failed",
            claimed1.Result,
            claimed1.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(retrying));

        var pending = RunStatusTransition.RetryingToPending(
            run.Id,
            claimed1.Attempt,
            DateTimeOffset.UtcNow.AddMilliseconds(-1),
            null,
            claimed1.Progress,
            claimed1.Error,
            claimed1.Result);
        Assert.True(await store.TryTransitionRunAsync(pending));

        var claimed2 = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
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
        ]);

        var completed = RunStatusTransition.RunningToCompleted(
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
        Assert.True(await store.TryTransitionRunAsync(completed));

        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id));
        await notifications.PublishAsync(NotificationChannels.RunCompleted(run.Id));

        var streamed = await streamTask;
        Assert.Equal([1, 2], streamed);
    }

    [Fact]
    public async Task WaitEachAsync_ProgressesWithMissedNotifications_ViaPollingFallback()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" });
        var jobName = "BatchWait_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" });

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }]);

        var waitTask = Task.Run(async () =>
        {
            var ids = new List<string>();
            await foreach (var result in client.WaitEachAsync(batchId))
            {
                ids.Add(result.RunId);
            }

            return ids;
        });

        await Task.Delay(50);

        var claimedA = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(claimedA);
        var completeA = RunStatusTransition.RunningToCompleted(
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
        Assert.True(await store.TryTransitionRunAsync(completeA));
        await store.TryIncrementBatchCounterAsync(batchId, false);

        var claimedB = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(claimedB);
        var completeB = RunStatusTransition.RunningToCompleted(
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
        Assert.True(await store.TryTransitionRunAsync(completeB));
        await store.TryIncrementBatchCounterAsync(batchId, false);

        var coordinator = await store.GetRunAsync(batchId);
        Assert.NotNull(coordinator);
        var completeCoordinator = RunStatusTransition.RunningToCompleted(
            batchId,
            coordinator.Attempt,
            DateTimeOffset.UtcNow,
            coordinator.NotBefore,
            coordinator.NodeName,
            1,
            null,
            null,
            coordinator.StartedAt,
            coordinator.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completeCoordinator));

        var completedIds = await waitTask;
        Assert.Equal(2, completedIds.Count);
        Assert.Equal(2, completedIds.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task TriggerAllAsync_EmptyBatch_CompletesCoordinatorImmediately()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" });
        var jobName = "EmptyBatch_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" });

        var batchId = await client.TriggerAllAsync(jobName, Array.Empty<object?>());
        var coordinator = await store.GetRunAsync(batchId);

        Assert.NotNull(coordinator);
        Assert.Equal(JobStatus.Completed, coordinator.Status);
        Assert.Equal(0, coordinator.BatchTotal);
        Assert.Equal(1, coordinator.Progress);
        Assert.NotNull(coordinator.CompletedAt);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var observed = new List<RunResult>();
        await foreach (var result in client.WaitEachAsync(batchId, cts.Token))
        {
            observed.Add(result);
        }

        Assert.Empty(observed);
    }

    [Fact]
    public async Task RerunAsync_EmptyBatch_CompletesCoordinatorImmediately()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" });
        var jobName = "EmptyBatchRerun_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" });

        var originalBatchId = await client.TriggerAllAsync(jobName, Array.Empty<object?>());
        var rerunBatchId = await client.RerunAsync(originalBatchId);
        var coordinator = await store.GetRunAsync(rerunBatchId);

        Assert.NotNull(coordinator);
        Assert.Equal(JobStatus.Completed, coordinator.Status);
        Assert.Equal(0, coordinator.BatchTotal);
        Assert.Equal(1, coordinator.Progress);
        Assert.NotNull(coordinator.CompletedAt);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var observed = new List<RunResult>();
        await foreach (var result in client.WaitEachAsync(rerunBatchId, cts.Token))
        {
            observed.Add(result);
        }

        Assert.Empty(observed);
    }

    [Fact]
    public async Task WaitEachAsync_DoesNotMissLargeBatchCompletions_WhenNotificationsAreMissed()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" });
        var jobName = "BatchWaitLarge_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" });

        var args = Enumerable.Range(1, 260).Select(i => (object?)new { x = i }).ToArray();
        var batchId = await client.TriggerAllAsync(jobName, args);

        var waitTask = Task.Run(async () =>
        {
            var ids = new List<string>();
            await foreach (var result in client.WaitEachAsync(batchId))
            {
                ids.Add(result.RunId);
            }

            return ids;
        });

        await Task.Delay(50);

        for (var i = 0; i < args.Length; i++)
        {
            var claimed = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
            Assert.NotNull(claimed);

            var complete = RunStatusTransition.RunningToCompleted(
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
            Assert.True(await store.TryTransitionRunAsync(complete));
            await store.TryIncrementBatchCounterAsync(batchId, false);
        }

        var coordinator = await store.GetRunAsync(batchId);
        Assert.NotNull(coordinator);
        var completeCoordinator = RunStatusTransition.RunningToCompleted(
            batchId,
            coordinator.Attempt,
            DateTimeOffset.UtcNow,
            coordinator.NotBefore,
            coordinator.NodeName,
            1,
            null,
            null,
            coordinator.StartedAt,
            coordinator.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completeCoordinator));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var completedIds = await waitTask.WaitAsync(cts.Token);
        Assert.Equal(args.Length, completedIds.Count);
        Assert.Equal(args.Length, completedIds.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task StreamEachAsync_PreservesGlobalBatchOutputEventOrder()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" });
        var jobName = "BatchStreamOrder_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" });

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }]);
        var childA = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
        var childB = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(childA);
        Assert.NotNull(childB);

        var streamTask = Task.Run(async () =>
        {
            var values = new List<int>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await foreach (var item in client.StreamEachAsync<int>(batchId, cts.Token))
            {
                values.Add(item.Item);
                if (values.Count == 4)
                {
                    break;
                }
            }

            return values;
        });

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
        ]);

        await notifications.PublishAsync(NotificationChannels.RunEvent(batchId));

        var values = await streamTask;
        Assert.Equal([200, 100, 201, 101], values);
    }

    [Fact]
    public async Task StreamEachAsync_ResumesWithoutSkippingOtherChildren_AfterCoordinatorTerminal()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" });
        var jobName = "BatchStreamResume_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" });

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }]);
        var childA = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
        var childB = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
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
        ]);

        var childAEvents = await store.GetEventsAsync(childA.Id, 0, [RunEventType.Output], childA.Attempt);
        Assert.Equal(2, childAEvents.Count);

        var completeA = RunStatusTransition.RunningToCompleted(
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
        var completeB = RunStatusTransition.RunningToCompleted(
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

        Assert.True(await store.TryTransitionRunAsync(completeA));
        Assert.True(await store.TryTransitionRunAsync(completeB));
        await store.TryIncrementBatchCounterAsync(batchId, false);
        await store.TryIncrementBatchCounterAsync(batchId, false);

        var coordinator = await store.GetRunAsync(batchId);
        Assert.NotNull(coordinator);
        var completeCoordinator = RunStatusTransition.RunningToCompleted(
            batchId,
            coordinator.Attempt,
            DateTimeOffset.UtcNow,
            coordinator.NotBefore,
            coordinator.NodeName,
            1,
            null,
            null,
            coordinator.StartedAt,
            coordinator.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completeCoordinator));

        var resumeCursor = new BatchRunEventCursor
        {
            SinceEventId = 0,
            ChildSinceEventIds = new Dictionary<string, long>(StringComparer.Ordinal)
            {
                [childA.Id] = childAEvents.Max(e => e.Id)
            }
        };

        var resumed = new List<BatchStreamItem<int>>();
        await foreach (var item in client.StreamEachAsync<int>(batchId, resumeCursor))
        {
            resumed.Add(item);
        }

        Assert.Single(resumed);
        Assert.Equal(childB.Id, resumed[0].RunId);
        Assert.Equal(21, resumed[0].Item);
    }

    [Fact]
    public async Task WaitEachAsync_DoesNotExitEarly_WhenCoordinatorCompletesBeforeAllChildrenObserved()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" });
        var jobName = "BatchWaitTerminal_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" });

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }, new { x = 3 }]);
        var claimed = new List<JobRun>();
        for (var i = 0; i < 3; i++)
        {
            var run = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
            Assert.NotNull(run);
            claimed.Add(run);
        }

        var waitTask = Task.Run(async () =>
        {
            var ids = new List<string>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await foreach (var result in client.WaitEachAsync(batchId, cts.Token))
            {
                ids.Add(result.RunId);
            }

            return ids;
        });

        var first = claimed[0];
        var completeFirst = RunStatusTransition.RunningToCompleted(
            first.Id,
            first.Attempt,
            DateTimeOffset.UtcNow,
            first.NotBefore,
            first.NodeName,
            1,
            "{}",
            null,
            first.StartedAt,
            first.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completeFirst));
        await store.TryIncrementBatchCounterAsync(batchId, false);

        var coordinator = await store.GetRunAsync(batchId);
        Assert.NotNull(coordinator);
        var completeCoordinator = RunStatusTransition.RunningToCompleted(
            batchId,
            coordinator.Attempt,
            DateTimeOffset.UtcNow,
            coordinator.NotBefore,
            coordinator.NodeName,
            1,
            null,
            null,
            coordinator.StartedAt,
            coordinator.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completeCoordinator));

        await Task.Delay(50);

        foreach (var run in claimed.Skip(1))
        {
            var complete = RunStatusTransition.RunningToCompleted(
                run.Id,
                run.Attempt,
                DateTimeOffset.UtcNow,
                run.NotBefore,
                run.NodeName,
                1,
                "{}",
                null,
                run.StartedAt,
                run.LastHeartbeatAt);
            Assert.True(await store.TryTransitionRunAsync(complete));
            await store.TryIncrementBatchCounterAsync(batchId, false);
        }

        var completedIds = await waitTask;
        Assert.Equal(3, completedIds.Count);
        Assert.Equal(3, completedIds.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public async Task WaitEachAsync_YieldsChildrenInCompletionOrder()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" });
        var jobName = "BatchWaitOrder_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" });

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }, new { x = 3 }]);

        var waitTask = Task.Run(async () =>
        {
            var ids = new List<string>();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await foreach (var result in client.WaitEachAsync(batchId, cts.Token))
            {
                ids.Add(result.RunId);
            }

            return ids;
        });

        var claimed = new List<JobRun>();
        for (var i = 0; i < 3; i++)
        {
            var run = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
            Assert.NotNull(run);
            claimed.Add(run);
        }

        var baseTime = DateTimeOffset.UtcNow;
        var completionPlan = new[]
        {
            (Run: claimed[0], CompletedAt: baseTime.AddMilliseconds(20)),
            (Run: claimed[1], CompletedAt: baseTime.AddMilliseconds(10)),
            (Run: claimed[2], CompletedAt: baseTime.AddMilliseconds(30))
        };

        foreach (var (run, completedAt) in completionPlan.Reverse())
        {
            var complete = RunStatusTransition.RunningToCompleted(
                run.Id,
                run.Attempt,
                completedAt,
                run.NotBefore,
                run.NodeName,
                1,
                "{}",
                null,
                run.StartedAt,
                run.LastHeartbeatAt);
            Assert.True(await store.TryTransitionRunAsync(complete));
            await store.TryIncrementBatchCounterAsync(batchId, false);
        }

        var coordinator = await store.GetRunAsync(batchId);
        Assert.NotNull(coordinator);
        var completeCoordinator = RunStatusTransition.RunningToCompleted(
            batchId,
            coordinator.Attempt,
            baseTime.AddMilliseconds(40),
            coordinator.NotBefore,
            coordinator.NodeName,
            1,
            null,
            null,
            coordinator.StartedAt,
            coordinator.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completeCoordinator));

        var completedIds = await waitTask;
        var expected = completionPlan
            .OrderBy(x => x.CompletedAt)
            .Select(x => x.Run.Id)
            .ToArray();
        Assert.Equal(expected, completedIds);
    }

    [Fact]
    public async Task WaitEachAsync_Terminates_WhenCoordinatorCountersReachBatchTotal()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var client = new JobClient(store, notifications, TimeProvider.System,
            new() { PollingInterval = TimeSpan.FromMilliseconds(10) },
            NullLogger<JobClient>.Instance);

        await store.UpsertQueueAsync(new() { Name = "default" });
        var jobName = "BatchWaitCounters_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new() { Name = jobName, Queue = "default" });

        var batchId = await client.TriggerAllAsync(jobName, [new { x = 1 }, new { x = 2 }]);

        var claimedA = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
        var claimedB = await store.ClaimRunAsync("node-1", [jobName], ["default"]);
        Assert.NotNull(claimedA);
        Assert.NotNull(claimedB);

        var completedAt = DateTimeOffset.UtcNow;
        var completeA = RunStatusTransition.RunningToCompleted(
            claimedA.Id,
            claimedA.Attempt,
            completedAt,
            claimedA.NotBefore,
            claimedA.NodeName,
            1,
            "{}",
            null,
            claimedA.StartedAt,
            claimedA.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completeA));
        await store.TryIncrementBatchCounterAsync(batchId, false);
        await store.TryIncrementBatchCounterAsync(batchId, false);

        var coordinator = await store.GetRunAsync(batchId);
        Assert.NotNull(coordinator);
        var completeCoordinator = RunStatusTransition.RunningToCompleted(
            batchId,
            coordinator.Attempt,
            completedAt.AddMilliseconds(10),
            coordinator.NotBefore,
            coordinator.NodeName,
            1,
            null,
            null,
            coordinator.StartedAt,
            coordinator.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completeCoordinator));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var results = new List<RunResult>();
        await foreach (var item in client.WaitEachAsync(batchId, cts.Token))
        {
            results.Add(item);
        }

        Assert.Single(results);
        Assert.Equal(claimedA.Id, results[0].RunId);
    }

    private static async Task<JobRun> WaitForRunAsync(IJobStore store, string jobName, string marker)
    {
        return await TestWait.PollUntilAsync(
            async _ =>
            {
                var page = await store.GetRunsAsync(new() { JobName = jobName, ExactJobName = true });
                return page.Items.FirstOrDefault(r =>
                    r.Arguments is { } args && args.Contains(marker, StringComparison.Ordinal));
            },
            _ => true,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(10),
            "Timed out waiting for run creation.");
    }
}