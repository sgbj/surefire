using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Text.Json;
using Surefire.Tests.Testing;

namespace Surefire.Tests;

public sealed class JobClientContractTests
{
    [Fact]
    public async Task TriggerAsync_UsesJobPriority_WhenRunPriorityNotProvided()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, TimeProvider.System, new SurefireOptions(), logger);

        var jobName = "PriorityDefault_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Priority = 17 });

        var triggered = await client.TriggerAsync(jobName, null, new RunOptions());
        var runId = triggered.Id;

        var run = await store.GetRunAsync(runId);
        Assert.NotNull(run);
        Assert.Equal(17, run.Priority);
        Assert.Empty(logger.Warnings);
    }

    [Fact]
    public async Task TriggerAsync_UsesRunPriorityOverride_WhenProvided()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, TimeProvider.System, new SurefireOptions(), logger);

        var jobName = "PriorityOverride_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Priority = 3 });

        var triggered = await client.TriggerAsync(jobName, null, new RunOptions { Priority = 99 });

        var runId = triggered.Id;

        var run = await store.GetRunAsync(runId);
        Assert.NotNull(run);
        Assert.Equal(99, run.Priority);
        Assert.Empty(logger.Warnings);
    }

    [Fact]
    public async Task TriggerAsync_UnknownJob_LogsWarning_AndUsesDefaultPriority()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, TimeProvider.System, new SurefireOptions(), logger);

        var jobName = "UnknownJob_" + Guid.CreateVersion7().ToString("N");

        var triggered = await client.TriggerAsync(jobName, null, new RunOptions());
        var runId = triggered.Id;

        var run = await store.GetRunAsync(runId);
        Assert.NotNull(run);
        Assert.Equal(0, run.Priority);
        Assert.Single(logger.Warnings);
        Assert.Contains(jobName, logger.Warnings[0], StringComparison.Ordinal);
    }

    [Fact]
    public async Task TriggerAllAsync_WithDeduplicationId_ThrowsArgumentException()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, TimeProvider.System, new SurefireOptions(), logger);

        var jobName = "BatchDedup_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName });

        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.TriggerAllAsync(jobName, [1, 2], new RunOptions { DeduplicationId = "dup" }));
    }

    [Fact]
    public async Task TriggerAsync_Rejected_ThrowsRunConflictException()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, TimeProvider.System, new SurefireOptions(), logger);

        var jobName = "ConflictType_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName });

        // Create first run with dedup id
        await client.TriggerAsync(jobName, null, new RunOptions { DeduplicationId = "unique-1" });

        // Second trigger with same dedup id should throw RunConflictException specifically
        await Assert.ThrowsAsync<RunConflictException>(() =>
            client.TriggerAsync(jobName, null, new RunOptions { DeduplicationId = "unique-1" }));
    }

    [Fact]
    public async Task RunAsync_Cancellation_PropagatesToOwnedRun()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(25) },
            logger);

        var jobName = "CancelOwned_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName });

        using var cts = new CancellationTokenSource();
        var runTask = client.RunAsync(jobName, cancellationToken: cts.Token);

        _ = await TestWait.PollUntilAsync(
            async _ =>
            {
                var runs = await store.GetRunsAsync(new RunFilter { JobName = jobName, ExactJobName = true }, 0, 5);
                return runs.Items.SingleOrDefault();
            },
            run => run is not null,
            TimeSpan.FromSeconds(3),
            TimeSpan.FromMilliseconds(20),
            "Expected RunAsync to create an owned run before cancellation.");

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => runTask);

        var cancelledRun = await TestWait.PollUntilAsync(
            async _ =>
            {
                var runs = await store.GetRunsAsync(new RunFilter { JobName = jobName, ExactJobName = true }, 0, 5);
                return runs.Items.SingleOrDefault();
            },
            run => run.Status == JobStatus.Cancelled,
            TimeSpan.FromSeconds(3),
            TimeSpan.FromMilliseconds(20),
            "Expected RunAsync cancellation to cancel the owned run.");

        Assert.NotNull(cancelledRun.CompletedAt);
        Assert.NotNull(cancelledRun.CancelledAt);
    }

    [Fact]
    public async Task WaitAsync_Cancellation_CancelsWaitOnly()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(25) },
            logger);

        var jobName = "WaitOnly_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName });
        var triggered = await client.TriggerAsync(jobName);
        var runId = triggered.Id;

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(80));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.WaitAsync(runId, cts.Token));

        var run = await store.GetRunAsync(runId);
        Assert.NotNull(run);
        Assert.Equal(JobStatus.Pending, run.Status);
        Assert.Null(run.CancelledAt);
    }

    [Fact]
    public async Task ObserveAsync_EmitsEvents_AndTerminalSnapshot()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "ObserveRun_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var triggered = await client.TriggerAsync(jobName);
        var runId = triggered.Id;
        var observedTask = Task.Run(async () =>
        {
            var observations = new List<RunObservation>();
            await foreach (var observation in client.ObserveAsync(runId))
            {
                observations.Add(observation);
                if (observation.Run.Status.IsTerminal)
                {
                    break;
                }
            }

            return observations;
        });

        var claimed = await WaitForClaimAsync(store, jobName);
        var now = DateTimeOffset.UtcNow;
        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Log,
                Payload = JsonSerializer.Serialize("hello"),
                CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ]);
        await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            DateTimeOffset.UtcNow,
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(7),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completed));
        await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId);

        var observations = await observedTask;
        Assert.Contains(observations, o => o.Event?.EventType == RunEventType.Log);
        Assert.Equal(JobStatus.Succeeded, observations[^1].Run.Status);
    }

    [Fact]
    public async Task ObserveAsync_MissingRun_ThrowsRunNotFoundException()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        await Assert.ThrowsAsync<RunNotFoundException>(async () =>
        {
            await foreach (var _ in client.ObserveAsync(Guid.CreateVersion7().ToString("N")))
            {
            }
        });
    }

    [Fact]
    public async Task CancelAsync_TerminalRun_IsIdempotentNoOp()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "CancelNoOp_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var triggered = await client.TriggerAsync(jobName);
        var runId = triggered.Id;
        var claimed = await WaitForClaimAsync(store, jobName);
        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            DateTimeOffset.UtcNow,
            claimed.NotBefore,
            claimed.NodeName,
            1,
            "{}",
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completed));

        await client.CancelAsync(runId);

        var run = await store.GetRunAsync(runId);
        Assert.NotNull(run);
        Assert.Equal(JobStatus.Succeeded, run.Status);
    }

    [Fact]
    public async Task WaitStreamAsync_StreamsOutputFromExistingRun()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "WaitStream_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var triggered = await client.TriggerAsync(jobName);
        var runId = triggered.Id;
        var collectTask = Task.Run(async () =>
        {
            var values = new List<int>();
            await foreach (var value in client.WaitStreamAsync<int>(runId))
            {
                values.Add(value);
            }

            return values;
        });

        var claimed = await WaitForClaimAsync(store, jobName);
        var now = DateTimeOffset.UtcNow;
        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(3),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(4),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ]);
        await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            DateTimeOffset.UtcNow,
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(new[] { 3, 4 }),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completed));
        await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId);

        var values = await collectTask;
        Assert.Equal([3, 4], values);
    }

    [Fact]
    public async Task WaitStreamAsync_DrainsAllOutputEvents_WhenRunAlreadyTerminal()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "WaitStreamTerminal_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var triggered = await client.TriggerAsync(jobName);
        var runId = triggered.Id;
        var claimed = await WaitForClaimAsync(store, jobName);

        var now = DateTimeOffset.UtcNow;
        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(1),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(2),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ]);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            DateTimeOffset.UtcNow,
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(new[] { 1, 2 }),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completed));

        var values = new List<int>();
        await foreach (var value in client.WaitStreamAsync<int>(runId))
        {
            values.Add(value);
        }

        Assert.Equal([1, 2], values);
    }

    [Fact]
    public async Task ObserveAsync_CanResumeFromCursor()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "ObserveCursor_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var triggered = await client.TriggerAsync(jobName);
        var runId = triggered.Id;
        var claimed = await WaitForClaimAsync(store, jobName);

        var now = DateTimeOffset.UtcNow;
        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(11),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(22),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ]);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            DateTimeOffset.UtcNow,
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(new[] { 11, 22 }),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completed));

        var firstOutputEvent = (await store.GetEventsAsync(runId, 0, [RunEventType.Output], null)).First();

        var resumed = new List<RunObservation>();
        await foreach (var observation in client.ObserveAsync(runId,
                           new RunEventCursor { SinceEventId = firstOutputEvent.Id }))
        {
            resumed.Add(observation);
        }

        var outputValues = resumed
            .Where(o => o.Event?.EventType == RunEventType.Output)
            .Select(o => JsonSerializer.Deserialize<int>(o.Event!.Payload))
            .ToArray();

        Assert.Equal([22], outputValues);
        Assert.NotEmpty(resumed);
        Assert.True(resumed[^1].Run.Status.IsTerminal);
        var lastResumedEventId = resumed.Where(o => o.Event is not null).Max(o => o.Event!.Id);
        Assert.Equal(lastResumedEventId, resumed[^1].Cursor.SinceEventId);
    }

    [Fact]
    public async Task WaitStreamAsync_CanResumeFromCursor()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "WaitStreamCursor_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var triggered = await client.TriggerAsync(jobName);
        var runId = triggered.Id;
        var claimed = await WaitForClaimAsync(store, jobName);

        var now = DateTimeOffset.UtcNow;
        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(5),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(6),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ]);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            DateTimeOffset.UtcNow,
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(new[] { 5, 6 }),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completed));

        var outputEvents = await store.GetEventsAsync(runId, 0, [RunEventType.Output], null);
        var cursor = new RunEventCursor { SinceEventId = outputEvents[0].Id };

        var resumedValues = new List<int>();
        await foreach (var value in client.WaitStreamAsync<int>(runId, cursor))
        {
            resumedValues.Add(value);
        }

        Assert.Equal([6], resumedValues);
    }

    [Fact]
    public async Task WaitStreamAsync_SkipsMalformedOutputPayload_AndContinues()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "WaitStreamMalformed_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var triggered = await client.TriggerAsync(jobName);
        var runId = triggered.Id;
        var claimed = await WaitForClaimAsync(store, jobName);

        var now = DateTimeOffset.UtcNow;
        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = "{ this is not valid json",
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(42),
                CreatedAt = now,
                Attempt = claimed.Attempt
            },
            new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = now,
                Attempt = claimed.Attempt
            }
        ]);
        await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId);

        var completed = RunStatusTransition.RunningToSucceeded(
            runId,
            claimed.Attempt,
            DateTimeOffset.UtcNow,
            claimed.NotBefore,
            claimed.NodeName,
            1,
            JsonSerializer.Serialize(new[] { 42 }),
            null,
            claimed.StartedAt,
            claimed.LastHeartbeatAt);
        Assert.True(await store.TryTransitionRunAsync(completed));
        await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId);

        var values = new List<int>();
        await foreach (var value in client.WaitStreamAsync<int>(runId))
        {
            values.Add(value);
        }

        Assert.Equal([42], values);
        Assert.NotEmpty(logger.Warnings);
    }

    [Fact]
    public async Task WaitEachAsync_EqualCompletionTimes_AreOrderedByRunId()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var now = DateTimeOffset.UtcNow;
        var jobName = "WaitEachOrdering_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var batchId = "batch_ordering_coordinator";
        var batch = new JobBatch
        {
            Id = batchId,
            Status = JobStatus.Succeeded,
            Total = 3,
            Succeeded = 3,
            CreatedAt = now,
            CompletedAt = now
        };

        var completedAt = now.AddSeconds(1);
        var children = new List<JobRun>
        {
            new JobRun
            {
                Id = "child-c",
                BatchId = batchId,
                RootRunId = batchId,
                JobName = jobName,
                Status = JobStatus.Succeeded,
                CreatedAt = now,
                NotBefore = now,
                StartedAt = now,
                CompletedAt = completedAt,
                Attempt = 1,
                Progress = 1,
                Result = "1"
            },
            new JobRun
            {
                Id = "child-a",
                BatchId = batchId,
                RootRunId = batchId,
                JobName = jobName,
                Status = JobStatus.Succeeded,
                CreatedAt = now,
                NotBefore = now,
                StartedAt = now,
                CompletedAt = completedAt,
                Attempt = 1,
                Progress = 1,
                Result = "2"
            },
            new JobRun
            {
                Id = "child-b",
                BatchId = batchId,
                RootRunId = batchId,
                JobName = jobName,
                Status = JobStatus.Succeeded,
                CreatedAt = now,
                NotBefore = now,
                StartedAt = now,
                CompletedAt = completedAt,
                Attempt = 1,
                Progress = 1,
                Result = "3"
            }
        };

        await store.CreateBatchAsync(batch, children);

        var orderedIds = new List<string>();
        await foreach (var result in client.WaitEachAsync(batchId))
        {
            orderedIds.Add(result.Id);
        }

        Assert.Equal(["child-a", "child-b", "child-c"], orderedIds);
    }

    [Fact]
    public async Task StreamEachAsync_ResumesWithoutSkippingOtherChildren_WhenCursorHasPerChildOffsets()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(15) },
            logger);

        var jobName = "BatchCursor_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var batchId = await client.TriggerAllAsync(jobName, [new { value = 1 }, new { value = 2 }]);
        var childA = await WaitForClaimAsync(store, jobName);
        var childB = await WaitForClaimAsync(store, jobName);

        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = childA.Id,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(11),
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childA.Attempt
            },
            new RunEvent
            {
                RunId = childA.Id,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(12),
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childA.Attempt
            },
            new RunEvent
            {
                RunId = childB.Id,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(21),
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = childB.Attempt
            }
        ]);

        var childAEvents = await store.GetEventsAsync(childA.Id, 0, [RunEventType.Output], childA.Attempt);
        Assert.Equal(2, childAEvents.Count);

        var completeA = RunStatusTransition.RunningToSucceeded(
            childA.Id,
            childA.Attempt,
            DateTimeOffset.UtcNow,
            childA.NotBefore,
            childA.NodeName,
            1,
            JsonSerializer.Serialize(12),
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
            JsonSerializer.Serialize(21),
            null,
            childB.StartedAt,
            childB.LastHeartbeatAt);

        Assert.True(await store.TryTransitionRunAsync(completeA));
        Assert.True(await store.TryTransitionRunAsync(completeB));

        await store.TryIncrementBatchProgressAsync(batchId, JobStatus.Succeeded);
        await store.TryIncrementBatchProgressAsync(batchId, JobStatus.Succeeded);
        Assert.True(await store.TryCompleteBatchAsync(batchId, JobStatus.Succeeded, DateTimeOffset.UtcNow));
        await notifications.PublishAsync(NotificationChannels.BatchTerminated(batchId), batchId);

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
        Assert.True(resumed[0].Cursor.ChildSinceEventIds.ContainsKey(childA.Id));
        Assert.True(resumed[0].Cursor.ChildSinceEventIds.ContainsKey(childB.Id));
    }

    [Fact]
    public async Task StreamEachAsync_TerminalCoordinator_GracePoll_CapturesLateOutput()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(50) },
            logger);

        var jobName = "BatchLateOutput_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var now = DateTimeOffset.UtcNow;
        var batchId = Guid.CreateVersion7().ToString("N");
        var childId = Guid.CreateVersion7().ToString("N");

        var batch = new JobBatch
        {
            Id = batchId,
            Status = JobStatus.Succeeded,
            Total = 1,
            Succeeded = 1,
            CreatedAt = now,
            CompletedAt = now
        };

        var childRun = new JobRun
        {
            Id = childId,
            BatchId = batchId,
            RootRunId = batchId,
            JobName = jobName,
            Status = JobStatus.Succeeded,
            CreatedAt = now,
            NotBefore = now,
            StartedAt = now,
            CompletedAt = now,
            Attempt = 1,
            Progress = 1,
            Result = "42"
        };

        await store.CreateBatchAsync(batch, [childRun]);

        var streamTask = Task.Run(async () =>
        {
            var items = new List<BatchStreamItem<int>>();
            await foreach (var item in client.StreamEachAsync<int>(batchId))
            {
                items.Add(item);
            }

            return items;
        });

        await Task.Delay(20);

        await store.AppendEventsAsync([
            new RunEvent
            {
                RunId = childId,
                EventType = RunEventType.Output,
                Payload = JsonSerializer.Serialize(42),
                CreatedAt = DateTimeOffset.UtcNow,
                Attempt = 1
            }
        ]);

        var streamed = await streamTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Single(streamed);
        Assert.Equal(childId, streamed[0].RunId);
        Assert.Equal(42, streamed[0].Item);
    }

    [Fact]
    public async Task GetRunsAsync_UsesStableUpperBound_ExcludesRunsCreatedAfterEnumerationStarts()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(20) },
            logger);

        var jobName = "StableGetRuns_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        var baseline = DateTimeOffset.UtcNow.AddSeconds(-2);
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

        await store.CreateRunsAsync(initialRuns);

        string? lateRunId = null;
        var observed = new List<string>();

        await foreach (var run in client.GetRunsAsync(new RunFilter
                       {
                           JobName = jobName,
                           ExactJobName = true
                       }))
        {
            observed.Add(run.Id);

            if (observed.Count == 1)
            {
                var lateRun = new JobRun
                {
                    Id = Guid.CreateVersion7().ToString("N"),
                    JobName = jobName,
                    Status = JobStatus.Pending,
                    CreatedAt = DateTimeOffset.UtcNow,
                    NotBefore = DateTimeOffset.UtcNow,
                    Progress = 0
                };

                lateRunId = lateRun.Id;
                await store.CreateRunsAsync([lateRun]);
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
        var provider = new InMemoryNotificationProvider();
        var fastHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await provider.SubscribeAsync("surefire:run:created", async _ =>
        {
            await Task.Delay(1000);
        });

        await provider.SubscribeAsync("surefire:run:created", _ =>
        {
            fastHandler.TrySetResult();
            return Task.CompletedTask;
        });

        var started = Stopwatch.StartNew();
        await provider.PublishAsync("surefire:run:created", "msg");
        started.Stop();

        Assert.True(started.Elapsed >= TimeSpan.FromMilliseconds(900),
            $"PublishAsync should await subscriber handlers. Elapsed: {started.Elapsed}.");

        await fastHandler.Task.WaitAsync(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task TriggerAsync_StreamInput_ContinuesAfterCallerCancellation()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(15) },
            logger);

        var jobName = "StreamAcceptsThenCallerCancels_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName, Queue = "default" });

        using var cts = new CancellationTokenSource();
        var triggered = await client.TriggerAsync(jobName, new
        {
            values = ProduceStream()
        }, cancellationToken: cts.Token);
        var runId = triggered.Id;

        cts.Cancel();

        await TestWait.PollUntilConditionAsync(
            async _ =>
            {
                var inputEvents = await store.GetEventsAsync(
                    runId,
                    0,
                    [RunEventType.InputDeclared, RunEventType.Input, RunEventType.InputComplete],
                    0);

                var declaredCount = inputEvents.Count(e => e.EventType == RunEventType.InputDeclared);
                var inputCount = inputEvents.Count(e => e.EventType == RunEventType.Input);
                var completedCount = inputEvents.Count(e => e.EventType == RunEventType.InputComplete);
                return declaredCount == 1 && inputCount == 2 && completedCount == 1;
            },
            TimeSpan.FromSeconds(3),
            TimeSpan.FromMilliseconds(20),
            "Expected both stream items and a single completion envelope after caller cancellation.");

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
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(
            store,
            notifications,
            TimeProvider.System,
            new SurefireOptions { PollingInterval = TimeSpan.FromMilliseconds(15) },
            logger);

        var jobName = "StreamOnlyArgs_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName });

        var triggered = await client.TriggerAsync(jobName, new
        {
            values = ProduceStream()
        });
        var runId = triggered.Id;

        var run = await store.GetRunAsync(runId);
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
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, TimeProvider.System, new SurefireOptions(), logger);

        var jobName = "RunIdsBatch_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName });

        var batch = await ((IJobClient)client).TriggerBatchAsync(jobName, [new { x = 1 }, new { x = 2 }, new { x = 3 }]);

        var runsPage = await store.GetRunsAsync(new() { BatchId = batch.Id });
        Assert.Equal(3, runsPage.TotalCount);
        Assert.All(runsPage.Items, r => Assert.False(string.IsNullOrEmpty(r.Id)));
        Assert.Equal(runsPage.Items.Select(r => r.Id).Distinct().Count(), runsPage.Items.Count);
    }

    [Fact]
    public async Task GetBatchAsync_PopulatesRunIds_OnReturnedBatch()
    {
        var store = new InMemoryJobStore(TimeProvider.System);
        var notifications = new InMemoryNotificationProvider();
        var logger = new CollectingLogger<JobClient>();
        var client = new JobClient(store, notifications, TimeProvider.System, new SurefireOptions(), logger);

        var jobName = "RunIdsBatchGet_" + Guid.CreateVersion7().ToString("N");
        await store.UpsertJobAsync(new JobDefinition { Name = jobName });

        var triggered = await ((IJobClient)client).TriggerBatchAsync(jobName, [new { x = 1 }, new { x = 2 }]);
        var fetched = await ((IJobClient)client).GetBatchAsync(triggered.Id);

        Assert.NotNull(fetched);
        Assert.Equal(2, fetched.Total);
        var runsPage = await store.GetRunsAsync(new() { BatchId = triggered.Id });
        Assert.Equal(2, runsPage.TotalCount);
    }

    private static async Task<JobRun> WaitForClaimAsync(InMemoryJobStore store, string jobName)
    {
        return await TestWait.PollUntilAsync(
            () => store.ClaimRunAsync("node-1", [jobName], ["default"]),
            _ => true,
            TimeSpan.FromSeconds(2),
            TimeSpan.FromMilliseconds(10),
            "Run was not claimable in time.");
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

