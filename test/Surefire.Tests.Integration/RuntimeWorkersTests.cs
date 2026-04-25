using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Surefire.Tests.Testing;

namespace Surefire.Tests.Integration;

public sealed class RuntimeWorkersTests
{
    [Fact]
    public async Task Executor_Completes_Triggered_Run()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Echo", (string value) => value);
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Echo", new { value = "ok" }, cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(10));
        Assert.Equal("ok", await harness.Client.WaitAsync<string>(run.Id, cts.Token));
    }

    [Fact]
    public async Task Waited_Run_Uses_Configured_SerializerOptions_For_GetResult()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
            options.SerializerOptions = new(JsonSerializerDefaults.Web)
            {
                Converters = { new JsonStringEnumConverter() }
            };
        });

        harness.Host.AddJob("EnumEcho", () => DayOfWeek.Wednesday);
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("EnumEcho", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        Assert.Equal(DayOfWeek.Wednesday, await harness.Client.WaitAsync<DayOfWeek>(run.Id, cts.Token));
    }

    [Fact]
    public async Task Executor_Retries_Then_Completes()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        var attempts = 0;
        harness.Host.AddJob("Flaky", () =>
        {
            if (Interlocked.Increment(ref attempts) == 1)
            {
                throw new InvalidOperationException("first attempt fails");
            }

            return 42;
        }).WithRetry(policy =>
        {
            policy.MaxRetries = 1;
            policy.InitialDelay = TimeSpan.FromMilliseconds(10);
            policy.MaxDelay = TimeSpan.FromMilliseconds(10);
            policy.Jitter = false;
        });

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Flaky", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        Assert.Equal(42, await harness.Client.WaitAsync<int>(run.Id, cts.Token));

        var runRecord = await harness.Store.GetRunAsync(run.Id, cts.Token);
        Assert.NotNull(runRecord);
        Assert.Equal(2, runRecord.Attempt);
    }

    [Fact]
    public async Task Executor_NonGenericTaskResult_IsStoredAsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("NoResultTask", async () => { await Task.Delay(1); });
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("NoResultTask", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<InvalidOperationException>(() => harness.Client.WaitAsync<object?>(run.Id, cts.Token));

        var runRecord = await harness.Store.GetRunAsync(run.Id, cts.Token);
        Assert.NotNull(runRecord);
        Assert.Equal(JobStatus.Succeeded, runRecord.Status);
        Assert.Null(runRecord.Result);
    }

    [Fact]
    public async Task Executor_ExplicitNullResult_IsSerializedAsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("ExplicitNull", () => (object?)null);
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("ExplicitNull", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        Assert.Null(await harness.Client.WaitAsync<object?>(run.Id, cts.Token));

        var runRecord = await harness.Store.GetRunAsync(run.Id, cts.Token);
        Assert.NotNull(runRecord);
        Assert.Equal("null", runRecord.Result);
    }

    [Fact]
    public async Task Executor_EmptyObjectResult_IsSerializedAsObject()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("EmptyObject", () => new { });
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("EmptyObject", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        var typed = await harness.Client.WaitAsync<JsonElement>(run.Id, cts.Token);
        Assert.Equal(JsonValueKind.Object, typed.ValueKind);

        var runRecord = await harness.Store.GetRunAsync(run.Id, cts.Token);
        Assert.NotNull(runRecord);
        Assert.Equal("{}", runRecord.Result);
    }

    [Fact]
    public async Task Scheduler_FireAll_Enqueues_Missed_Occurrences()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(100);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("CronFireAll", () => 1)
            .WithCron("* * * * * *")
            .WithMisfirePolicy(MisfirePolicy.FireAll);

        await harness.StartAsync(ct);

        await harness.Store.UpdateLastCronFireAtAsync("CronFireAll", DateTimeOffset.UtcNow.AddSeconds(-3), ct);

        await TestWait.PollUntilAsync(
            async _ => (PagedResult<JobRun>?)await harness.Store.GetRunsAsync(new()
            {
                JobName = "CronFireAll"
            }, cancellationToken: ct),
            runsPage => runsPage.TotalCount >= 2,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(30),
            "Expected scheduler with FireAll misfire policy to enqueue at least two runs.",
            ct);
    }

    [Fact]
    public async Task Scheduler_FireAll_WithCatchUpCap_ReplaysBacklogAcrossTicks()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(900);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("CronFireAllBounded", () => 1)
            .WithCron("* * * * * *")
            .WithMisfirePolicy(MisfirePolicy.FireAll, 1);

        await harness.StartAsync(ct);

        await harness.Store.UpdateLastCronFireAtAsync("CronFireAllBounded", DateTimeOffset.UtcNow.AddSeconds(-3), ct);

        var firstPage = await TestWait.PollUntilAsync(
            async _ => (PagedResult<JobRun>?)await harness.Store.GetRunsAsync(new()
            {
                JobName = "CronFireAllBounded"
            }, cancellationToken: ct),
            runsPage => runsPage.TotalCount >= 1,
            TimeSpan.FromSeconds(4),
            TimeSpan.FromMilliseconds(25),
            "Expected scheduler to enqueue the first FireAll catch-up run.",
            ct);

        Assert.Equal(1, firstPage.TotalCount);

        await TestWait.PollUntilAsync(
            async _ => (PagedResult<JobRun>?)await harness.Store.GetRunsAsync(new()
            {
                JobName = "CronFireAllBounded"
            }, cancellationToken: ct),
            runsPage => runsPage.TotalCount >= 3,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(50),
            "Expected capped FireAll catch-up to continue replaying remaining missed occurrences on later ticks.",
            ct);
    }

    [Fact]
    public async Task Scheduler_NewCronJob_WithNullLastCronFireAt_StartsScheduling()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(50);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("CronInitialSeed", () => 1)
            .WithCron("* * * * * *")
            .WithMisfirePolicy(MisfirePolicy.FireOnce);

        await harness.StartAsync(ct);

        await TestWait.PollUntilAsync(
            async _ => (PagedResult<JobRun>?)await harness.Store.GetRunsAsync(new()
            {
                JobName = "CronInitialSeed"
            }, 0, 10, ct),
            runsPage => runsPage.TotalCount > 0,
            TimeSpan.FromSeconds(4),
            TimeSpan.FromMilliseconds(40),
            "Expected new cron job to start scheduling when LastCronFireAt was initially null.",
            ct);
    }

    [Fact]
    public async Task Maintenance_Updates_Heartbeat_For_Active_Run()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Slow", async (CancellationToken ct) =>
        {
            await Task.Delay(800, ct);
            return 1;
        });

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Slow", cancellationToken: ct);
        var runId = run.Id;
        var first = await WaitForStatusAsync(harness, runId, JobStatus.Running, ct);
        var hb1 = first.LastHeartbeatAt;
        Assert.NotNull(hb1);

        var updated = await TestWait.PollUntilAsync(
            () => harness.Store.GetRunAsync(runId, ct),
            run => run.Status == JobStatus.Running
                   && run.LastHeartbeatAt is { } latestHeartbeat
                   && latestHeartbeat > hb1.Value,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            "Expected heartbeat to advance while run is active.",
            ct);

        Assert.Equal(JobStatus.Running, updated.Status);
        Assert.NotNull(updated.LastHeartbeatAt);
        Assert.True(updated.LastHeartbeatAt > hb1);
    }

    [Fact]
    public async Task ExternalStoreCancellation_CancelsRunningRun_WithoutNotification()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(25);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Spin", async (CancellationToken ct) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return 0;
        });

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Spin", cancellationToken: ct);
        var runId = run.Id;
        await WaitForStatusAsync(harness, runId, JobStatus.Running, ct);

        var cancelResult = await harness.Store.TryCancelRunAsync(runId, cancellationToken: ct);
        Assert.True(cancelResult.Transitioned);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        run = await harness.Client.WaitAsync(run.Id, cts.Token);
        Assert.True(run.IsCancelled);
    }

    [Fact]
    public async Task RunAsync_Cancellation_PropagatesToRunExecution()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("CancelableRun", async (CancellationToken ct) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return 0;
        });

        await harness.StartAsync(ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromMilliseconds(120));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            harness.Client.RunAsync("CancelableRun", cancellationToken: cts.Token));

        await TestWait.PollUntilAsync(
            async _ =>
            {
                var page = await harness.Store.GetRunsAsync(new()
                {
                    JobName = "CancelableRun"
                }, 0, 1, ct);

                return page.Items.SingleOrDefault();
            },
            run => run.Status == JobStatus.Cancelled,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(25),
            "Expected RunAsync cancellation to propagate and cancel execution.",
            ct);
    }

    [Fact]
    public async Task WaitAsync_Cancellation_DoesNotCancelRunExecution()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("WaitObserverOnly", async (CancellationToken ct) =>
        {
            await Task.Delay(300, ct);
            return 123;
        });

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("WaitObserverOnly", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromMilliseconds(70));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => harness.Client.WaitAsync(run.Id, cts.Token));

        using var completionCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        completionCts.CancelAfter(TimeSpan.FromSeconds(5));
        Assert.Equal(123, await harness.Client.WaitAsync<int>(run.Id, completionCts.Token));
    }

    [Fact]
    public async Task CancelBatchAsync_CancelsChildren()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SlowBatch", async (CancellationToken ct) =>
        {
            await Task.Delay(1000, ct);
            return 1;
        });

        await harness.StartAsync(ct);

        var batchId = await ((JobClient)harness.Client).TriggerAllAsync(
            "SlowBatch",
            [new { x = 1 }, new { x = 2 }, new { x = 3 }],
            ct);
        await harness.Client.CancelBatchAsync(batchId, ct);

        // CancelBatchAsync transitions Running children to Cancelling (not Cancelled).
        // The executor finalizes Cancelling → Cancelled asynchronously. Poll until all
        // children reach Cancelled rather than asserting immediately.
        await TestWait.PollUntilAsync(
            async _ =>
            {
                var page = await harness.Store.GetRunsAsync(new() { BatchId = batchId }, 0, 10, ct);
                return page.Items;
            },
            items => items.Count > 0 && items.All(r => r.Status == JobStatus.Cancelled),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for all batch children to reach Cancelled status.",
            ct);

        var batch = await harness.Client.GetBatchAsync(batchId, ct);
        Assert.NotNull(batch);
        Assert.True(batch.Status == JobStatus.Cancelled || batch.Status == JobStatus.Failed);
    }

    [Fact]
    public async Task StreamAsync_FallsBackToResultJsonList_WhenNoOutputEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("ResultList", () => new List<int> { 3, 4, 5 });
        await harness.StartAsync(ct);

        var values = new List<int>();
        await foreach (var item in harness.Client.StreamAsync<int>("ResultList", cancellationToken: ct))
        {
            values.Add(item);
        }

        Assert.Equal([3, 4, 5], values);
    }

    [Fact]
    public async Task ObserveBatchEventsAsync_StreamsChildOutputItems()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("BatchStream", BatchStream);
        await harness.StartAsync(ct);

        var batchId = await ((JobClient)harness.Client).TriggerAllAsync("BatchStream", [
            new { start = 1 },
            new { start = 10 }
        ], ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        var outputs = new List<(string RunId, int Item)>();
        await foreach (var @event in harness.Client.ObserveBatchEventsAsync(batchId, 0, cts.Token))
        {
            if (@event.EventType != RunEventType.Output)
            {
                continue;
            }

            outputs.Add((@event.RunId, JsonSerializer.Deserialize<int>(@event.Payload)));
        }

        Assert.Equal(4, outputs.Count);
        Assert.Equal([1, 2, 10, 11], outputs.Select(i => i.Item).OrderBy(v => v).ToArray());
        Assert.Equal(2, outputs.Select(i => i.RunId).Distinct(StringComparer.Ordinal).Count());

        static async IAsyncEnumerable<int> BatchStream(int start, [EnumeratorCancellation] CancellationToken ct)
        {
            yield return start;
            await Task.Delay(10, ct);
            yield return start + 1;
        }
    }

    [Fact]
    public async Task ObserveBatchEventsAsync_CanResumeFromEventCursor()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("BatchResumeStream", BatchResumeStream);
        await harness.StartAsync(ct);

        var batchId = await ((JobClient)harness.Client).TriggerAllAsync("BatchResumeStream", [
            new { start = 1 },
            new { start = 10 }
        ], ct);

        long cursorId = 0;
        await foreach (var @event in harness.Client.ObserveBatchEventsAsync(batchId, 0, ct))
        {
            if (@event.EventType != RunEventType.Output)
            {
                continue;
            }

            cursorId = @event.Id;
            break;
        }

        Assert.True(cursorId > 0);

        var resumed = new List<(string RunId, int Item)>();
        await foreach (var @event in harness.Client.ObserveBatchEventsAsync(batchId, cursorId, ct))
        {
            if (@event.EventType != RunEventType.Output)
            {
                continue;
            }

            resumed.Add((@event.RunId, JsonSerializer.Deserialize<int>(@event.Payload)));
        }

        // The resumed set plus the one we saw at the cursor cover all output events.
        Assert.Equal(3, resumed.Count);

        static async IAsyncEnumerable<int> BatchResumeStream(int start, [EnumeratorCancellation] CancellationToken ct)
        {
            yield return start;
            await Task.Delay(15, ct);
            yield return start + 1;
        }
    }

    [Fact]
    public async Task ObserveBatchEventsAsync_LargeBatch_FirstItem_IsNotPollingBound()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromSeconds(2);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("BatchWakeHint", BatchWakeHint);
        await harness.StartAsync(ct);

        var args = Enumerable.Range(1, 200).Select(i => (object?)new { value = i }).ToArray();
        var batchId = await ((JobClient)harness.Client).TriggerAllAsync("BatchWakeHint", args, ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(10));
        var startedAt = Stopwatch.GetTimestamp();
        int? first = null;
        await foreach (var @event in harness.Client.ObserveBatchEventsAsync(batchId, 0, cts.Token))
        {
            if (@event.EventType != RunEventType.Output)
            {
                continue;
            }

            first = JsonSerializer.Deserialize<int>(@event.Payload);
            break;
        }

        Assert.NotNull(first);
        var elapsed = Stopwatch.GetElapsedTime(startedAt);
        Assert.True(elapsed < TimeSpan.FromSeconds(2.8),
            $"Expected first streamed item before polling interval. Elapsed={elapsed}.");

        static async IAsyncEnumerable<int> BatchWakeHint(int value, [EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Delay(200, ct);
            yield return value;
        }
    }

    [Fact]
    public async Task RunAsync_List_UsesOutputEvents_WhenHandlerReturnsAsyncEnumerable()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("StreamSource", StreamSource);

        await harness.StartAsync(ct);

        var values = await harness.Client.RunAsync<List<int>>("StreamSource", cancellationToken: ct);
        Assert.Equal([10, 20], values);

        static async IAsyncEnumerable<int> StreamSource([EnumeratorCancellation] CancellationToken ct)
        {
            yield return 10;
            await Task.Delay(10, ct);
            yield return 20;
        }
    }

    [Fact]
    public async Task StreamRun_PersistsOutputEvents_BeforeRunCompletes()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("IncrementalOutput", IncrementalOutput);
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("IncrementalOutput", cancellationToken: ct);
        var runId = run.Id;
        await WaitForStatusAsync(harness, runId, JobStatus.Running, ct);

        await TestWait.PollUntilConditionAsync(
            async _ =>
            {
                var run = await harness.Store.GetRunAsync(runId, ct)
                          ?? throw new InvalidOperationException($"Run '{runId}' not found.");
                var outputs =
                    await harness.Store.GetEventsAsync(runId, types: [RunEventType.Output], cancellationToken: ct);
                return run.Status == JobStatus.Running && outputs.Count > 0;
            },
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            "Expected at least one output event while run status was Running.",
            ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        Assert.Equal([1, 2, 3], await harness.Client.WaitAsync<List<int>>(run.Id, cts.Token));

        var allOutputEvents =
            await harness.Store.GetEventsAsync(runId, types: [RunEventType.Output], cancellationToken: ct);
        var completionEvents =
            await harness.Store.GetEventsAsync(runId, types: [RunEventType.OutputComplete], cancellationToken: ct);
        Assert.Equal(3, allOutputEvents.Count);
        Assert.Single(completionEvents);

        static async IAsyncEnumerable<int> IncrementalOutput([EnumeratorCancellation] CancellationToken ct)
        {
            for (var i = 1; i <= 3; i++)
            {
                await Task.Delay(200, ct);
                yield return i;
            }
        }
    }

    [Fact]
    public async Task ContinuousJob_RequeuesAfterCompletion()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        var count = 0;
        harness.Host.AddJob("Continuous", () => Interlocked.Increment(ref count))
            .Continuous();

        await harness.StartAsync(ct);

        var firstRun = await harness.Client.TriggerAsync("Continuous", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        firstRun = await harness.Client.WaitAsync(firstRun.Id, cts.Token);
        Assert.True(firstRun.IsSuccess);

        var second = await WaitForAnotherRunAsync(harness.Store, "Continuous", firstRun.Id, cts.Token);
        Assert.NotNull(second);
    }

    [Fact]
    public async Task ContinuousJob_Successor_UsesDeduplicationIdBasedOnCompletedRun()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Continuous", () => 1).Continuous();

        await harness.StartAsync(ct);

        var firstRun = await harness.Client.TriggerAsync("Continuous", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        firstRun = await harness.Client.WaitAsync(firstRun.Id, cts.Token);
        Assert.True(firstRun.IsSuccess);

        var successor = await WaitForAnotherRunAsync(harness.Store, "Continuous", firstRun.Id, cts.Token);
        Assert.NotNull(successor);
        var successorRun = successor;
        Assert.StartsWith("continuous:", successorRun.DeduplicationId);

        var predecessorId = successorRun.DeduplicationId!["continuous:".Length..];
        var predecessor = await harness.Store.GetRunAsync(predecessorId, cts.Token);
        Assert.NotNull(predecessor);
        Assert.Equal("Continuous", predecessor.JobName);
        Assert.Equal(JobStatus.Succeeded, predecessor.Status);
    }

    [Fact]
    public async Task ContinuousJob_SeedsToMaxConcurrency_OnStartup()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.InactiveThreshold = TimeSpan.FromMilliseconds(200);
            options.RetentionPeriod = null;
            options.MaxNodeConcurrency = 10;
        });

        harness.Host.AddJob("ContinuousSeedStartup", async (CancellationToken ct) =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                return 1;
            })
            .Continuous()
            .WithMaxConcurrency(3);

        await harness.StartAsync(ct);

        var page = await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = "ContinuousSeedStartup",
                IsTerminal = false
            }, 0, 20, ct),
            p => p.Items.Count == 3 && p.Items.Count(r => r.Status == JobStatus.Running) is >= 1 and <= 3,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            "Timed out waiting for startup continuous seeding to create max-concurrency runs.",
            ct);

        Assert.Equal(3, page.Items.Count);
        Assert.InRange(page.Items.Count(r => r.Status == JobStatus.Running), 1, 3);
    }

    [Fact]
    public async Task ContinuousJob_DoesNotCreateExtraRuns_WhenManualBacklogAlreadySatisfiesMax()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.InactiveThreshold = TimeSpan.FromMilliseconds(200);
            options.RetentionPeriod = null;
        });

        var permits = Channel.CreateUnbounded<bool>();
        harness.Host.AddJob("ContinuousBacklog", async (CancellationToken ct) =>
            {
                await permits.Reader.ReadAsync(ct);
                return 1;
            })
            .Continuous()
            .WithMaxConcurrency(1);

        await harness.StartAsync(ct);

        await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = "ContinuousBacklog",
                Status = JobStatus.Running
            }, 0, 10, ct),
            p => p.Items.Count == 1,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            "Timed out waiting for first continuous run to start.",
            ct);

        await harness.Client.TriggerAsync("ContinuousBacklog", cancellationToken: ct);
        await harness.Client.TriggerAsync("ContinuousBacklog", cancellationToken: ct);

        await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = "ContinuousBacklog",
                IsTerminal = false
            }, 0, 20, ct),
            p => p.Items.Count == 3,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            "Timed out waiting for manual backlog to reach three non-terminal runs.",
            ct);

        await permits.Writer.WriteAsync(true, ct);

        var settled = await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = "ContinuousBacklog",
                IsTerminal = false
            }, 0, 20, ct),
            p => p.Items.Count == 2 && p.Items.Count(r => r.Status == JobStatus.Running) == 1,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            "Timed out waiting for backlog to drain without over-seeding.",
            ct);

        Assert.Equal(2, settled.Items.Count);
    }

    [Fact]
    public async Task StaleRunningRun_IsRecoveredAndReclaimed_ByAnotherNodeLoop()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.InactiveThreshold = TimeSpan.FromMilliseconds(120);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("CrashRecovery", () => 7).WithRetry(retry =>
        {
            retry.MaxRetries = 1;
            retry.InitialDelay = TimeSpan.Zero;
            retry.Jitter = false;
        });

        await harness.StartAsync(ct);

        var staleAt = DateTimeOffset.UtcNow.AddSeconds(-10);
        var stale = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = "CrashRecovery",
            Status = JobStatus.Running,
            CreatedAt = staleAt,
            NotBefore = staleAt,
            StartedAt = staleAt,
            LastHeartbeatAt = staleAt,
            NodeName = "dead-node",
            Attempt = 1,
            Progress = 0
        };

        await harness.Store.CreateRunsAsync([stale], cancellationToken: ct);

        var recovered = await TestWait.AwaitRunAsync(
            harness.Store, harness.Notifications, stale.Id,
            r => r.Status == JobStatus.Succeeded,
            TimeSpan.FromSeconds(5),
            "Timed out waiting for stale running run to be recovered and completed.",
            ct);

        Assert.Equal(2, recovered.Attempt);
    }

    [Fact]
    public async Task StaleRunningRun_WithNonBatchParent_DoesNotFailMaintenanceTick()
    {
        var ct = TestContext.Current.CancellationToken;
        var logs = new ConcurrentQueue<LogEntry>();

        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.InactiveThreshold = TimeSpan.FromMilliseconds(120);
            options.RetentionPeriod = null;
        }, services => { services.AddLogging(builder => builder.AddProvider(new CollectingLoggerProvider(logs))); });

        harness.Host.AddJob("CrashRecoveryParent", () => 7).WithRetry(0);

        await harness.StartAsync(ct);

        var staleAt = DateTimeOffset.UtcNow.AddSeconds(-10);
        var parent = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = "CrashRecoveryParent",
            Status = JobStatus.Pending,
            CreatedAt = staleAt,
            NotBefore = staleAt
        };

        var staleChild = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = "CrashRecoveryParent",
            Status = JobStatus.Running,
            ParentRunId = parent.Id,
            RootRunId = parent.Id,
            CreatedAt = staleAt,
            NotBefore = staleAt,
            StartedAt = staleAt,
            LastHeartbeatAt = staleAt,
            NodeName = "dead-node",
            Attempt = 1,
            Progress = 0
        };

        await harness.Store.CreateRunsAsync([parent, staleChild], cancellationToken: ct);

        var recovered = await TestWait.AwaitRunAsync(
            harness.Store, harness.Notifications, staleChild.Id,
            run => run.Status == JobStatus.Failed,
            TimeSpan.FromSeconds(5),
            "Timed out waiting for stale child run with non-batch parent to dead-letter.",
            ct);

        Assert.DoesNotContain(
            logs,
            entry =>
                entry.Level >= LogLevel.Error &&
                string.Equals(entry.Category, "Surefire.SurefireMaintenanceService", StringComparison.Ordinal) &&
                entry.Message.Contains("Maintenance tick failed.", StringComparison.Ordinal) &&
                entry.Exception is InvalidOperationException invalidOperation &&
                invalidOperation.Message.Contains("not a batch coordinator", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task StaleRunningRun_DeadLetter_PersistsMaintenanceAttemptFailureEvent()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.InactiveThreshold = TimeSpan.FromMilliseconds(120);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("CrashRecoveryDeadLetterEvent", () => 7).WithRetry(0);

        await harness.StartAsync(ct);

        var staleAt = DateTimeOffset.UtcNow.AddSeconds(-10);
        var stale = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = "CrashRecoveryDeadLetterEvent",
            Status = JobStatus.Running,
            CreatedAt = staleAt,
            NotBefore = staleAt,
            StartedAt = staleAt,
            LastHeartbeatAt = staleAt,
            NodeName = "dead-node",
            Attempt = 1,
            Progress = 0
        };

        await harness.Store.CreateRunsAsync([stale], cancellationToken: ct);

        await TestWait.PollUntilAsync(
            () => harness.Store.GetRunAsync(stale.Id, ct),
            run => run.Status == JobStatus.Failed,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for stale run to dead-letter.",
            ct);

        var failureEvents = await TestWait.PollUntilAsync(
            async _ => await harness.Store.GetEventsAsync(stale.Id, 0, [RunEventType.AttemptFailure],
                cancellationToken: ct),
            events => events.Count > 0,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for stale dead-letter attempt-failure event.",
            ct);

        var envelope = JsonDocument.Parse(failureEvents[^1].Payload).RootElement;
        Assert.Equal(1, ReadInt(envelope, "Attempt", "attempt"));
        Assert.Equal("maintenance", ReadString(envelope, "FailureSource", "failureSource"));
        Assert.Equal("stale_recovery", ReadString(envelope, "FailureCode", "failureCode"));
        Assert.Null(ReadOptionalString(envelope, "StackTrace", "stackTrace"));

        static int ReadInt(JsonElement element, string first, string second)
        {
            if (element.TryGetProperty(first, out var value))
            {
                return value.GetInt32();
            }

            return element.GetProperty(second).GetInt32();
        }

        static string ReadString(JsonElement element, string first, string second)
        {
            if (element.TryGetProperty(first, out var value))
            {
                return value.GetString()!;
            }

            return element.GetProperty(second).GetString()!;
        }

        static string? ReadOptionalString(JsonElement element, string first, string second)
        {
            if (element.TryGetProperty(first, out var firstValue))
            {
                return firstValue.ValueKind == JsonValueKind.Null ? null : firstValue.GetString();
            }

            if (element.TryGetProperty(second, out var secondValue))
            {
                return secondValue.ValueKind == JsonValueKind.Null ? null : secondValue.GetString();
            }

            return null;
        }
    }

    [Fact]
    public async Task StaleRunningRun_Retry_PersistsMaintenanceAttemptFailureEvent()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.InactiveThreshold = TimeSpan.FromMilliseconds(120);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("CrashRecoveryRetryEvent", () => 7).WithRetry(policy =>
        {
            policy.MaxRetries = 1;
            policy.InitialDelay = TimeSpan.FromMilliseconds(50);
        });

        await harness.StartAsync(ct);

        var staleAt = DateTimeOffset.UtcNow.AddSeconds(-10);
        var stale = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = "CrashRecoveryRetryEvent",
            Status = JobStatus.Running,
            CreatedAt = staleAt,
            NotBefore = staleAt,
            StartedAt = staleAt,
            LastHeartbeatAt = staleAt,
            NodeName = "dead-node",
            Attempt = 1,
            Progress = 0
        };

        await harness.Store.CreateRunsAsync([stale], cancellationToken: ct);

        await TestWait.PollUntilAsync(
            () => harness.Store.GetRunAsync(stale.Id, ct),
            run => run.Status == JobStatus.Succeeded,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for stale run to recover and complete.",
            ct);

        var failureEvents = await TestWait.PollUntilAsync(
            async _ => await harness.Store.GetEventsAsync(stale.Id, 0, [RunEventType.AttemptFailure],
                cancellationToken: ct),
            events => events.Count > 0,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for stale retry attempt-failure event.",
            ct);

        var envelope = JsonDocument.Parse(failureEvents[^1].Payload).RootElement;
        Assert.Equal(1, ReadInt(envelope, "Attempt", "attempt"));
        Assert.Equal("maintenance", ReadString(envelope, "FailureSource", "failureSource"));
        Assert.Equal("stale_recovery", ReadString(envelope, "FailureCode", "failureCode"));
        Assert.Null(ReadOptionalString(envelope, "StackTrace", "stackTrace"));

        static int ReadInt(JsonElement element, string first, string second)
        {
            if (element.TryGetProperty(first, out var value))
            {
                return value.GetInt32();
            }

            return element.GetProperty(second).GetInt32();
        }

        static string ReadString(JsonElement element, string first, string second)
        {
            if (element.TryGetProperty(first, out var value))
            {
                return value.GetString()!;
            }

            return element.GetProperty(second).GetString()!;
        }

        static string? ReadOptionalString(JsonElement element, string first, string second)
        {
            if (element.TryGetProperty(first, out var firstValue))
            {
                return firstValue.ValueKind == JsonValueKind.Null ? null : firstValue.GetString();
            }

            if (element.TryGetProperty(second, out var secondValue))
            {
                return secondValue.ValueKind == JsonValueKind.Null ? null : secondValue.GetString();
            }

            return null;
        }
    }

    [Fact]
    public async Task StaleRunningRun_Recovery_UsesConfiguredRetryBackoff()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.InactiveThreshold = TimeSpan.FromMilliseconds(120);
            options.RetentionPeriod = null;
            options.AddQueue("paused", queue => queue.IsPaused = true);
        });

        harness.Host.AddJob("CrashRecoveryBackoff", () => 7)
            .WithQueue("paused")
            .WithRetry(policy =>
            {
                policy.MaxRetries = 2;
                policy.InitialDelay = TimeSpan.FromMilliseconds(500);
                policy.MaxDelay = TimeSpan.FromMilliseconds(500);
                policy.Jitter = false;
            });

        await harness.StartAsync(ct);

        var staleAt = DateTimeOffset.UtcNow.AddSeconds(-10);
        var stale = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = "CrashRecoveryBackoff",
            Status = JobStatus.Running,
            CreatedAt = staleAt,
            NotBefore = staleAt,
            StartedAt = staleAt,
            LastHeartbeatAt = staleAt,
            NodeName = "dead-node",
            Attempt = 1,
            Progress = 0
        };

        await harness.Store.CreateRunsAsync([stale], cancellationToken: ct);

        var recovered = await TestWait.AwaitRunAsync(
            harness.Store, harness.Notifications, stale.Id,
            run => run.Status == JobStatus.Pending,
            TimeSpan.FromSeconds(8),
            "Timed out waiting for stale run recovery to Pending state.",
            ct);

        var remainingDelay = recovered.NotBefore - DateTimeOffset.UtcNow;
        Assert.True(
            remainingDelay > TimeSpan.FromMilliseconds(250),
            $"Expected stale recovery to preserve retry backoff. Remaining delay was {remainingDelay}.");
    }

    [Fact]
    public async Task BatchChild_DeadLetter_AppendsAttemptFailureEvent()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("BatchFailure",
            (int value) => { throw new InvalidOperationException($"child failed: {value}"); }).WithRetry(0);

        await harness.StartAsync(ct);

        var batchId = await ((JobClient)harness.Client).TriggerAllAsync("BatchFailure", [new { value = 1 }], ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(10));
        var batch = await harness.Client.WaitBatchAsync(batchId, cts.Token);
        Assert.Equal(JobStatus.Failed, batch.Status);

        var childRuns = await harness.Store.GetRunsAsync(new() { BatchId = batchId }, 0, 10, cts.Token);
        Assert.Single(childRuns.Items);
        var childRunId = childRuns.Items[0].Id;

        var failureEvents = await TestWait.PollUntilAsync(
            async _ => await harness.Store.GetEventsAsync(childRunId, 0, [RunEventType.AttemptFailure],
                cancellationToken: ct),
            events => events.Count > 0,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for child attempt-failure event.",
            ct);

        Assert.Contains(failureEvents, e => e.Payload.Contains("child failed: 1", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Filters_GlobalThenJob_OrderIsDeterministic()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
            options.UseFilter<GlobalTraceFilter>();
        }, services => services.AddSingleton<TraceSink>());

        harness.Host.AddJob("Filtered", (TraceSink sink) =>
        {
            sink.Events.Enqueue("handler");
            return 1;
        }).UseFilter<JobTraceFilter>();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Filtered", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        run = await harness.Client.WaitAsync(run.Id, cts.Token);

        Assert.True(run.IsSuccess);
        var sink = harness.Provider.GetRequiredService<TraceSink>();
        Assert.Equal(
            ["global-before", "job-before", "handler", "job-after", "global-after"],
            sink.Events.ToArray());
    }

    [Fact]
    public async Task LifecycleCallbacks_GlobalAndJob_AreInvoked()
    {
        var ct = TestContext.Current.CancellationToken;
        var globalRetry = 0;
        var globalDead = 0;
        var globalSuccess = 0;
        var jobRetry = 0;
        var jobDead = 0;
        var attempts = 0;

        // Use signals to wait for callbacks deterministically instead of inferring
        // from run status. Eliminates the race between "run is terminal" and "callback
        // has fired and its side effects are visible to the test thread."
        var retrySignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deadLetterSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var jobRetrySignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var jobDeadLetterSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
            options.OnRetry((JobContext _) =>
            {
                Interlocked.Increment(ref globalRetry);
                retrySignal.TrySetResult();
            });
            options.OnDeadLetter((JobContext _) =>
            {
                Interlocked.Increment(ref globalDead);
                deadLetterSignal.TrySetResult();
            });
            options.OnSuccess((JobContext _) => Interlocked.Increment(ref globalSuccess));
        });

        harness.Host.AddJob("CallbackFlaky", () =>
            {
                if (Interlocked.Increment(ref attempts) <= 2)
                {
                    throw new InvalidOperationException("boom");
                }

                return 7;
            })
            .WithRetry(policy =>
            {
                policy.MaxRetries = 1;
                policy.InitialDelay = TimeSpan.FromMilliseconds(50);
            })
            .OnRetry((JobContext _) =>
            {
                Interlocked.Increment(ref jobRetry);
                jobRetrySignal.TrySetResult();
            })
            .OnDeadLetter((JobContext _) =>
            {
                Interlocked.Increment(ref jobDead);
                jobDeadLetterSignal.TrySetResult();
            });

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("CallbackFlaky", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(10));

        // Wait for each signal explicitly — no timing assumptions.
        await retrySignal.Task.WaitAsync(cts.Token);
        await deadLetterSignal.Task.WaitAsync(cts.Token);
        await jobRetrySignal.Task.WaitAsync(cts.Token);
        await jobDeadLetterSignal.Task.WaitAsync(cts.Token);

        run = await harness.Client.WaitAsync(run.Id, cts.Token);

        Assert.True(run.IsFailure);
        Assert.Equal(1, globalRetry);
        Assert.Equal(1, globalDead);
        Assert.Equal(0, globalSuccess);
        Assert.Equal(1, jobRetry);
        Assert.Equal(1, jobDead);
    }

    [Fact]
    public async Task DeadLetter_RecordsExceptionDetailOnAttemptFailureEvent_NotOnRunReason()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("DeadLetterDetails", () => { throw new InvalidOperationException("dead-letter-details"); })
            .WithRetry(0);

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("DeadLetterDetails", cancellationToken: ct);
        var runId = run.Id;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        run = await harness.Client.WaitAsync(run.Id, cts.Token);

        Assert.True(run.IsFailure);

        var storedRun = await harness.Store.GetRunAsync(runId, cts.Token);
        Assert.NotNull(storedRun);
        Assert.Equal(JobStatus.Failed, storedRun.Status);

        // Per run.Reason contract: retry exhaustion leaves Reason null; exception detail
        // lives on the AttemptFailure event for the failing attempt.
        Assert.Null(storedRun.Reason);

        var failures =
            await harness.Store.GetEventsAsync(runId, 0, [RunEventType.AttemptFailure], cancellationToken: cts.Token);
        var failure = Assert.Single(failures);
        Assert.NotNull(failure.Payload);
        Assert.Contains("System.InvalidOperationException", failure.Payload, StringComparison.Ordinal);
        Assert.Contains("dead-letter-details", failure.Payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RetryAndDeadLetter_EmitFrameworkLogEventsPerAttempt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("RetryFrameworkLogs",
                () => { throw new InvalidOperationException("retry-framework-logs"); })
            .WithRetry(policy =>
            {
                policy.MaxRetries = 1;
                policy.InitialDelay = TimeSpan.FromMilliseconds(25);
                policy.MaxDelay = TimeSpan.FromMilliseconds(25);
                policy.Jitter = false;
            });

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("RetryFrameworkLogs", cancellationToken: ct);
        var runId = run.Id;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        run = await harness.Client.WaitAsync(run.Id, cts.Token);

        Assert.True(run.IsFailure);

        var logEvents = await harness.Store.GetEventsAsync(runId, 0, [RunEventType.Log], cancellationToken: cts.Token);
        var logs = logEvents
            .Select(e => JsonDocument.Parse(e.Payload).RootElement)
            .ToList();

        Assert.NotEmpty(logs);
        Assert.Contains(logs, e =>
            e.TryGetProperty("message", out var msg)
            && msg.GetString()!.Contains("Attempt 1 failed", StringComparison.Ordinal));
        Assert.Contains(logs, e =>
            e.TryGetProperty("message", out var msg)
            && msg.GetString()!.Contains("dead letter", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(logs, e =>
            e.TryGetProperty("exception", out var ex)
            && ex.GetString()!.Contains("System.InvalidOperationException", StringComparison.Ordinal));
    }

    [Fact]
    public async Task RetryAndDeadLetter_PersistAttemptFailureEventsPerAttempt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("RetryFailureEvents",
                () => { throw new InvalidOperationException("retry-failure-events"); })
            .WithRetry(policy =>
            {
                policy.MaxRetries = 1;
                policy.InitialDelay = TimeSpan.FromMilliseconds(25);
                policy.MaxDelay = TimeSpan.FromMilliseconds(25);
                policy.Jitter = false;
            });

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("RetryFailureEvents", cancellationToken: ct);
        var runId = run.Id;
        using var completionCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        completionCts.CancelAfter(TimeSpan.FromSeconds(15));
        run = await harness.Client.WaitAsync(run.Id, completionCts.Token);

        Assert.True(run.IsFailure);

        using var eventsCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        eventsCts.CancelAfter(TimeSpan.FromSeconds(10));
        var failureEvents = await TestWait.PollUntilAsync(
            async innerCt =>
                await harness.Store.GetEventsAsync(runId, 0, [RunEventType.AttemptFailure], cancellationToken: innerCt),
            events => events.Count == 2,
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMilliseconds(100),
            "Timed out waiting for both attempt-failure events to be persisted.",
            eventsCts.Token);
        Assert.Equal(2, failureEvents.Count);

        var envelopes = failureEvents
            .Select(e => JsonDocument.Parse(e.Payload).RootElement)
            .OrderBy(e => ReadInt(e, "Attempt", "attempt"))
            .ToList();

        Assert.Equal(1, ReadInt(envelopes[0], "Attempt", "attempt"));
        Assert.Equal(2, ReadInt(envelopes[1], "Attempt", "attempt"));

        Assert.All(envelopes, envelope =>
        {
            Assert.Equal("System.InvalidOperationException", ReadString(envelope, "ExceptionType", "exceptionType"));
            Assert.Equal("executor", ReadString(envelope, "FailureSource", "failureSource"));
            Assert.Equal("exception", ReadString(envelope, "FailureCode", "failureCode"));
            Assert.Contains("retry-failure-events", ReadString(envelope, "Message", "message"),
                StringComparison.Ordinal);
            Assert.Contains("System.InvalidOperationException", ReadString(envelope, "StackTrace", "stackTrace"),
                StringComparison.Ordinal);
        });

        static int ReadInt(JsonElement element, string first, string second)
        {
            if (element.TryGetProperty(first, out var value))
            {
                return value.GetInt32();
            }

            return element.GetProperty(second).GetInt32();
        }

        static string ReadString(JsonElement element, string first, string second)
        {
            if (element.TryGetProperty(first, out var value))
            {
                return value.GetString()!;
            }

            return element.GetProperty(second).GetString()!;
        }
    }

    [Fact]
    public async Task ParameterBinder_MapsJsonArrayToAsyncEnumerable()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumAsyncEnumerable", async (IAsyncEnumerable<int> values, CancellationToken ct) =>
        {
            var sum = 0;
            await foreach (var value in values.WithCancellation(ct))
            {
                sum += value;
            }

            return sum;
        });

        await harness.StartAsync(ct);

        var total = await harness.Client.RunAsync<int>("SumAsyncEnumerable", new { values = new[] { 1, 2, 3, 4 } },
            cancellationToken: ct);
        Assert.Equal(10, total);
    }

    [Fact]
    public async Task TriggerAsync_InsideRun_AutoLinksParentAndRoot()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("NestedChild", () => 7);
        harness.Host.AddJob("NestedParent", async (IJobClient client) =>
            (await client.TriggerAsync("NestedChild")).Id);

        await harness.StartAsync(ct);

        var parentRun = await harness.Client.TriggerAsync("NestedParent", cancellationToken: ct);
        var parentRunId = parentRun.Id;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        var childRunId = await harness.Client.WaitAsync<string>(parentRunId, cts.Token);

        var childRun = await harness.Store.GetRunAsync(childRunId, cts.Token);
        Assert.NotNull(childRun);
        Assert.Equal(parentRunId, childRun.ParentRunId);
        Assert.Equal(parentRunId, childRun.RootRunId);
    }

    [Fact]
    public async Task TriggerAllAsync_InsideRun_AutoLinksChildrenToOuterRoot()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("NestedBatchChild", (int value) => value);
        harness.Host.AddJob("NestedBatchParent", async (IJobClient client) =>
            await ((JobClient)client).TriggerAllAsync("NestedBatchChild", [new { value = 1 }, new { value = 2 }]));

        await harness.StartAsync(ct);

        var parentRun = await harness.Client.TriggerAsync("NestedBatchParent", cancellationToken: ct);
        var parentRunId = parentRun.Id;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        var batchId = await harness.Client.WaitAsync<string>(parentRunId, cts.Token);

        var batch = await harness.Store.GetBatchAsync(batchId, cts.Token);
        Assert.NotNull(batch);
        Assert.Equal(2, batch.Total);

        var children = await harness.Store.GetRunsAsync(new() { BatchId = batchId }, 0, 20, cts.Token);
        Assert.Equal(2, children.Items.Count);
        Assert.All(children.Items, child =>
        {
            Assert.Equal(parentRunId, child.ParentRunId);
            Assert.Equal(parentRunId, child.RootRunId);
        });
    }

    [Fact]
    public async Task ParameterBinder_MapsJsonArrayToList()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumList", (List<int> values) => values.Sum());

        await harness.StartAsync(ct);

        var total = await harness.Client.RunAsync<int>("SumList", new { values = new[] { 2, 3, 5 } },
            cancellationToken: ct);
        Assert.Equal(10, total);
    }

    [Fact]
    public async Task StreamingHandler_BindsScalarArguments()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("StreamFromCount", StreamFromCount);

        await harness.StartAsync(ct);

        var values =
            await harness.Client.RunAsync<List<int>>("StreamFromCount", new { count = 3 }, cancellationToken: ct);
        Assert.Equal([0, 1, 2], values);

        static async IAsyncEnumerable<int> StreamFromCount(int count, [EnumeratorCancellation] CancellationToken ct)
        {
            for (var i = 0; i < count; i++)
            {
                ct.ThrowIfCancellationRequested();
                yield return i;
                await Task.Yield();
            }
        }
    }

    [Fact]
    public async Task AsyncEnumerableArgument_BindsToListParameter()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumFromList", (List<int> input) => input.Sum());

        await harness.StartAsync(ct);

        var total = await harness.Client.RunAsync<int>("SumFromList", new
        {
            input = ProduceDelayedRange(1, 5, 10, ct)
        }, cancellationToken: ct);

        Assert.Equal(15, total);
    }

    [Fact]
    public async Task AsyncEnumerableArgument_StreamsThroughHandlerIncrementally()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(15);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(30);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("DoubleStream", DoubleStream);

        await harness.StartAsync(ct);

        var started = DateTimeOffset.UtcNow;
        var outputs = new List<int>();
        await foreach (var item in harness.Client.StreamAsync<int>("DoubleStream", new
                       {
                           input = ProduceDelayedRange(1, 3, 120, ct)
                       }, cancellationToken: ct))
        {
            outputs.Add(item);
            if (outputs.Count == 1)
            {
                var firstDelay = DateTimeOffset.UtcNow - started;
                Assert.True(firstDelay < TimeSpan.FromMilliseconds(2500));
            }
        }

        Assert.Equal([2, 4, 6], outputs);

        static async IAsyncEnumerable<int> DoubleStream(IAsyncEnumerable<int> input,
            [EnumeratorCancellation] CancellationToken ct)
        {
            await foreach (var value in input.WithCancellation(ct))
            {
                yield return value * 2;
            }
        }
    }

    [Fact]
    public async Task StreamBindableParameter_MissingArgument_FailsFastWithoutDeclaredInput()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumFromListMissing", (List<int> input) => input.Sum());

        await harness.StartAsync(ct);

        var startedAt = Stopwatch.GetTimestamp();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(2));
        var ex = await Assert.ThrowsAsync<JobRunException>(async () =>
            await harness.Client.RunAsync<int>("SumFromListMissing", new { }, cancellationToken: cts.Token));
        var elapsed = Stopwatch.GetElapsedTime(startedAt);

        Assert.True(elapsed < TimeSpan.FromSeconds(2),
            $"Missing undeclared stream binding should fail fast. Elapsed: {elapsed}.");

        // Binding error detail is captured on the AttemptFailure event (run.Reason is
        // reserved for non-exception termination causes).
        var failures =
            await harness.Store.GetEventsAsync(ex.RunId, 0, [RunEventType.AttemptFailure], cancellationToken: ct);
        Assert.NotEmpty(failures);
        Assert.Contains(failures,
            f => f.Payload is { } p && p.Contains("Unable to bind parameter", StringComparison.Ordinal) &&
                 p.Contains("input", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DeclaredInputStream_CanDelayFirstItemWithoutTimingOut()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumFromDelayedList", (List<int> input) => input.Sum());

        await harness.StartAsync(ct);

        var total = await harness.Client.RunAsync<int>("SumFromDelayedList", new
        {
            input = ProduceDelayedRange(1, 5, 600, ct)
        }, cancellationToken: ct);

        Assert.Equal(15, total);
    }

    [Fact]
    public async Task AsyncEnumerableArgument_SourceCancellation_DoesNotSilentlyCompleteWithPartialInput()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumSourceCancelledStream", async (IAsyncEnumerable<int> values, CancellationToken ct) =>
        {
            var sum = 0;
            await foreach (var value in values.WithCancellation(ct))
            {
                sum += value;
            }

            return sum;
        }).WithRetry(0);

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("SumSourceCancelledStream", new
        {
            values = SourceCancelledStream()
        }, cancellationToken: ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        run = await harness.Client.WaitAsync(run.Id, cts.Token);

        Assert.True(run.IsFailure);
        Assert.NotEqual(JobStatus.Succeeded, run.Status);

        static async IAsyncEnumerable<int> SourceCancelledStream()
        {
            yield return 1;
            await Task.Yield();
            throw new OperationCanceledException("source canceled");
        }
    }

    [Fact]
    public async Task MultipleAsyncEnumerableArguments_AreBoundByName()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("MergeStreams", MergeStreams);

        await harness.StartAsync(ct);

        var values = await harness.Client.RunAsync<List<int>>("MergeStreams", new
        {
            left = ProduceDelayedRange(1, 3, 5, ct),
            right = ProduceDelayedRange(10, 12, 7, ct)
        }, cancellationToken: ct);

        Assert.Equal([1, 2, 3, 10, 11, 12], values);

        static async IAsyncEnumerable<int> MergeStreams(IAsyncEnumerable<int> left,
            IAsyncEnumerable<int> right,
            [EnumeratorCancellation] CancellationToken ct)
        {
            await foreach (var item in left.WithCancellation(ct))
            {
                yield return item;
            }

            await foreach (var item in right.WithCancellation(ct))
            {
                yield return item;
            }
        }
    }

    [Fact]
    public async Task RerunAsync_SingleRun_ReplaysStreamInputEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumInputStreamRerun", async (IAsyncEnumerable<int> values, CancellationToken ct) =>
        {
            var sum = 0;
            await foreach (var value in values.WithCancellation(ct))
            {
                sum += value;
            }

            return sum;
        });

        await harness.StartAsync(ct);

        var original = await harness.Client.TriggerAsync("SumInputStreamRerun", new
        {
            values = ProduceDelayedRange(1, 3, 5, ct)
        }, cancellationToken: ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        Assert.Equal(6, await harness.Client.WaitAsync<int>(original.Id, cts.Token));

        var rerun = await harness.Client.RerunAsync(original.Id, cts.Token);
        Assert.Equal(6, await harness.Client.WaitAsync<int>(rerun.Id, cts.Token));

        var rerunRun = await harness.Store.GetRunAsync(rerun.Id, cts.Token);
        Assert.NotNull(rerunRun);
        Assert.Equal(original.Id, rerunRun.RerunOfRunId);
    }

    [Fact]
    public async Task RerunAsync_BatchChildren_ReplayStreamInputEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumBatchInputStreamRerun", async (IAsyncEnumerable<int> values, CancellationToken ct) =>
        {
            var sum = 0;
            await foreach (var value in values.WithCancellation(ct))
            {
                sum += value;
            }

            return sum;
        });

        await harness.StartAsync(ct);

        var originalBatchId = await ((JobClient)harness.Client).TriggerAllAsync("SumBatchInputStreamRerun", [
            new { values = ProduceDelayedRange(1, 2, 5, ct) },
            new { values = ProduceDelayedRange(10, 20, 5, ct) }
        ], ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(8));
        var firstPass = new List<int>();
        await foreach (var result in harness.Client.WaitEachAsync<int>(originalBatchId, cts.Token))
        {
            firstPass.Add(result);
        }

        Assert.Equal([3, 165], firstPass.OrderBy(v => v).ToArray());

        var originalChildrenPage = await harness.Store.GetRunsAsync(
            new() { BatchId = originalBatchId },
            0,
            20,
            cts.Token);
        Assert.Equal(2, originalChildrenPage.Items.Count);
        var originalChildIds = originalChildrenPage.Items.Select(run => run.Id).ToHashSet(StringComparer.Ordinal);

        // Rerun each child individually — stream inputs are replayed
        var rerunRuns = new List<JobRun>();
        foreach (var child in originalChildrenPage.Items)
        {
            rerunRuns.Add(await harness.Client.RerunAsync(child.Id, cts.Token));
        }

        var rerunResults = new List<int>();
        foreach (var rerun in rerunRuns)
        {
            rerunResults.Add(await harness.Client.WaitAsync<int>(rerun.Id, cts.Token));
        }

        Assert.Equal([3, 165], rerunResults.OrderBy(v => v).ToArray());

        // Each rerun references the original child it was derived from
        Assert.All(rerunRuns, rerun =>
        {
            Assert.NotNull(rerun.RerunOfRunId);
            Assert.Contains(rerun.RerunOfRunId, originalChildIds);
        });
    }

    [Fact]
    public async Task RetentionService_PurgesTerminalRuns_ByRetentionInterval()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromSeconds(10);
            options.RetentionCheckInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = TimeSpan.Zero;
        });

        await harness.StartAsync(ct);

        var completed = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = "retention-job",
            Status = JobStatus.Succeeded,
            CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
            NotBefore = DateTimeOffset.UtcNow.AddMinutes(-1),
            CompletedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
            Progress = 1
        };

        await harness.Store.CreateRunsAsync([completed], cancellationToken: ct);

        await TestWait.PollUntilConditionAsync(
            async _ => await harness.Store.GetRunAsync(completed.Id, ct) is null,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(25),
            "Retention service did not purge the completed run in time.",
            ct);
    }

    private static Task<JobRun> WaitForStatusAsync(RuntimeHarness harness, string runId, JobStatus status,
        CancellationToken ct = default)
    {
        return TestWait.AwaitRunAsync(
            harness.Store, harness.Notifications, runId,
            run => run.Status == status,
            TimeSpan.FromSeconds(5),
            $"Timed out waiting for run '{runId}' to reach status {status}.",
            ct);
    }

    private static async Task<JobRun?> WaitForAnotherRunAsync(IJobStore store, string jobName, string excludeRunId,
        CancellationToken cancellationToken)
    {
        try
        {
            return await TestWait.PollUntilAsync(
                async () =>
                {
                    var page = await store.GetRunsAsync(new() { JobName = jobName }, 0,
                        20, cancellationToken);
                    return page.Items.FirstOrDefault(r =>
                        !string.Equals(r.Id, excludeRunId, StringComparison.Ordinal));
                },
                _ => true,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20),
                "Timed out waiting for another run.",
                cancellationToken);
        }
        catch (TimeoutException)
        {
            return null;
        }
    }

    private static async IAsyncEnumerable<int> ProduceDelayedRange(int startInclusive, int endInclusive,
        int delayMs,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var i = startInclusive; i <= endInclusive; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Delay(delayMs, cancellationToken);
            yield return i;
        }
    }

    private static Task<RuntimeHarness> CreateHarnessAsync(Action<SurefireOptions> configure,
        Action<IServiceCollection>? configureServices = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSurefire(configure);
        configureServices?.Invoke(services);

        var provider = services.BuildServiceProvider();
        var host = new TestHost(provider);
        var client = provider.GetRequiredService<IJobClient>();
        var store = provider.GetRequiredService<IJobStore>();
        var notifications = provider.GetRequiredService<INotificationProvider>();
        var hostedServices = provider.GetServices<IHostedService>().ToArray();

        return Task.FromResult(new RuntimeHarness(provider, host, store, notifications, client, hostedServices));
    }

    private sealed class TraceSink
    {
        public Queue<string> Events { get; } = new();
    }

    private sealed class GlobalTraceFilter(TraceSink sink) : IJobFilter
    {
        public async Task InvokeAsync(JobContext context, JobFilterDelegate next)
        {
            sink.Events.Enqueue("global-before");
            await next(context);
            sink.Events.Enqueue("global-after");
        }
    }

    private sealed class JobTraceFilter(TraceSink sink) : IJobFilter
    {
        public async Task InvokeAsync(JobContext context, JobFilterDelegate next)
        {
            sink.Events.Enqueue("job-before");
            await next(context);
            sink.Events.Enqueue("job-after");
        }
    }

    private sealed record LogEntry(LogLevel Level, string Category, string Message, Exception? Exception);

    private sealed class CollectingLoggerProvider(ConcurrentQueue<LogEntry> entries) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new CollectingLogger(entries, categoryName);

        public void Dispose()
        {
        }
    }

    private sealed class CollectingLogger(ConcurrentQueue<LogEntry> entries, string categoryName) : ILogger
    {
        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter)
        {
            entries.Enqueue(new(logLevel, categoryName, formatter(state, exception), exception));
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