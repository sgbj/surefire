using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Surefire.Tests.Integration;

public sealed class RuntimeWorkersTests
{
    [Fact]
    public async Task Executor_Completes_Triggered_Run()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Echo", (string value) => value);
        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("Echo", new { value = "ok" });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);

        Assert.True(result.IsSuccess);
        Assert.Equal("ok", result.GetResult<string>());
    }

    [Fact]
    public async Task Executor_Retries_Then_Completes()
    {
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

        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("Flaky");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);

        Assert.True(result.IsSuccess);
        Assert.Equal(42, result.GetResult<int>());

        var run = await harness.Store.GetRunAsync(runId, cts.Token);
        Assert.NotNull(run);
        Assert.Equal(2, run.Attempt);
    }

    [Fact]
    public async Task Executor_NonGenericTaskResult_IsStoredAsEmpty()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("NoResultTask", async () => { await Task.Delay(1); });
        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("NoResultTask");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);

        Assert.True(result.IsSuccess);
        Assert.False(result.TryGetResult<object?>(out _));

        var run = await harness.Store.GetRunAsync(runId, cts.Token);
        Assert.NotNull(run);
        Assert.Null(run.Result);
    }

    [Fact]
    public async Task Executor_ExplicitNullResult_IsSerializedAsNull()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("ExplicitNull", () => (object?)null);
        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("ExplicitNull");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);

        Assert.True(result.IsSuccess);
        Assert.True(result.TryGetResult<object?>(out var typed));
        Assert.Null(typed);

        var run = await harness.Store.GetRunAsync(runId, cts.Token);
        Assert.NotNull(run);
        Assert.Equal("null", run.Result);
    }

    [Fact]
    public async Task Executor_EmptyObjectResult_IsSerializedAsObject()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("EmptyObject", () => new { });
        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("EmptyObject");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);

        Assert.True(result.IsSuccess);
        Assert.True(result.TryGetResult<JsonElement>(out var typed));
        Assert.Equal(JsonValueKind.Object, typed.ValueKind);

        var run = await harness.Store.GetRunAsync(runId, cts.Token);
        Assert.NotNull(run);
        Assert.Equal("{}", run.Result);
    }

    [Fact]
    public async Task Scheduler_FireAll_Enqueues_Missed_Occurrences()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(100);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("CronFireAll", () => 1)
            .WithCron("* * * * * *")
            .WithMisfirePolicy(MisfirePolicy.FireAll);

        await harness.StartAsync();

        await harness.Store.UpdateLastCronFireAtAsync("CronFireAll", DateTimeOffset.UtcNow.AddSeconds(-3));

        var timeout = DateTimeOffset.UtcNow.AddSeconds(5);
        while (DateTimeOffset.UtcNow < timeout)
        {
            var runsPage = await harness.Store.GetRunsAsync(new()
            {
                JobName = "CronFireAll",
                ExactJobName = true
            });

            if (runsPage.TotalCount >= 2)
            {
                return;
            }

            await Task.Delay(30);
        }

        throw new TimeoutException("Expected scheduler with FireAll misfire policy to enqueue at least two runs.");
    }

    [Fact]
    public async Task Scheduler_NewCronJob_WithNullLastCronFireAt_StartsScheduling()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(50);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("CronInitialSeed", () => 1)
            .WithCron("* * * * * *")
            .WithMisfirePolicy(MisfirePolicy.FireOnce);

        await harness.StartAsync();

        var timeout = DateTimeOffset.UtcNow.AddSeconds(4);
        while (DateTimeOffset.UtcNow < timeout)
        {
            var runsPage = await harness.Store.GetRunsAsync(new()
            {
                JobName = "CronInitialSeed",
                ExactJobName = true
            }, 0, 10);

            if (runsPage.TotalCount > 0)
            {
                return;
            }

            await Task.Delay(40);
        }

        throw new TimeoutException("Expected new cron job to start scheduling when LastCronFireAt was initially null.");
    }

    [Fact]
    public async Task Maintenance_Updates_Heartbeat_For_Active_Run()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Slow", async (CancellationToken ct) =>
        {
            await Task.Delay(300, ct);
            return 1;
        });

        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("Slow");
        var first = await WaitForStatusAsync(harness.Store, runId, JobStatus.Running);
        var hb1 = first.LastHeartbeatAt;

        await Task.Delay(160);

        var running = await harness.Store.GetRunAsync(runId);
        Assert.NotNull(running);
        Assert.Equal(JobStatus.Running, running.Status);
        Assert.NotNull(hb1);
        Assert.NotNull(running.LastHeartbeatAt);
        Assert.True(running.LastHeartbeatAt >= hb1);
    }

    [Fact]
    public async Task ExternalStoreCancellation_CancelsRunningRun_WithoutNotification()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(25);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Spin", async (CancellationToken ct) =>
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                await Task.Delay(20, ct);
            }
#pragma warning disable CS0162
            return 0;
#pragma warning restore CS0162
        });

        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("Spin");
        await WaitForStatusAsync(harness.Store, runId, JobStatus.Running);

        var cancelled = await harness.Store.TryCancelRunAsync(runId);
        Assert.True(cancelled);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);
        Assert.True(result.IsCancelled);
    }

    [Fact]
    public async Task RunAsync_Cancellation_PropagatesToRunExecution()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("CancelableRun", async (CancellationToken ct) =>
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                await Task.Delay(20, ct);
            }
#pragma warning disable CS0162
            return 0;
#pragma warning restore CS0162
        });

        await harness.StartAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(120));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            harness.Client.RunAsync<int>("CancelableRun", cts.Token));

        var timeout = DateTimeOffset.UtcNow.AddSeconds(5);
        while (DateTimeOffset.UtcNow < timeout)
        {
            var page = await harness.Store.GetRunsAsync(new()
            {
                JobName = "CancelableRun",
                ExactJobName = true
            }, 0, 1);

            var run = page.Items.SingleOrDefault();
            if (run is { Status: JobStatus.Cancelled })
            {
                return;
            }

            await Task.Delay(25);
        }

        throw new TimeoutException("Expected RunAsync cancellation to propagate and cancel execution.");
    }

    [Fact]
    public async Task WaitAsync_Cancellation_DoesNotCancelRunExecution()
    {
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

        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("WaitObserverOnly");
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(70));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => harness.Client.WaitAsync(runId, cts.Token));

        using var completionCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, completionCts.Token);

        Assert.True(result.IsSuccess);
        Assert.Equal(123, result.GetResult<int>());
    }

    [Fact]
    public async Task CancelAsync_OnBatchCoordinator_CancelsChildren()
    {
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

        await harness.StartAsync();

        var batchId = await harness.Client.TriggerAllAsync("SlowBatch", [new { x = 1 }, new { x = 2 }, new { x = 3 }]);
        await harness.Client.CancelAsync(batchId);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var coordinator = await harness.Client.WaitAsync(batchId, cts.Token);
        Assert.True(coordinator.IsCancelled || coordinator.IsFailure);

        var children = new List<JobRun>();
        await foreach (var run in harness.Client.GetRunsAsync(new() { ParentRunId = batchId }, cts.Token))
        {
            children.Add(run);
        }

        Assert.NotEmpty(children);
        Assert.All(children, child => Assert.Equal(JobStatus.Cancelled, child.Status));
    }

    [Fact]
    public async Task StreamAsync_FallsBackToResultJsonList_WhenNoOutputEvents()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("ResultList", () => new List<int> { 3, 4, 5 });
        await harness.StartAsync();

        var values = new List<int>();
        await foreach (var item in harness.Client.StreamAsync<int>("ResultList"))
        {
            values.Add(item);
        }

        Assert.Equal([3, 4, 5], values);
    }

    [Fact]
    public async Task StreamEachAsync_StreamsChildOutputItems()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("BatchStream", BatchStream);
        await harness.StartAsync();

        var batchId = await harness.Client.TriggerAllAsync("BatchStream", [
            new { start = 1 },
            new { start = 10 }
        ]);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var items = new List<BatchStreamItem<int>>();
        await foreach (var item in harness.Client.StreamEachAsync<int>(batchId, cts.Token))
        {
            items.Add(item);
        }

        Assert.Equal(4, items.Count);
        Assert.Equal([1, 2, 10, 11], items.Select(i => i.Item).OrderBy(v => v).ToArray());
        Assert.Equal(2, items.Select(i => i.RunId).Distinct(StringComparer.Ordinal).Count());

        static async IAsyncEnumerable<int> BatchStream(int start, [EnumeratorCancellation] CancellationToken ct)
        {
            yield return start;
            await Task.Delay(10, ct);
            yield return start + 1;
        }
    }

    [Fact]
    public async Task StreamEachAsync_CanResumeFromBatchCursor()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("BatchResumeStream", BatchResumeStream);
        await harness.StartAsync();

        var batchId = await harness.Client.TriggerAllAsync("BatchResumeStream", [
            new { start = 1 },
            new { start = 10 }
        ]);

        var firstPass = new List<BatchStreamItem<int>>();
        await foreach (var item in harness.Client.StreamEachAsync<int>(batchId))
        {
            firstPass.Add(item);
            break;
        }

        Assert.Single(firstPass);
        var cursor = firstPass[0].Cursor;

        var resumed = new List<BatchStreamItem<int>>();
        await foreach (var item in harness.Client.StreamEachAsync<int>(batchId, cursor))
        {
            resumed.Add(item);
        }

        var allValues = firstPass.Select(i => i.Item).Concat(resumed.Select(i => i.Item)).OrderBy(v => v).ToArray();
        Assert.Equal([1, 2, 10, 11], allValues);

        static async IAsyncEnumerable<int> BatchResumeStream(int start, [EnumeratorCancellation] CancellationToken ct)
        {
            yield return start;
            await Task.Delay(15, ct);
            yield return start + 1;
        }
    }

    [Fact]
    public async Task StreamEachAsync_LargeBatch_FirstItem_IsNotPollingBound()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromSeconds(2);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("BatchWakeHint", BatchWakeHint);
        await harness.StartAsync();

        var args = Enumerable.Range(1, 200).Select(i => (object?)new { value = i }).ToArray();
        var batchId = await harness.Client.TriggerAllAsync("BatchWakeHint", args);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var startedAt = Stopwatch.GetTimestamp();
        BatchStreamItem<int>? first = null;
        await foreach (var item in harness.Client.StreamEachAsync<int>(batchId, cts.Token))
        {
            first = item;
            break;
        }

        Assert.NotNull(first);
        var elapsed = Stopwatch.GetElapsedTime(startedAt);
        Assert.True(elapsed < TimeSpan.FromSeconds(1.2),
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
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("StreamSource", StreamSource);

        await harness.StartAsync();

        var values = await harness.Client.RunAsync<List<int>>("StreamSource");
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
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("IncrementalOutput", IncrementalOutput);
        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("IncrementalOutput");
        await WaitForStatusAsync(harness.Store, runId, JobStatus.Running);

        var timeout = DateTimeOffset.UtcNow.AddSeconds(5);
        var sawOutputWhileRunning = false;
        while (DateTimeOffset.UtcNow < timeout)
        {
            var run = await harness.Store.GetRunAsync(runId)
                      ?? throw new InvalidOperationException($"Run '{runId}' not found.");
            var outputs = await harness.Store.GetEventsAsync(runId, types: [RunEventType.Output]);
            if (run.Status == JobStatus.Running && outputs.Count > 0)
            {
                sawOutputWhileRunning = true;
                break;
            }

            await Task.Delay(20);
        }

        Assert.True(sawOutputWhileRunning, "Expected at least one output event while run status was Running.");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);
        Assert.True(result.IsSuccess);
        Assert.Equal([1, 2, 3], result.GetResult<List<int>>());

        var allOutputEvents = await harness.Store.GetEventsAsync(runId, types: [RunEventType.Output]);
        var completionEvents = await harness.Store.GetEventsAsync(runId, types: [RunEventType.OutputComplete]);
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
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        var count = 0;
        harness.Host.AddJob("Continuous", () => Interlocked.Increment(ref count))
            .Continuous();

        await harness.StartAsync();

        var firstRunId = await harness.Client.TriggerAsync("Continuous");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var first = await harness.Client.WaitAsync(firstRunId, cts.Token);
        Assert.True(first.IsSuccess);

        var second = await WaitForAnotherRunAsync(harness.Store, "Continuous", firstRunId, cts.Token);
        Assert.NotNull(second);
    }

    [Fact]
    public async Task ContinuousJob_Successor_UsesDeduplicationIdBasedOnCompletedRun()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Continuous", () => 1).Continuous();

        await harness.StartAsync();

        var firstRunId = await harness.Client.TriggerAsync("Continuous");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var first = await harness.Client.WaitAsync(firstRunId, cts.Token);
        Assert.True(first.IsSuccess);

        var successor = await WaitForAnotherRunAsync(harness.Store, "Continuous", firstRunId, cts.Token);
        Assert.NotNull(successor);
        var successorRun = successor;
        Assert.StartsWith("continuous:", successorRun.DeduplicationId);

        var predecessorId = successorRun.DeduplicationId!["continuous:".Length..];
        var predecessor = await harness.Store.GetRunAsync(predecessorId, cts.Token);
        Assert.NotNull(predecessor);
        Assert.Equal("Continuous", predecessor.JobName);
        Assert.Equal(JobStatus.Completed, predecessor.Status);
    }

    [Fact]
    public async Task ContinuousJob_SeedsToMaxConcurrency_OnStartup()
    {
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

        await harness.StartAsync();

        var page = await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = "ContinuousSeedStartup",
                ExactJobName = true,
                IsTerminal = false
            }, 0, 20),
            p => p.Items.Count == 3,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            "Timed out waiting for startup continuous seeding to create max-concurrency runs.");

        Assert.Equal(3, page.Items.Count);
        Assert.InRange(page.Items.Count(r => r.Status == JobStatus.Running), 1, 3);
    }

    [Fact]
    public async Task ContinuousJob_DoesNotCreateExtraRuns_WhenManualBacklogAlreadySatisfiesMax()
    {
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

        await harness.StartAsync();

        await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = "ContinuousBacklog",
                ExactJobName = true,
                Status = JobStatus.Running
            }, 0, 10),
            p => p.Items.Count == 1,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            "Timed out waiting for first continuous run to start.");

        await harness.Client.TriggerAsync("ContinuousBacklog");
        await harness.Client.TriggerAsync("ContinuousBacklog");

        await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = "ContinuousBacklog",
                ExactJobName = true,
                IsTerminal = false
            }, 0, 20),
            p => p.Items.Count == 3,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            "Timed out waiting for manual backlog to reach three non-terminal runs.");

        await permits.Writer.WriteAsync(true);

        var settled = await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = "ContinuousBacklog",
                ExactJobName = true,
                IsTerminal = false
            }, 0, 20),
            p => p.Items.Count == 2 && p.Items.Count(r => r.Status == JobStatus.Running) == 1,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            "Timed out waiting for backlog to drain without over-seeding.");

        Assert.Equal(2, settled.Items.Count);
    }

    [Fact]
    public async Task StaleRunningRun_IsRecoveredAndReclaimed_ByAnotherNodeLoop()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.InactiveThreshold = TimeSpan.FromMilliseconds(120);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("CrashRecovery", () => 7).WithRetry(1);

        await harness.StartAsync();

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

        await harness.Store.CreateRunsAsync([stale]);

        var recovered = await TestWait.PollUntilAsync(
            () => harness.Store.GetRunAsync(stale.Id),
            r => r.Status == JobStatus.Completed,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            "Timed out waiting for stale running run to be recovered and completed.");

        Assert.Equal(2, recovered.Attempt);
    }

    [Fact]
    public async Task Filters_GlobalThenJob_OrderIsDeterministic()
    {
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

        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("Filtered");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);

        Assert.True(result.IsSuccess);
        var sink = harness.Provider.GetRequiredService<TraceSink>();
        Assert.Equal(
            ["global-before", "job-before", "handler", "job-after", "global-after"],
            sink.Events.ToArray());
    }

    [Fact]
    public async Task LifecycleCallbacks_GlobalAndJob_AreInvoked()
    {
        var globalRetry = 0;
        var globalDead = 0;
        var globalSuccess = 0;
        var jobRetry = 0;
        var jobDead = 0;
        var attempts = 0;

        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
            options.OnRetry((JobContext _) => Interlocked.Increment(ref globalRetry));
            options.OnDeadLetter((JobContext _) => Interlocked.Increment(ref globalDead));
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
            .WithRetry(1)
            .OnRetry((JobContext _) => Interlocked.Increment(ref jobRetry))
            .OnDeadLetter((JobContext _) => Interlocked.Increment(ref jobDead));

        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("CallbackFlaky");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);

        Assert.True(result.IsFailure);
        Assert.Equal(1, globalRetry);
        Assert.Equal(1, globalDead);
        Assert.Equal(0, globalSuccess);
        Assert.Equal(1, jobRetry);
        Assert.Equal(1, jobDead);
    }

    [Fact]
    public async Task DeadLetterError_IncludesExceptionTypeAndMessage()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("DeadLetterDetails", () => { throw new InvalidOperationException("dead-letter-details"); })
            .WithRetry(0);

        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("DeadLetterDetails");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);

        Assert.True(result.IsFailure);

        var run = await harness.Store.GetRunAsync(runId, cts.Token);
        Assert.NotNull(run);
        Assert.Equal(JobStatus.DeadLetter, run.Status);
        Assert.Contains("System.InvalidOperationException", run.Error, StringComparison.Ordinal);
        Assert.Contains("dead-letter-details", run.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RetryAndDeadLetter_EmitFrameworkLogEventsPerAttempt()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("RetryFrameworkLogs",
                () => { throw new InvalidOperationException("retry-framework-logs"); })
            .WithRetry(1);

        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("RetryFrameworkLogs");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);

        Assert.True(result.IsFailure);

        var logEvents = await harness.Store.GetEventsAsync(runId, 0, [RunEventType.Log], null, cts.Token);
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
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("RetryFailureEvents",
                () => { throw new InvalidOperationException("retry-failure-events"); })
            .WithRetry(1);

        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("RetryFailureEvents");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);

        Assert.True(result.IsFailure);

        var failureEvents =
            await harness.Store.GetEventsAsync(runId, 0, [RunEventType.AttemptFailure], null, cts.Token);
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

        await harness.StartAsync();

        var total = await harness.Client.RunAsync<int>("SumAsyncEnumerable", new { values = new[] { 1, 2, 3, 4 } });
        Assert.Equal(10, total);
    }

    [Fact]
    public async Task TriggerAsync_InsideRun_AutoLinksParentAndRoot()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("NestedChild", () => 7);
        harness.Host.AddJob("NestedParent", async (IJobClient client) => await client.TriggerAsync("NestedChild"));

        await harness.StartAsync();

        var parentRunId = await harness.Client.TriggerAsync("NestedParent");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var childRunId = await harness.Client.WaitAsync<string>(parentRunId, cts.Token);

        var childRun = await harness.Store.GetRunAsync(childRunId, cts.Token);
        Assert.NotNull(childRun);
        Assert.Equal(parentRunId, childRun.ParentRunId);
        Assert.Equal(parentRunId, childRun.RootRunId);
    }

    [Fact]
    public async Task TriggerAllAsync_InsideRun_AutoLinksCoordinatorAndChildrenToOuterRoot()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("NestedBatchChild", (int value) => value);
        harness.Host.AddJob("NestedBatchParent", async (IJobClient client) =>
            await client.TriggerAllAsync("NestedBatchChild", [new { value = 1 }, new { value = 2 }]));

        await harness.StartAsync();

        var parentRunId = await harness.Client.TriggerAsync("NestedBatchParent");
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var batchCoordinatorId = await harness.Client.WaitAsync<string>(parentRunId, cts.Token);

        var coordinator = await harness.Store.GetRunAsync(batchCoordinatorId, cts.Token);
        Assert.NotNull(coordinator);
        Assert.Equal(parentRunId, coordinator.ParentRunId);
        Assert.Equal(parentRunId, coordinator.RootRunId);

        var children = await harness.Store.GetRunsAsync(new() { ParentRunId = batchCoordinatorId }, 0, 20, cts.Token);
        Assert.Equal(2, children.Items.Count);
        Assert.All(children.Items, child =>
        {
            Assert.Equal(batchCoordinatorId, child.ParentRunId);
            Assert.Equal(parentRunId, child.RootRunId);
        });
    }

    [Fact]
    public async Task ParameterBinder_MapsJsonArrayToList()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumList", (List<int> values) => values.Sum());

        await harness.StartAsync();

        var total = await harness.Client.RunAsync<int>("SumList", new { values = new[] { 2, 3, 5 } });
        Assert.Equal(10, total);
    }

    [Fact]
    public async Task StreamingHandler_BindsScalarArguments()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("StreamFromCount", StreamFromCount);

        await harness.StartAsync();

        var values = await harness.Client.RunAsync<List<int>>("StreamFromCount", new { count = 3 });
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
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumFromList", (List<int> input) => input.Sum());

        await harness.StartAsync();

        var total = await harness.Client.RunAsync<int>("SumFromList", new
        {
            input = ProduceDelayedRange(1, 5, 10)
        });

        Assert.Equal(15, total);
    }

    [Fact]
    public async Task AsyncEnumerableArgument_StreamsThroughHandlerIncrementally()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(15);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(30);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("DoubleStream", DoubleStream);

        await harness.StartAsync();

        var started = DateTimeOffset.UtcNow;
        var outputs = new List<int>();
        await foreach (var item in harness.Client.StreamAsync<int>("DoubleStream", new
                       {
                           input = ProduceDelayedRange(1, 3, 120)
                       }))
        {
            outputs.Add(item);
            if (outputs.Count == 1)
            {
                var firstDelay = DateTimeOffset.UtcNow - started;
                Assert.True(firstDelay < TimeSpan.FromMilliseconds(650));
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
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumFromListMissing", (List<int> input) => input.Sum());

        await harness.StartAsync();

        var started = Stopwatch.StartNew();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var ex = await Assert.ThrowsAsync<JobRunFailedException>(async () =>
            await harness.Client.RunAsync<int>("SumFromListMissing", new { }, cts.Token));
        started.Stop();

        Assert.Contains("Unable to bind parameter 'input'", ex.ToString(), StringComparison.Ordinal);
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(1),
            $"Missing undeclared stream binding should fail fast. Elapsed: {started.Elapsed}.");
    }

    [Fact]
    public async Task DeclaredInputStream_CanDelayFirstItemWithoutTimingOut()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("SumFromDelayedList", (List<int> input) => input.Sum());

        await harness.StartAsync();

        var total = await harness.Client.RunAsync<int>("SumFromDelayedList", new
        {
            input = ProduceDelayedRange(1, 5, 600)
        });

        Assert.Equal(15, total);
    }

    [Fact]
    public async Task AsyncEnumerableArgument_SourceCancellation_DoesNotSilentlyCompleteWithPartialInput()
    {
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

        await harness.StartAsync();

        var runId = await harness.Client.TriggerAsync("SumSourceCancelledStream", new
        {
            values = SourceCancelledStream()
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await harness.Client.WaitAsync(runId, cts.Token);

        Assert.True(result.IsFailure);
        Assert.NotEqual(JobStatus.Completed, result.Status);

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
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("MergeStreams", MergeStreams);

        await harness.StartAsync();

        var values = await harness.Client.RunAsync<List<int>>("MergeStreams", new
        {
            left = ProduceDelayedRange(1, 3, 5),
            right = ProduceDelayedRange(10, 12, 7)
        });

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

        await harness.StartAsync();

        var originalRunId = await harness.Client.TriggerAsync("SumInputStreamRerun", new
        {
            values = ProduceDelayedRange(1, 3, 5)
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var original = await harness.Client.WaitAsync(originalRunId, cts.Token);
        Assert.True(original.IsSuccess);
        Assert.Equal(6, original.GetResult<int>());

        var rerunId = await harness.Client.RerunAsync(originalRunId, cts.Token);
        var rerun = await harness.Client.WaitAsync(rerunId, cts.Token);
        Assert.True(rerun.IsSuccess);
        Assert.Equal(6, rerun.GetResult<int>());
    }

    [Fact]
    public async Task RerunAsync_BatchCoordinator_ReplaysChildStreamInputEvents()
    {
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

        await harness.StartAsync();

        var originalBatchId = await harness.Client.TriggerAllAsync("SumBatchInputStreamRerun", [
            new { values = ProduceDelayedRange(1, 2, 5) },
            new { values = ProduceDelayedRange(10, 20, 5) }
        ]);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(8));
        var firstPass = new List<RunResult>();
        await foreach (var result in harness.Client.WaitEachAsync(originalBatchId, cts.Token))
        {
            firstPass.Add(result);
        }

        Assert.Equal(2, firstPass.Count);
        Assert.All(firstPass, result => Assert.True(result.IsSuccess));
        Assert.Equal([3, 165], firstPass.Select(result => result.GetResult<int>()).OrderBy(v => v).ToArray());

        var rerunBatchId = await harness.Client.RerunAsync(originalBatchId, cts.Token);
        var rerunPass = new List<RunResult>();
        await foreach (var result in harness.Client.WaitEachAsync(rerunBatchId, cts.Token))
        {
            rerunPass.Add(result);
        }

        Assert.Equal(2, rerunPass.Count);
        Assert.All(rerunPass, result => Assert.True(result.IsSuccess));
        Assert.Equal([3, 165], rerunPass.Select(result => result.GetResult<int>()).OrderBy(v => v).ToArray());
    }

    [Fact]
    public async Task RetentionService_PurgesTerminalRuns_ByRetentionInterval()
    {
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromSeconds(10);
            options.RetentionCheckInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = TimeSpan.Zero;
        });

        await harness.StartAsync();

        var completed = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = "retention-job",
            Status = JobStatus.Completed,
            CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
            NotBefore = DateTimeOffset.UtcNow.AddMinutes(-1),
            CompletedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
            Progress = 1
        };

        await harness.Store.CreateRunsAsync([completed]);

        var timeout = DateTimeOffset.UtcNow.AddSeconds(5);
        while (DateTimeOffset.UtcNow < timeout)
        {
            if (await harness.Store.GetRunAsync(completed.Id) is null)
            {
                return;
            }

            await Task.Delay(25);
        }

        throw new TimeoutException("Retention service did not purge the completed run in time.");
    }

    private static async Task<JobRun> WaitForStatusAsync(IJobStore store, string runId, JobStatus status)
    {
        return await TestWait.PollUntilAsync(
            () => store.GetRunAsync(runId),
            run => run.Status == status,
            TimeSpan.FromSeconds(5),
            TimeSpan.FromMilliseconds(20),
            $"Timed out waiting for run '{runId}' to reach status {status}.");
    }

    private static async Task<JobRun?> WaitForAnotherRunAsync(IJobStore store, string jobName, string excludeRunId,
        CancellationToken cancellationToken)
    {
        try
        {
            return await TestWait.PollUntilAsync(
                async () =>
                {
                    var page = await store.GetRunsAsync(new() { JobName = jobName, ExactJobName = true }, 0,
                        20, cancellationToken);
                    return page.Items.FirstOrDefault(r =>
                        !string.Equals(r.Id, excludeRunId, StringComparison.Ordinal));
                },
                _ => true,
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(20),
                "Timed out waiting for another run.");
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

    private static Task<TestHarness> CreateHarnessAsync(Action<SurefireOptions> configure,
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
        var hostedServices = provider.GetServices<IHostedService>().ToArray();

        return Task.FromResult(new TestHarness(provider, host, client, store, hostedServices));
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

    private sealed class TestHost(IServiceProvider services) : IHost
    {
        public IServiceProvider Services { get; } = services;

        public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void Dispose()
        {
            if (Services is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
    }

    private sealed class TestHarness(
        ServiceProvider provider,
        TestHost host,
        IJobClient client,
        IJobStore store,
        IReadOnlyList<IHostedService> hostedServices) : IAsyncDisposable
    {
        public ServiceProvider Provider { get; } = provider;
        public TestHost Host { get; } = host;
        public IJobClient Client { get; } = client;
        public IJobStore Store { get; } = store;

        public async ValueTask DisposeAsync()
        {
            foreach (var service in hostedServices.Reverse())
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StoppingAsync(CancellationToken.None);
                }
            }

            foreach (var service in hostedServices.Reverse())
            {
                await service.StopAsync(CancellationToken.None);
            }

            foreach (var service in hostedServices.Reverse())
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StoppedAsync(CancellationToken.None);
                }
            }

            await Provider.DisposeAsync();
        }

        public async Task StartAsync()
        {
            foreach (var service in hostedServices)
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StartingAsync(CancellationToken.None);
                }
            }

            foreach (var service in hostedServices)
            {
                await service.StartAsync(CancellationToken.None);
            }

            foreach (var service in hostedServices)
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StartedAsync(CancellationToken.None);
                }
            }
        }
    }
}