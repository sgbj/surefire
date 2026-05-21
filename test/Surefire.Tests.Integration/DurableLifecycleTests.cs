using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Surefire.Tests.Conformance;
using Surefire.Tests.Testing;
using static Surefire.Tests.Testing.TestConcurrency;

namespace Surefire.Tests.Integration;

/// <summary>
///     Covers the cancellation / shutdown / replay-streaming surface for durable orchestrators.
///     Each test pins one row of the durable behavior matrix so a future refactor can't silently
///     break a path.
/// </summary>
public sealed class DurableLifecycleTests
{

    [Fact]
    public async Task Durable_StreamBatch_Does_Not_Cancel_Batch_On_First_Suspend()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("StreamChild", (int x) => x * 2);
        harness.Host.AddJob("StreamOrch", async (IJobClient client) =>
        {
            var sum = 0;
            await foreach (var v in client.StreamBatchAsync<int>("StreamChild", new object?[]
                           {
                               new { x = 1 }, new { x = 2 }, new { x = 3 }
                           }))
            {
                sum += v;
            }

            return sum;
        }).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("StreamOrch", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        // Should succeed with the doubled sum.
        Assert.Equal(12, await harness.Client.WaitAsync<int>(run.Id, cts.Token));

        // Verify none of the children ended up Canceled along the way.
        var page = await harness.Store.GetRunsAsync(new() { ParentRunId = run.Id }, 0, 100, cts.Token);
        Assert.Equal(3, page.Items.Count);
        Assert.All(page.Items, r => Assert.Equal(JobStatus.Succeeded, r.Status));
    }

    [Fact]
    public async Task Durable_StreamAsync_Does_Not_Cancel_Run_On_First_Suspend()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Producer", () =>
        {
            return Produce();

            async IAsyncEnumerable<int> Produce()
            {
                yield return 1;
                yield return 2;
                yield return 3;
                await Task.CompletedTask;
            }
        });

        harness.Host.AddJob("StreamOrch", async (IJobClient client) =>
        {
            var sum = 0;
            await foreach (var v in client.StreamAsync<int>("Producer"))
            {
                sum += v;
            }

            return sum;
        }).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("StreamOrch", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        Assert.Equal(6, await harness.Client.WaitAsync<int>(run.Id, cts.Token));

        var page = await harness.Store.GetRunsAsync(new() { ParentRunId = run.Id }, 0, 10, cts.Token);
        Assert.Single(page.Items);
        Assert.Equal(JobStatus.Succeeded, page.Items[0].Status);
    }


    [Fact]
    public async Task Durable_Attempt_Stays_Failure_Aware_Across_Suspend_Resume()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        var observedAttempts = new List<int>();
        var observedIsReplaying = new List<bool>();
        harness.Host.AddJob("Multiply", (int x) => x * 10);
        harness.Host.AddJob("Counter", async (IJobClient client, JobContext ctx) =>
        {
            lock (observedAttempts)
            {
                observedAttempts.Add(ctx.Attempt);
                observedIsReplaying.Add(ctx.IsReplaying);
            }

            var a = await client.RunAsync<int>("Multiply", new { x = 1 });
            var b = await client.RunAsync<int>("Multiply", new { x = 2 });
            return a + b;
        }).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Counter", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        Assert.Equal(30, await harness.Client.WaitAsync<int>(run.Id, cts.Token));

        // Attempt should be 1 throughout (no real failures). IsReplaying should be true on at
        // least one of the entries (the post-suspend replays).
        Assert.All(observedAttempts, a => Assert.Equal(1, a));
        Assert.True(observedIsReplaying.Any(r => r),
            "expected IsReplaying = true on at least one replay entry");

        var orchestrator = await harness.Store.GetRunAsync(run.Id, cts.Token);
        Assert.NotNull(orchestrator);
        Assert.Equal(1, orchestrator.Attempt);
        Assert.True(orchestrator.ReplayCount >= 1,
            $"expected at least 1 replay, saw {orchestrator.ReplayCount}");
    }

    [Fact]
    public async Task Durable_IsReplaying_Flips_From_True_To_False_Past_Recorded_Steps()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        // Capture IsReplaying values at three points across the run lifecycle: before any
        // step, after step 1, after step 2. We expect that on the LAST replay, IsReplaying
        // transitions from true (replaying step 1 and step 2) to false (new territory after
        // the final step).
        var capturedByPosition = new List<(int Position, bool IsReplaying)>();
        harness.Host.AddJob("Square", (int x) => x * x);
        harness.Host.AddJob("Boundary", async (IJobClient client, JobContext ctx) =>
        {
            lock (capturedByPosition)
            {
                capturedByPosition.Add((0, ctx.IsReplaying));
            }

            var a = await client.RunAsync<int>("Square", new { x = 2 });
            lock (capturedByPosition)
            {
                capturedByPosition.Add((1, ctx.IsReplaying));
            }

            var b = await client.RunAsync<int>("Square", new { x = 3 });
            lock (capturedByPosition)
            {
                capturedByPosition.Add((2, ctx.IsReplaying));
            }

            return a + b;
        }).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Boundary", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        Assert.Equal(13, await harness.Client.WaitAsync<int>(run.Id, cts.Token));

        // Invariants the fine-grained model must satisfy regardless of how many replay cycles
        // occur:
        //   1. At some point during a replay, IsReplaying is true while inside recorded
        //      territory (position 0 or 1, before passing the recorded last step).
        //   2. The final execution sees IsReplaying = false at position 2 - the point past
        //      every recorded step, i.e. "new territory" where any new code path runs once.
        Assert.True(capturedByPosition.Any(e => e.Position < 2 && e.IsReplaying),
            "expected IsReplaying = true at a replay entry inside recorded territory; saw: " +
            $"[{string.Join(",", capturedByPosition)}]");
        Assert.True(capturedByPosition.Any(e => e.Position == 2 && !e.IsReplaying),
            "expected IsReplaying = false past the last recorded step; saw: " +
            $"[{string.Join(",", capturedByPosition)}]");
    }


    [Fact]
    public async Task Durable_Suspended_Orchestrator_Cancels_Cascades_To_Descendants()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        var permits = new SemaphoreSlim(0);
        harness.Host.AddJob("Blocker", async (CancellationToken jobCt) =>
        {
            await permits.WaitAsync(jobCt);
            return 1;
        });
        harness.Host.AddJob("Wait", (IJobClient client) =>
            client.RunAsync<int>("Blocker")).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Wait", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        // Wait until the orchestrator is Suspended (it spawned its child and yielded).
        await WaitForAsync(async () =>
        {
            var r = await harness.Store.GetRunAsync(run.Id, cts.Token);
            return r?.Status == JobStatus.Suspended;
        }, cts.Token);

        await harness.Client.CancelAsync(run.Id, cts.Token);

        await WaitForAsync(async () =>
        {
            var r = await harness.Store.GetRunAsync(run.Id, cts.Token);
            return r?.Status == JobStatus.Canceled;
        }, cts.Token);

        // The blocker child must also have been canceled by the cascade.
        var page = await harness.Store.GetRunsAsync(new() { ParentRunId = run.Id }, 0, 10, cts.Token);
        Assert.Single(page.Items);
        Assert.Equal(JobStatus.Canceled, page.Items[0].Status);

        permits.Release();
    }

    [Fact]
    public async Task Durable_Parent_Cancellation_Cascades_To_Durable_Child()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        var permits = new SemaphoreSlim(0);
        harness.Host.AddJob("Blocker", async (CancellationToken jobCt) =>
        {
            await permits.WaitAsync(jobCt);
            return 1;
        });
        harness.Host.AddJob("InnerDurable", (IJobClient client) =>
            client.RunAsync<int>("Blocker")).Durable();
        harness.Host.AddJob("Outer", (IJobClient client) =>
            client.RunAsync<int>("InnerDurable"));

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Outer", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        // Wait until the durable inner orchestrator is Suspended.
        await WaitForAsync(async () =>
        {
            var children = await harness.Store.GetRunsAsync(
                new() { ParentRunId = run.Id }, 0, 10, cts.Token);
            return children.Items.Any(r => r.Status == JobStatus.Suspended);
        }, cts.Token);

        await harness.Client.CancelAsync(run.Id, cts.Token);

        await WaitForAsync(async () =>
        {
            var r = await harness.Store.GetRunAsync(run.Id, cts.Token);
            return r?.Status is JobStatus.Canceled or JobStatus.Failed;
        }, cts.Token);

        // Every descendant terminated.
        var allDescendants = await harness.Store.GetRunsAsync(
            new() { RootRunId = run.Id }, 0, 100, cts.Token);
        Assert.All(allDescendants.Items, r => Assert.True(r.Status.IsTerminal));

        permits.Release();
    }

    [Fact]
    public async Task Durable_Orchestrator_With_NotAfter_Expiry_DoesNotCancelAfterStart()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        var permits = new SemaphoreSlim(0);
        harness.Host.AddJob("Blocker", async (CancellationToken jobCt) =>
        {
            await permits.WaitAsync(jobCt);
            return 1;
        });
        harness.Host.AddJob("ExpiringWait", (IJobClient client) =>
            client.RunAsync<int>("Blocker")).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("ExpiringWait",
            options: new RunOptions { NotAfter = DateTimeOffset.UtcNow.AddMilliseconds(200) },
            cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        await WaitForAsync(async () =>
        {
            var r = await harness.Store.GetRunAsync(run.Id, cts.Token);
            return r?.Status == JobStatus.Suspended;
        }, cts.Token);

        await Task.Delay(TimeSpan.FromMilliseconds(300), cts.Token);
        var expired = await harness.Store.CancelExpiredRunsWithIdsAsync(cts.Token);
        Assert.DoesNotContain(expired.Runs, r => r.RunId == run.Id);

        var suspended = await harness.Store.GetRunAsync(run.Id, cts.Token);
        Assert.NotNull(suspended);
        Assert.Equal(JobStatus.Suspended, suspended.Status);

        permits.Release();
        Assert.Equal(1, await harness.Client.WaitAsync<int>(run.Id, cts.Token));
    }

    [Fact]
    public async Task Durable_EmptyAwaitSet_DeadLettersWithoutRetry()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        var invocations = 0;
        harness.Host.AddJob("InvalidYield", () =>
        {
            Interlocked.Increment(ref invocations);
            throw new DurableYieldException();
        }).Durable().WithRetry(p =>
        {
            p.MaxRetries = 3;
            p.InitialDelay = TimeSpan.FromMilliseconds(10);
            p.MaxDelay = TimeSpan.FromMilliseconds(10);
            p.Jitter = false;
        });

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("InvalidYield", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        await WaitForAsync(async () =>
        {
            var r = await harness.Store.GetRunAsync(run.Id, cts.Token);
            return r?.Status == JobStatus.Failed;
        }, cts.Token);

        Assert.Equal(1, Volatile.Read(ref invocations));
        var failed = await harness.Store.GetRunAsync(run.Id, cts.Token);
        Assert.NotNull(failed);
        Assert.Equal(1, failed.FailureCount);
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
}
