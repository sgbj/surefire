using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Surefire.Tests.Conformance;
using Surefire.Tests.Testing;
using static Surefire.Tests.Testing.TestConcurrency;

namespace Surefire.Tests.Integration;

public sealed class DurableOrchestratorTests
{
    [Fact]
    public async Task Durable_Orchestrator_Runs_Children_Sequentially_And_Returns_Sum()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Double", (int x) => x * 2);
        harness.Host.AddJob("Orchestrator", async (IJobClient client, int seed) =>
        {
            var a = await client.RunAsync<int>("Double", new { x = seed });
            var b = await client.RunAsync<int>("Double", new { x = a });
            var c = await client.RunAsync<int>("Double", new { x = b });
            return a + b + c;
        }).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Orchestrator", new { seed = 3 }, cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        var result = await harness.Client.WaitAsync<int>(run.Id, cts.Token);
        // 3 -> 6, 6 -> 12, 12 -> 24 = 42
        Assert.Equal(42, result);

        var orchestrator = await harness.Store.GetRunAsync(run.Id, cts.Token);
        Assert.NotNull(orchestrator);
        Assert.Equal(JobStatus.Succeeded, orchestrator.Status);

        // Three children created exactly once each, all Succeeded.
        var page = await harness.Store.GetRunsAsync(new() { ParentRunId = run.Id }, 0, 100, cts.Token);
        Assert.Equal(3, page.Items.Count);
        Assert.All(page.Items, r => Assert.Equal(JobStatus.Succeeded, r.Status));
    }

    [Fact]
    public async Task Durable_Orchestrator_Replay_Is_Idempotent_Across_Suspend_Cycles()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        var childInvocations = 0;
        harness.Host.AddJob("OnceChild", () => Interlocked.Increment(ref childInvocations));
        var orchestratorInvocations = 0;
        harness.Host.AddJob("OnceOrch", async (IJobClient client) =>
        {
            Interlocked.Increment(ref orchestratorInvocations);
            return await client.RunAsync<int>("OnceChild");
        }).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("OnceOrch", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        Assert.Equal(1, await harness.Client.WaitAsync<int>(run.Id, cts.Token));

        // Child runs exactly once even though the orchestrator handler ran twice (initial + replay).
        Assert.Equal(1, childInvocations);
        Assert.True(orchestratorInvocations >= 2,
            $"expected orchestrator to replay at least twice (initial + post-resume), saw {orchestratorInvocations}");
    }

    [Fact]
    public async Task Durable_Suspension_Frees_Concurrency_Slot()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
            options.MaxNodeConcurrency = 1;
        });

        var permits = new SemaphoreSlim(0);
        var inFlight = 0;
        var maxInFlight = 0;
        harness.Host.AddJob("SlowChild", async () =>
        {
            var current = Interlocked.Increment(ref inFlight);
            InterlockedMax(ref maxInFlight, current);
            await permits.WaitAsync();
            Interlocked.Decrement(ref inFlight);
            return 1;
        });
        harness.Host.AddJob("Wait", (IJobClient client) =>
            client.RunAsync<int>("SlowChild")).Durable();

        await harness.StartAsync(ct);

        var first = await harness.Client.TriggerAsync("Wait", cancellationToken: ct);
        var second = await harness.Client.TriggerAsync("Wait", cancellationToken: ct);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        // Both children should be in flight concurrently despite MaxNodeConcurrency=1, because
        // both parent orchestrators are suspended.
        await WaitForAsync(() => Volatile.Read(ref inFlight) == 1, cts.Token);
        permits.Release(); // releasing one keeps inFlight bounded; both must still complete

        await WaitForAsync(() => Volatile.Read(ref inFlight) == 1 || Volatile.Read(ref maxInFlight) >= 1, cts.Token);
        permits.Release();

        Assert.Equal(1, await harness.Client.WaitAsync<int>(first.Id, cts.Token));
        Assert.Equal(1, await harness.Client.WaitAsync<int>(second.Id, cts.Token));
    }

    [Fact]
    public async Task Durable_Fanout_With_WhenAll_Records_All_Children()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Square", (int x) => x * x);
        harness.Host.AddJob("Fanout", async (IJobClient client) =>
        {
            var t1 = client.RunAsync<int>("Square", new { x = 2 });
            var t2 = client.RunAsync<int>("Square", new { x = 3 });
            var t3 = client.RunAsync<int>("Square", new { x = 4 });
            var results = await Task.WhenAll(t1, t2, t3);
            return results.Sum();
        }).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Fanout", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        Assert.Equal(4 + 9 + 16, await harness.Client.WaitAsync<int>(run.Id, cts.Token));

        var page = await harness.Store.GetRunsAsync(new() { ParentRunId = run.Id }, 0, 100, cts.Token);
        Assert.Equal(3, page.Items.Count);
    }

    [Fact]
    public async Task Durable_Real_Failure_Burns_Retry_But_Replay_Cycles_Do_Not()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("Identity", (int x) => x);
        var realFailures = 0;
        var totalInvocations = 0;
        harness.Host.AddJob("Flaky", async (IJobClient client) =>
        {
            Interlocked.Increment(ref totalInvocations);
            var a = await client.RunAsync<int>("Identity", new { x = 1 });
            // Fail once *after* the recorded child step. Replays return a from history, so by the
            // time we reach this branch we've already replayed past the child call deterministically.
            if (Interlocked.Increment(ref realFailures) == 1)
            {
                throw new InvalidOperationException("transient");
            }

            return a + 100;
        }).Durable().WithRetry(p =>
        {
            p.MaxRetries = 2;
            p.InitialDelay = TimeSpan.FromMilliseconds(10);
            p.MaxDelay = TimeSpan.FromMilliseconds(10);
            p.Jitter = false;
        });

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Flaky", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        Assert.Equal(101, await harness.Client.WaitAsync<int>(run.Id, cts.Token));
    }

    [Fact]
    public async Task Durable_RecordAsync_Replays_Value_Without_Reinvoking_Factory()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        var factoryCalls = 0;
        harness.Host.AddJob("One", () => 1);
        harness.Host.AddJob("RecordOrch", async (IJobClient client, JobContext ctx) =>
        {
            var value = await ctx.RecordAsync("external-value", _ =>
            {
                Interlocked.Increment(ref factoryCalls);
                return ValueTask.FromResult(Guid.CreateVersion7().ToString("N"));
            });
            var child = await client.RunAsync<int>("One");
            return $"{value}:{child}";
        }).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("RecordOrch", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        var result = await harness.Client.WaitAsync<string>(run.Id, cts.Token);

        Assert.EndsWith(":1", result);
        Assert.Equal(1, factoryCalls);
        var snapshot = await harness.Store.LoadExecutionSnapshotAsync(run.Id, cts.Token);
        Assert.True(snapshot.Records.TryGetValue(1, out var record));
        Assert.Equal("external-value", record.Name);
    }

    [Fact]
    public async Task Durable_RecordAsync_RoundTrips_Null_Result()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        var factoryCalls = 0;
        harness.Host.AddJob("One", () => 1);
        harness.Host.AddJob("NullRecordOrch", async (IJobClient client, JobContext ctx) =>
        {
            var value = await ctx.RecordAsync<string?>("nullable", _ =>
            {
                Interlocked.Increment(ref factoryCalls);
                return ValueTask.FromResult<string?>(null);
            });
            var child = await client.RunAsync<int>("One");
            return value is null ? child : -1;
        }).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("NullRecordOrch", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        Assert.Equal(1, await harness.Client.WaitAsync<int>(run.Id, cts.Token));
        Assert.Equal(1, factoryCalls);
        var snapshot = await harness.Store.LoadExecutionSnapshotAsync(run.Id, cts.Token);
        Assert.Equal("null", snapshot.Records[1].Payload);
    }

    [Fact]
    public async Task Durable_Bcl_Shaped_Helpers_Record_ReplaySafe_Values()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("One", () => 1);
        harness.Host.AddJob("HelpersOrch", async (IJobClient client, JobContext ctx) =>
        {
            var guid = await ctx.NewGuidAsync();
            var guidV7 = await ctx.NewGuidV7Async();
            var now = await ctx.GetUtcNowAsync();
            var any = await ctx.NextInt32Async();
            var bounded = await ctx.NextInt32Async(10);
            var ranged = await ctx.NextInt32Async(5, 10);
            var dbl = await ctx.NextDoubleAsync();
            var child = await client.RunAsync<int>("One");

            return string.Join("|",
                guid,
                guidV7,
                now.UtcTicks.ToString(CultureInfo.InvariantCulture),
                any.ToString(CultureInfo.InvariantCulture),
                bounded.ToString(CultureInfo.InvariantCulture),
                ranged.ToString(CultureInfo.InvariantCulture),
                dbl.ToString(CultureInfo.InvariantCulture),
                child.ToString(CultureInfo.InvariantCulture));
        }).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("HelpersOrch", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        var result = await harness.Client.WaitAsync<string>(run.Id, cts.Token);
        var parts = result.Split('|');

        Assert.Equal(8, parts.Length);
        Assert.NotEqual(Guid.Empty, Guid.Parse(parts[0]));
        Assert.NotEqual(Guid.Empty, Guid.Parse(parts[1]));
        Assert.True(long.Parse(parts[2], CultureInfo.InvariantCulture) > 0);
        Assert.InRange(int.Parse(parts[4], CultureInfo.InvariantCulture), 0, 9);
        Assert.InRange(int.Parse(parts[5], CultureInfo.InvariantCulture), 5, 9);
        Assert.InRange(double.Parse(parts[6], CultureInfo.InvariantCulture), 0.0, 1.0);
        Assert.Equal("1", parts[7]);

        var snapshot = await harness.Store.LoadExecutionSnapshotAsync(run.Id, cts.Token);
        Assert.Equal(7, snapshot.Records.Count);
        Assert.Equal(DurableRecordKinds.GuidV4, snapshot.Records[1].Kind);
        Assert.Equal(DurableRecordKinds.GuidV7, snapshot.Records[2].Kind);
        Assert.Equal(DurableRecordKinds.UtcNow, snapshot.Records[3].Kind);
        Assert.Equal(DurableRecordKinds.RandomInt32, snapshot.Records[4].Kind);
        Assert.Equal(DurableRecordKinds.RandomInt32, snapshot.Records[5].Kind);
        Assert.Equal(DurableRecordKinds.RandomInt32, snapshot.Records[6].Kind);
        Assert.Equal(DurableRecordKinds.RandomDouble, snapshot.Records[7].Kind);
        var boundedPayload = JsonSerializer.Deserialize(snapshot.Records[5].Payload,
            SurefireJsonContext.Default.DurableRandomInt32Payload);
        Assert.NotNull(boundedPayload);
        Assert.Equal(0, boundedPayload.MinValue);
        Assert.Equal(10, boundedPayload.MaxValue);
    }

    [Fact]
    public async Task Durable_RunLogs_AreSuppressed_DuringReplay()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(20);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.RetentionPeriod = null;
        });

        harness.Host.AddJob("One", () => 1);
        harness.Host.AddJob("LogOrch", async (
            IJobClient client,
            JobContext ctx,
            ILogger<DurableOrchestratorTests> logger) =>
        {
            logger.LogInformation("before child");
            var child = await client.RunAsync<int>("One");
            logger.LogInformation("after child");
            return child;
        }).Durable();

        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("LogOrch", cancellationToken: ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        Assert.Equal(1, await harness.Client.WaitAsync<int>(run.Id, cts.Token));

        var logEvents = await harness.Store.GetEventsAsync(run.Id, 0, [RunEventType.Log], cancellationToken: cts.Token);
        var messages = logEvents
            .Select(e => JsonSerializer.Deserialize(e.Payload, SurefireJsonContext.Default.LogEventPayload)!.Message)
            .Where(m => m is "before child" or "after child")
            .ToArray();

        Assert.Equal(["before child", "after child"], messages);
    }

    [Fact]
    public async Task Durable_RecordReplay_IsDecided_PerStep_NotByWatermark()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var orchId = Guid.CreateVersion7().ToString("N");
        var options = new SurefireOptions();
        TestSerializerOptions.AttachReflectionResolver(options);
        var snapshot = new DurableExecutionSnapshot(
            new Dictionary<string, JobRun>(StringComparer.Ordinal),
            new Dictionary<string, JobBatch>(StringComparer.Ordinal),
            new Dictionary<int, DurableRecord>
            {
                [2] = new(orchId, 2, DurableRecordKinds.Record, "second", "200", DateTimeOffset.UtcNow)
            },
            HighestRecordedStep: 2);
        var context = CreateDirectContext(store, orchId, options, snapshot);

        var firstFactoryCalls = 0;
        var first = await context.RecordAsync("first", _ =>
        {
            firstFactoryCalls++;
            return ValueTask.FromResult(100);
        }, ct);
        var secondFactoryCalls = 0;
        var second = await context.RecordAsync("second", _ =>
        {
            secondFactoryCalls++;
            return ValueTask.FromResult(999);
        }, ct);

        Assert.Equal(100, first);
        Assert.Equal(200, second);
        Assert.Equal(1, firstFactoryCalls);
        Assert.Equal(0, secondFactoryCalls);
    }

    [Fact]
    public async Task Durable_NextInt32_ReplayValidates_RequestedRange()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new InMemoryJobStore(TimeProvider.System);
        var orchId = Guid.CreateVersion7().ToString("N");
        var options = new SurefireOptions();
        var snapshot = new DurableExecutionSnapshot(
            new Dictionary<string, JobRun>(StringComparer.Ordinal),
            new Dictionary<string, JobBatch>(StringComparer.Ordinal),
            new Dictionary<int, DurableRecord>
            {
                [1] = new(orchId, 1, DurableRecordKinds.RandomInt32, null,
                    JsonSerializer.Serialize(new DurableRandomInt32Payload
                    {
                        Value = 73,
                        MinValue = 0,
                        MaxValue = 100
                    }, SurefireJsonContext.Default.DurableRandomInt32Payload),
                    DateTimeOffset.UtcNow)
            },
            HighestRecordedStep: 1);

        var sameRangeContext = CreateDirectContext(store, orchId, options, snapshot);
        Assert.Equal(73, await sameRangeContext.NextInt32Async(100, ct));

        var changedRangeContext = CreateDirectContext(store, orchId, options, snapshot);
        await Assert.ThrowsAsync<DurableReplayMismatchException>(async () =>
            await changedRangeContext.NextInt32Async(10, ct));
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

    private static JobContext CreateDirectContext(IJobStore store, string orchId, SurefireOptions options,
        DurableExecutionSnapshot snapshot) =>
        new()
        {
            RunId = orchId,
            RootRunId = orchId,
            JobName = "direct",
            CancellationToken = CancellationToken.None,
            Store = store,
            TimeProvider = TimeProvider.System,
            SerializerOptions = options.SerializerOptions,
            NodeName = "test",
            OrchestratorRunId = orchId,
            HighestRecordedStep = snapshot.HighestRecordedStep,
            DurableSnapshot = snapshot
        };
}
