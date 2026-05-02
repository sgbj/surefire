using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Surefire.Tests.Testing;

namespace Surefire.Tests.Integration;

/// <summary>
///     Contract tests for the hydration resolver, exception semantics, and batch streaming.
/// </summary>
public sealed class HydrationContractTests
{
    [Fact]
    public async Task WaitAsync_NonGeneric_OnFailedRun_ReturnsRunWithoutThrowing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("AlwaysFail", () => ThrowBoom());
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("AlwaysFail", cancellationToken: ct);
        var final = await harness.Client.WaitAsync(run.Id, ct);

        Assert.Equal(JobStatus.Failed, final.Status);
        // Non-generic WaitAsync surfaces the JobRun directly; no exception is raised.
    }

    private static int ThrowBoom() => throw new InvalidOperationException("boom");

    [Fact]
    public async Task WaitAsync_NonGeneric_OnCanceledRun_ReturnsRunWithoutThrowing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        var entered = new TaskCompletionSource();
        harness.Host.AddJob("LongRunning", async (CancellationToken innerCt) =>
        {
            entered.TrySetResult();
            await Task.Delay(TimeSpan.FromMinutes(5), innerCt);
        });
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("LongRunning", cancellationToken: ct);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
        await harness.Client.CancelAsync(run.Id, ct);

        var final = await harness.Client.WaitAsync(run.Id, ct);
        Assert.Equal(JobStatus.Canceled, final.Status);
    }

    [Fact]
    public async Task WaitEachAsync_YieldsAllChildren_RegardlessOfOutcome()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("MixedOutcome", (int code) => Mixed(code));

        static int Mixed(int code) => code switch
        {
            0 => 42,
            1 => throw new InvalidOperationException("boom"),
            _ => throw new OperationCanceledException()
        };

        await harness.StartAsync(ct);

        var batch = await harness.Client.TriggerBatchAsync("MixedOutcome",
            [new { code = 0 }, new { code = 1 }], cancellationToken: ct);

        var statuses = new List<JobStatus>();
        await foreach (var child in harness.Client.WaitEachAsync(batch.Id, ct))
        {
            statuses.Add(child.Status);
        }

        // WaitEachAsync yields all children regardless of outcome; no AggregateException.
        Assert.Equal(2, statuses.Count);
        Assert.Contains(JobStatus.Succeeded, statuses);
        Assert.Contains(JobStatus.Failed, statuses);
    }

    [Fact]
    public async Task WaitAsyncT_OnFailedRun_ThrowsJobRunExceptionWithFailedStatus()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("AlwaysFail", () => ThrowBoom());
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("AlwaysFail", cancellationToken: ct);

        var ex = await Assert.ThrowsAsync<JobRunException>(() => harness.Client.WaitAsync<int>(run.Id, ct));
        Assert.Equal(JobStatus.Failed, ex.Status);
    }

    [Fact]
    public async Task WaitAsyncT_OnCanceledRun_ThrowsJobRunExceptionWithCanceledStatus()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        var entered = new TaskCompletionSource();
        harness.Host.AddJob("Hang", async (CancellationToken innerCt) =>
        {
            entered.TrySetResult();
            await Task.Delay(TimeSpan.FromMinutes(5), innerCt);
            return 1;
        });
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Hang", cancellationToken: ct);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);
        await harness.Client.CancelAsync(run.Id, ct);

        var ex = await Assert.ThrowsAsync<JobRunException>(() => harness.Client.WaitAsync<int>(run.Id, ct));
        Assert.Equal(JobStatus.Canceled, ex.Status);
    }

    [Fact]
    public async Task WaitBatchAsyncT_MixedOutcomes_ThrowsAggregateException()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("MaybeFail", (int code) => code == 0 ? 42 : ThrowBoom());
        await harness.StartAsync(ct);

        var batch = await harness.Client.TriggerBatchAsync("MaybeFail",
            [new { code = 0 }, new { code = 1 }], cancellationToken: ct);

        var ex = await Assert.ThrowsAsync<AggregateException>(() => harness.Client.WaitBatchAsync<int>(batch.Id, ct));
        Assert.Single(ex.InnerExceptions);
        var inner = Assert.IsType<JobRunException>(ex.InnerExceptions[0]);
        Assert.Equal(JobStatus.Failed, inner.Status);
    }

    [Fact]
    public async Task WaitAsyncT_OnVoidJob_ThrowsInvalidOperationException()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("VoidJob", () => Task.CompletedTask);
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("VoidJob", cancellationToken: ct);

        await Assert.ThrowsAsync<InvalidOperationException>(() => harness.Client.WaitAsync<int>(run.Id, ct));
    }

    [Fact]
    public async Task ScalarJob_WaitAsyncT_Scalar_ReturnsValue()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("Scalar", () => 42);
        await harness.StartAsync(ct);

        var result = await harness.Client.RunAsync<int>("Scalar", cancellationToken: ct);
        Assert.Equal(42, result);
    }

    [Fact]
    public async Task CollectionJob_WaitAsyncT_List_ReturnsCollection()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("Collection", () => new List<int> { 1, 2, 3 });
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Collection", cancellationToken: ct);
        var list = await harness.Client.WaitAsync<List<int>>(run.Id, ct);
        Assert.Equal([1, 2, 3], list);
    }

    [Fact]
    public async Task CollectionJob_WaitAsyncT_Array_ReturnsArray()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("Collection", () => new List<int> { 1, 2, 3 });
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Collection", cancellationToken: ct);
        var arr = await harness.Client.WaitAsync<int[]>(run.Id, ct);
        Assert.Equal([1, 2, 3], arr);
    }

    [Fact]
    public async Task StreamJob_WaitAsyncT_List_MaterializesFromOutputEvents()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("Stream", Stream);
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Stream", cancellationToken: ct);
        var list = await harness.Client.WaitAsync<List<int>>(run.Id, ct);
        Assert.Equal([10, 20, 30], list);

        static async IAsyncEnumerable<int> Stream([EnumeratorCancellation] CancellationToken inner)
        {
            yield return 10;
            await Task.Delay(5, inner);
            yield return 20;
            yield return 30;
        }
    }

    [Fact]
    public async Task StreamJob_StreamAsyncT_YieldsItemsLive()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("Stream", Stream);
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Stream", cancellationToken: ct);
        var items = new List<int>();
        await foreach (var item in harness.Client.WaitStreamAsync<int>(run.Id, ct))
        {
            items.Add(item);
        }

        Assert.Equal([1, 2, 3], items);

        static async IAsyncEnumerable<int> Stream([EnumeratorCancellation] CancellationToken inner)
        {
            yield return 1;
            await Task.Delay(5, inner);
            yield return 2;
            yield return 3;
        }
    }

    [Fact]
    public async Task CollectionJob_StreamAsyncT_YieldsCollectionAsStream()
    {
        // Collection-returning job consumed as a stream; resolver iterates the collection.
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("Collection", () => new List<int> { 1, 2, 3 });
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Collection", cancellationToken: ct);
        var items = new List<int>();
        await foreach (var item in harness.Client.WaitStreamAsync<int>(run.Id, ct))
        {
            items.Add(item);
        }

        Assert.Equal([1, 2, 3], items);
    }

    [Fact]
    public async Task ScalarJob_StreamAsyncT_YieldsScalarAsSingleItem()
    {
        // Scalar-returning job consumed as a stream; resolver yields once.
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("Scalar", () => 42);
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Scalar", cancellationToken: ct);
        var items = new List<int>();
        await foreach (var item in harness.Client.WaitStreamAsync<int>(run.Id, ct))
        {
            items.Add(item);
        }

        Assert.Equal([42], items);
    }

    [Fact]
    public async Task WaitBatchAsyncT_AllSucceeded_ReturnsResultsInOrder()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("Double", (int x) => x * 2);
        await harness.StartAsync(ct);

        var values = await harness.Client.RunBatchAsync<int>("Double",
            [new { x = 1 }, new { x = 2 }, new { x = 3 }], cancellationToken: ct);

        Assert.Equal(3, values.Count);
        Assert.Contains(2, values);
        Assert.Contains(4, values);
        Assert.Contains(6, values);
    }

    [Fact]
    public async Task WaitEachAsync_IAsyncEnumerableT_YieldsLiveInnerStreamsPerChild()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("Stream", Stream);
        await harness.StartAsync(ct);

        var batch = await harness.Client.TriggerBatchAsync("Stream",
            [new { start = 100 }, new { start = 200 }], cancellationToken: ct);

        var yieldedInnerStreams = 0;
        var allItems = new List<int>();
        await foreach (var inner in harness.Client.WaitEachAsync<IAsyncEnumerable<int>>(batch.Id, ct))
        {
            yieldedInnerStreams++;
            await foreach (var item in inner.WithCancellation(ct))
            {
                allItems.Add(item);
            }
        }

        Assert.Equal(2, yieldedInnerStreams);
        // Each child yields [start, start+1, start+2].
        Assert.Contains(100, allItems);
        Assert.Contains(101, allItems);
        Assert.Contains(102, allItems);
        Assert.Contains(200, allItems);
        Assert.Contains(201, allItems);
        Assert.Contains(202, allItems);
        Assert.Equal(6, allItems.Count);

        static async IAsyncEnumerable<int> Stream(int start, [EnumeratorCancellation] CancellationToken inner)
        {
            yield return start;
            await Task.Delay(5, inner);
            yield return start + 1;
            yield return start + 2;
        }
    }

    [Fact]
    public async Task WaitEachAsync_IAsyncEnumerableT_ChildFailure_SurfacesOnInnerStreamOnly()
    {
        // Per contract: a failed child faults that child's inner stream; other children's
        // inner streams and the outer enumerator are unaffected.
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("MaybeFailStream", MaybeFailStream);
        await harness.StartAsync(ct);

        var batch = await harness.Client.TriggerBatchAsync("MaybeFailStream",
            [new { code = 0 }, new { code = 1 }, new { code = 2 }], cancellationToken: ct);

        var perChildOutcomes = new List<(int Count, Exception? Error)>();
        await foreach (var inner in harness.Client.WaitEachAsync<IAsyncEnumerable<int>>(batch.Id, ct))
        {
            var items = new List<int>();
            Exception? caught = null;
            try
            {
                await foreach (var item in inner.WithCancellation(ct))
                {
                    items.Add(item);
                }
            }
            catch (Exception ex)
            {
                caught = ex;
            }

            perChildOutcomes.Add((items.Count, caught));
        }

        // Three children observed; one of them faulted on its inner stream.
        Assert.Equal(3, perChildOutcomes.Count);
        Assert.Single(perChildOutcomes, o => o.Error is JobRunException);
        Assert.Equal(2, perChildOutcomes.Count(o => o.Error is null));

        static async IAsyncEnumerable<int> MaybeFailStream(int code, [EnumeratorCancellation] CancellationToken inner)
        {
            yield return code * 10;
            await Task.Delay(5, inner);
            if (code == 1)
            {
                throw new InvalidOperationException("boom");
            }

            yield return code * 10 + 1;
        }
    }

    [Fact]
    public async Task WaitEachAsyncT_FailFast_ThrowsOnFirstFailure()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("MaybeFail", (int code) => code == 1 ? ThrowBoom() : code * 10);
        await harness.StartAsync(ct);

        var batch = await harness.Client.TriggerBatchAsync("MaybeFail",
            [new { code = 0 }, new { code = 1 }, new { code = 2 }], cancellationToken: ct);

        var observed = new List<int>();
        var ex = await Assert.ThrowsAsync<JobRunException>(async () =>
        {
            await foreach (var item in harness.Client.WaitEachAsync<int>(batch.Id, ct))
            {
                observed.Add(item);
            }
        });

        Assert.Equal(JobStatus.Failed, ex.Status);
        // At most observed the succeeded value before the failure bubble.
        Assert.All(observed, v => Assert.NotEqual(10, v));
    }

    [Fact]
    public async Task WaitAsync_Observation_Cancellation_DoesNotCancelRun()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        var entered = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        harness.Host.AddJob("Gate", async (CancellationToken inner) =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(inner);
            return 1;
        });
        await harness.StartAsync(ct);

        var run = await harness.Client.TriggerAsync("Gate", cancellationToken: ct);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);

        using var waitCts = new CancellationTokenSource();
        var waitTask = harness.Client.WaitAsync(run.Id, waitCts.Token);
        waitCts.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(() => waitTask);

        // Run must still be non-terminal: observation does not own the run.
        var snapshot = await harness.Client.GetRunAsync(run.Id, ct);
        Assert.NotNull(snapshot);
        Assert.False(snapshot.Status.IsTerminal);

        // Release the run and confirm it completes naturally.
        release.TrySetResult();
        var final = await harness.Client.WaitAsync(run.Id, ct);
        Assert.Equal(JobStatus.Succeeded, final.Status);
    }

    [Fact]
    public async Task RunAsyncT_OwnerCancellation_CancelsRun()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        var entered = new TaskCompletionSource();
        harness.Host.AddJob("Hang", async (CancellationToken inner) =>
        {
            entered.TrySetResult();
            await Task.Delay(TimeSpan.FromMinutes(5), inner);
            return 1;
        });
        await harness.StartAsync(ct);

        using var ownerCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var runTask = harness.Client.RunAsync<int>("Hang", cancellationToken: ownerCts.Token);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5), ct);

        ownerCts.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(() => runTask);

        // Cancellation of the owning token must cancel the run it triggered.
        var runsInStore = new List<JobRun>();
        await foreach (var r in harness.Client.GetRunsAsync(
                           new() { JobName = "Hang" }, ct))
        {
            runsInStore.Add(r);
        }

        Assert.Single(runsInStore);
        // Poll briefly for the cancellation to settle.
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(5);
        JobRun? final = null;
        while (DateTimeOffset.UtcNow < deadline)
        {
            final = await harness.Client.GetRunAsync(runsInStore[0].Id, ct);
            if (final is { Status: JobStatus.Canceled })
            {
                break;
            }

            await Task.Delay(25, ct);
        }

        Assert.NotNull(final);
        Assert.Equal(JobStatus.Canceled, final.Status);
    }

    [Fact]
    public async Task HandlerIAsyncEnumerable_CallerConcreteArray_MaterializesCorrectly()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await CreateHarnessAsync();
        harness.Host.AddJob("SumStream", async (IAsyncEnumerable<int> values, CancellationToken inner) =>
        {
            var sum = 0;
            await foreach (var v in values.WithCancellation(inner))
            {
                sum += v;
            }

            return sum;
        });
        await harness.StartAsync(ct);

        var result = await harness.Client.RunAsync<int>("SumStream",
            new { values = new[] { 1, 2, 3, 4 } }, cancellationToken: ct);

        Assert.Equal(10, result);
    }

    [Fact]
    public async Task StreamBatchAsync_LargeBatch_UsesBulkFetchNotPerChildRoundTrips()
    {
        var ct = TestContext.Current.CancellationToken;
        var counter = new FetchCounter();

        // Pre-register the counting store so AddSurefire's TryAddSingleton<IJobStore> skips its default.
        var wrapped = new CountingJobStore(new InMemoryJobStore(TimeProvider.System), counter);
        await using var harness = await CreateHarnessAsync(configureServices: services =>
        {
            services.AddSingleton<IJobStore>(wrapped);
        });
        harness.Host.AddJob("Identity", (int x) => x);
        await harness.StartAsync(ct);

        var args = Enumerable.Range(0, 200).Select(i => (object?)new { x = i }).ToArray();
        var results = await harness.Client.RunBatchAsync<int>("Identity", args, cancellationToken: ct);

        Assert.Equal(200, results.Count);

        // 200 children / 64-window = at most ~4 bulk fetches plus some tail flushes.
        // Assert the number of GetRunsByIdsAsync calls is much lower than 200 (not per-child).
        Assert.InRange(counter.GetRunsByIdsCalls, 1, 20);
    }

    private static Task<RuntimeHarness> CreateHarnessAsync(
        Action<SurefireOptions>? configureOptions = null,
        Action<IServiceCollection>? configureServices = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSurefire(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(15);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
            configureOptions?.Invoke(options);
        });
        configureServices?.Invoke(services);

        var provider = services.BuildServiceProvider();
        var host = new TestHost(provider);
        var client = provider.GetRequiredService<IJobClient>();
        var store = provider.GetRequiredService<IJobStore>();
        var notifications = provider.GetRequiredService<INotificationProvider>();
        var hostedServices = provider.GetServices<IHostedService>().ToArray();

        return Task.FromResult(new RuntimeHarness(provider, host, store, notifications, client, hostedServices));
    }

    private sealed class FetchCounter
    {
        public int GetRunsByIdsCalls;
    }

    /// <summary>
    ///     Wrapping store that counts <see cref="IJobStore.GetRunsByIdsAsync" /> calls,
    ///     used to verify batch hydration uses bulk fetch rather than per-child round trips.
    /// </summary>
    private sealed class CountingJobStore(IJobStore inner, FetchCounter counter) : IJobStore
    {
        public Task MigrateAsync(CancellationToken ct = default) => inner.MigrateAsync(ct);
        public Task PingAsync(CancellationToken ct = default) => inner.PingAsync(ct);
        public bool IsTransientException(Exception ex) => inner.IsTransientException(ex);

        public Task UpsertJobsAsync(IReadOnlyList<JobDefinition> jobs, CancellationToken ct = default) =>
            inner.UpsertJobsAsync(jobs, ct);

        public Task<JobDefinition?> GetJobAsync(string name, CancellationToken ct = default) =>
            inner.GetJobAsync(name, ct);

        public Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null,
            CancellationToken ct = default) => inner.GetJobsAsync(filter, ct);

        public Task SetJobEnabledAsync(string name, bool enabled, CancellationToken ct = default) =>
            inner.SetJobEnabledAsync(name, enabled, ct);

        public Task UpdateLastCronFireAtAsync(string jobName, DateTimeOffset fireAt, CancellationToken ct = default) =>
            inner.UpdateLastCronFireAtAsync(jobName, fireAt, ct);

        public Task CreateRunsAsync(IReadOnlyList<JobRun> runs, IReadOnlyList<RunEvent>? initialEvents = null,
            CancellationToken ct = default) => inner.CreateRunsAsync(runs, initialEvents, ct);

        public Task<bool> TryCreateRunAsync(JobRun run, int? maxActiveForJob = null,
            DateTimeOffset? lastCronFireAt = null, IReadOnlyList<RunEvent>? initialEvents = null,
            CancellationToken ct = default) =>
            inner.TryCreateRunAsync(run, maxActiveForJob, lastCronFireAt, initialEvents, ct);

        public Task<JobRun?> GetRunAsync(string id, CancellationToken ct = default) => inner.GetRunAsync(id, ct);

        public Task<IReadOnlyList<JobRun>> GetRunsByIdsAsync(IReadOnlyList<string> ids, CancellationToken ct = default)
        {
            Interlocked.Increment(ref counter.GetRunsByIdsCalls);
            return inner.GetRunsByIdsAsync(ids, ct);
        }

        public Task<DirectChildrenPage> GetDirectChildrenAsync(string parentRunId, string? afterCursor = null,
            string? beforeCursor = null, int take = 50, CancellationToken ct = default) =>
            inner.GetDirectChildrenAsync(parentRunId, afterCursor, beforeCursor, take, ct);

        public Task<IReadOnlyList<JobRun>> GetAncestorChainAsync(string runId, CancellationToken ct = default) =>
            inner.GetAncestorChainAsync(runId, ct);

        public Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, int skip = 0, int take = 50,
            CancellationToken ct = default) => inner.GetRunsAsync(filter, skip, take, ct);

        public Task UpdateRunAsync(JobRun run, CancellationToken ct = default) => inner.UpdateRunAsync(run, ct);

        public Task<RunTransitionResult> TryTransitionRunAsync(RunStatusTransition transition,
            CancellationToken ct = default) => inner.TryTransitionRunAsync(transition, ct);

        public Task<RunTransitionResult> TryCancelRunAsync(string runId, int? expectedAttempt = null,
            string? reason = null, IReadOnlyList<RunEvent>? events = null, CancellationToken ct = default) =>
            inner.TryCancelRunAsync(runId, expectedAttempt, reason, events, ct);

        public Task<SubtreeCancellation> CancelRunSubtreeAsync(string rootRunId, string? reason = null,
            bool includeRoot = true, CancellationToken ct = default) =>
            inner.CancelRunSubtreeAsync(rootRunId, reason, includeRoot, ct);

        public Task<IReadOnlyList<JobRun>> ClaimRunsAsync(string nodeName, IReadOnlyCollection<string> jobNames,
            IReadOnlyCollection<string> queueNames, int maxCount, CancellationToken ct = default) =>
            inner.ClaimRunsAsync(nodeName, jobNames, queueNames, maxCount, ct);

        public Task CreateBatchAsync(JobBatch batch, IReadOnlyList<JobRun> runs,
            IReadOnlyList<RunEvent>? initialEvents = null, CancellationToken ct = default) =>
            inner.CreateBatchAsync(batch, runs, initialEvents, ct);

        public Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken ct = default) =>
            inner.GetBatchAsync(batchId, ct);

        public Task<bool> TryCompleteBatchAsync(string batchId, JobStatus status, DateTimeOffset completedAt,
            CancellationToken ct = default) => inner.TryCompleteBatchAsync(batchId, status, completedAt, ct);

        public Task<SubtreeCancellation> CancelBatchSubtreeAsync(string batchId, string? reason = null,
            CancellationToken ct = default) => inner.CancelBatchSubtreeAsync(batchId, reason, ct);

        public Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken ct = default) =>
            inner.AppendEventsAsync(events, ct);

        public Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0,
            RunEventType[]? types = null, int? attempt = null, int? take = null, CancellationToken ct = default) =>
            inner.GetEventsAsync(runId, sinceId, types, attempt, take, ct);

        public Task<IReadOnlyList<RunEvent>> GetBatchEventsAsync(string batchId, long sinceEventId = 0, int take = 200,
            CancellationToken ct = default) => inner.GetBatchEventsAsync(batchId, sinceEventId, take, ct);

        public Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchId, long sinceEventId = 0,
            int take = 200, CancellationToken ct = default) =>
            inner.GetBatchOutputEventsAsync(batchId, sinceEventId, take, ct);

        public Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> jobNames,
            IReadOnlyCollection<string> queueNames, IReadOnlyCollection<string> activeRunIds,
            CancellationToken ct = default) => inner.HeartbeatAsync(nodeName, jobNames, queueNames, activeRunIds, ct);

        public Task<IReadOnlyList<string>> GetExternallyStoppedRunIdsAsync(IReadOnlyCollection<string> runIds,
            CancellationToken ct = default) => inner.GetExternallyStoppedRunIdsAsync(runIds, ct);

        public Task<IReadOnlyList<string>> GetStaleRunningRunIdsAsync(DateTimeOffset staleBefore, int take,
            CancellationToken ct = default) => inner.GetStaleRunningRunIdsAsync(staleBefore, take, ct);

        public Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken ct = default) => inner.GetNodesAsync(ct);

        public Task<NodeInfo?> GetNodeAsync(string name, CancellationToken ct = default) =>
            inner.GetNodeAsync(name, ct);

        public Task UpsertQueuesAsync(IReadOnlyList<QueueDefinition> queues, CancellationToken ct = default) =>
            inner.UpsertQueuesAsync(queues, ct);

        public Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken ct = default) =>
            inner.GetQueuesAsync(ct);

        public Task<bool> SetQueuePausedAsync(string name, bool isPaused, CancellationToken ct = default) =>
            inner.SetQueuePausedAsync(name, isPaused, ct);

        public Task UpsertRateLimitsAsync(IReadOnlyList<RateLimitDefinition> rateLimits,
            CancellationToken ct = default) => inner.UpsertRateLimitsAsync(rateLimits, ct);

        public Task<SubtreeCancellation> CancelExpiredRunsWithIdsAsync(CancellationToken ct = default) =>
            inner.CancelExpiredRunsWithIdsAsync(ct);

        public Task PurgeAsync(DateTimeOffset threshold, CancellationToken ct = default) =>
            inner.PurgeAsync(threshold, ct);

        public Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60,
            CancellationToken ct = default) => inner.GetDashboardStatsAsync(since, bucketMinutes, ct);

        public Task<JobStats> GetJobStatsAsync(string jobName, CancellationToken ct = default) =>
            inner.GetJobStatsAsync(jobName, ct);

        public Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(CancellationToken ct = default) =>
            inner.GetQueueStatsAsync(ct);

        public Task<IReadOnlyList<string>> GetCompletableBatchIdsAsync(CancellationToken ct = default) =>
            inner.GetCompletableBatchIdsAsync(ct);
    }
}
