using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Surefire.Tests.Fakes;
using Surefire.Tests.Testing;

namespace Surefire.Tests;

/// <summary>
///     Direct coverage of the leading + trailing throttle in <see cref="JobContext.ReportProgressAsync" />
///     and the synchronization with <c>FlushPendingProgressAsync</c> that the executor invokes before
///     transitioning a run to a terminal status. Runs against a fake store that captures every
///     UpdateRunAsync call so assertions can verify exactly which progress values were persisted
///     and in what order.
/// </summary>
public sealed class JobContextProgressTests
{
    private static readonly DateTimeOffset Epoch = new(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan ThrottleInterval = TimeSpan.FromMilliseconds(100);

    [Fact]
    public async Task FirstReport_FlushesImmediatelyAsLeadingEdge()
    {
        await using var harness = await Harness.CreateAsync();

        await harness.Context.ReportProgressAsync(0.25);
        await harness.WaitForUpdatesAsync(1);

        Assert.Equal([0.25], harness.Store.PersistedProgress);
    }

    [Fact]
    public async Task ReportsWithinWindow_CoalesceIntoSingleTrailingFlush()
    {
        await using var harness = await Harness.CreateAsync();

        await harness.Context.ReportProgressAsync(0.10); // leading edge
        await harness.WaitForUpdatesAsync(1);
        Assert.Equal([0.10], harness.Store.PersistedProgress);

        // Three quick reports within the window. Only the LAST should be flushed by the trailing edge.
        await harness.Context.ReportProgressAsync(0.20);
        await harness.Context.ReportProgressAsync(0.30);
        await harness.Context.ReportProgressAsync(0.40);

        // No additional persists yet — the trailing-edge timer hasn't fired.
        Assert.Equal([0.10], harness.Store.PersistedProgress);

        // Advance past the throttle interval; the timer fires and flushes the latest pending value.
        harness.Time.Advance(ThrottleInterval);
        await harness.WaitForUpdatesAsync(2);

        Assert.Equal([0.10, 0.40], harness.Store.PersistedProgress);
    }

    [Fact]
    public async Task FlushPendingProgress_AtTerminalTransition_PersistsLatestPendingValue()
    {
        await using var harness = await Harness.CreateAsync();

        await harness.Context.ReportProgressAsync(0.10);
        await harness.WaitForUpdatesAsync(1);

        await harness.Context.ReportProgressAsync(0.50);
        await harness.Context.ReportProgressAsync(0.75);

        // Handler is "returning" before the timer fires — executor calls FlushPendingProgressAsync
        // synchronously to drain the trailing-edge value.
        await harness.Context.FlushPendingProgressAsync(CancellationToken.None);

        Assert.Equal([0.10, 0.75], harness.Store.PersistedProgress);
    }

    [Fact]
    public async Task FlushPendingProgress_AwaitsInFlightTimerCallback()
    {
        // Race: the timer's fire-and-forget PersistProgressAsync is in flight when the terminal
        // transition calls FlushPendingProgressAsync. The flush must observe the trailing flush as
        // in-flight and await it — otherwise the late progress event lands AFTER the terminal
        // status, violating the durability invariant. This test gates the persist on a manual
        // signal and asserts FlushPendingProgressAsync waits for it.
        await using var harness = await Harness.CreateAsync();

        await harness.Context.ReportProgressAsync(0.10);
        await harness.WaitForUpdatesAsync(1);

        await harness.Context.ReportProgressAsync(0.60);

        // Block the next UpdateRunAsync so the trailing flush hangs after entering the persist path.
        var release = harness.Store.GateNextUpdate();

        // Fire the trailing-edge timer.
        harness.Time.Advance(ThrottleInterval);

        // FlushPendingProgressAsync should still see the in-flight task and await it.
        var flushTask = harness.Context.FlushPendingProgressAsync(CancellationToken.None);
        Assert.False(flushTask.IsCompleted, "FlushPendingProgressAsync must wait for the in-flight trailing flush.");

        release.SetResult();

        await flushTask;

        Assert.Equal([0.10, 0.60], harness.Store.PersistedProgress);
    }

    [Fact]
    public async Task ReportAtFullValue_StillCoalescesWithinWindow()
    {
        // The throttle has no special case for 1.0 — terminal value safety is guaranteed by the
        // executor calling FlushPendingProgressAsync at the terminal transition. This test pins
        // that 1.0 inside a throttle window does NOT bypass the throttle (no double-write).
        await using var harness = await Harness.CreateAsync();

        await harness.Context.ReportProgressAsync(0.50);
        await harness.WaitForUpdatesAsync(1);

        await harness.Context.ReportProgressAsync(1.0);

        // 1.0 is within the throttle window → coalesced as trailing pending value, not flushed yet.
        Assert.Equal([0.50], harness.Store.PersistedProgress);

        harness.Time.Advance(ThrottleInterval);
        await harness.WaitForUpdatesAsync(2);

        Assert.Equal([0.50, 1.0], harness.Store.PersistedProgress);
    }

    [Fact]
    public async Task ReportProgressAsync_RejectsOutOfRange()
    {
        await using var harness = await Harness.CreateAsync();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => harness.Context.ReportProgressAsync(-0.01));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => harness.Context.ReportProgressAsync(1.01));
    }

    private sealed class Harness : IAsyncDisposable
    {
        public required JobContext Context { get; init; }
        public required FakeTimeProvider Time { get; init; }
        public required CapturingProgressStore Store { get; init; }
        public required TestEventWriter EventWriter { get; init; }
        public required InMemoryNotificationProvider Notifications { get; init; }

        public async ValueTask DisposeAsync()
        {
            await EventWriter.DisposeAsync();
        }

        public static async Task<Harness> CreateAsync()
        {
            var time = new FakeTimeProvider(Epoch);
            var store = new CapturingProgressStore();
            var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
            var eventWriter = await TestEventWriter.StartAsync(store, notifications);
            var context = new JobContext
            {
                RunId = "run-1",
                RootRunId = "run-1",
                JobName = "TestJob",
                Attempt = 1,
                CancellationToken = CancellationToken.None,
                Store = store,
                Notifications = notifications,
                EventWriter = eventWriter,
                TimeProvider = time,
                NodeName = "node-1"
            };
            return new()
            {
                Context = context,
                Time = time,
                Store = store,
                EventWriter = eventWriter,
                Notifications = notifications
            };
        }

        public async Task WaitForUpdatesAsync(int expected)
        {
            // Persists are async (UpdateRunAsync + EventWriter.EnqueueAsync). Spin briefly for the
            // store-side update to land. The fake store is in-process so this is fast in practice.
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
            while (Store.PersistedProgress.Count < expected && DateTime.UtcNow < deadline)
            {
                await Task.Delay(5);
            }

            Assert.True(Store.PersistedProgress.Count >= expected,
                $"Expected at least {expected} progress persists; observed {Store.PersistedProgress.Count}.");
        }
    }

    private sealed class CapturingProgressStore : ThrowingJobStore
    {
        private readonly Lock _gate = new();
        private readonly List<double> _persisted = [];
        private TaskCompletionSource? _pendingGate;

        public IReadOnlyList<double> PersistedProgress
        {
            get
            {
                lock (_gate)
                {
                    return [.. _persisted];
                }
            }
        }

        public TaskCompletionSource GateNextUpdate()
        {
            lock (_gate)
            {
                _pendingGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
                return _pendingGate;
            }
        }

        public override async Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
        {
            TaskCompletionSource? gate;
            lock (_gate)
            {
                gate = _pendingGate;
                _pendingGate = null;
            }

            if (gate is { })
            {
                await gate.Task.WaitAsync(cancellationToken);
            }

            lock (_gate)
            {
                _persisted.Add(run.Progress);
            }
        }

        public override Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken ct = default) =>
            // Progress events flow through the event writer in tests but their payload is not
            // asserted here — we observe persistence ordering via UpdateRunAsync.
            Task.CompletedTask;
    }
}