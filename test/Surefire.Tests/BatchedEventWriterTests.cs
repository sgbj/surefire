using Microsoft.Extensions.Logging.Abstractions;
using Surefire.Tests.Fakes;

namespace Surefire.Tests;

/// <summary>
///     Direct coverage of <see cref="BatchedEventWriter" /> behavior that the durability invariant
///     of <c>FlushRunAsync</c> rests on: events flush in order, FlushRunAsync only returns
///     successfully when the underlying store accepts the writes for that run, transient errors
///     retry transparently, permanent errors propagate to FlushRunAsync waiters only for the runs
///     whose events were in the failing batch, sibling runs stay unaffected, and shutdown drains
///     everything still queued.
/// </summary>
public sealed class BatchedEventWriterTests
{
    private const string RunA = "run-a";
    private const string RunB = "run-b";

    [Fact]
    public async Task FlushRunAsync_AfterEnqueue_ReturnsOnceEventsArePersisted()
    {
        var store = new RecordingStore();
        await using var harness = await StartAsync(store);

        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 1), [], TestContext.Current.CancellationToken);
        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 2), [], TestContext.Current.CancellationToken);
        await harness.Writer.FlushRunAsync(RunA, TestContext.Current.CancellationToken);

        Assert.Equal(2, store.AppendedCount);
    }

    [Fact]
    public async Task FlushRunAsync_PermanentStoreError_ThrowsToCallerAtOrBelowSequence()
    {
        // Permanent errors must propagate so terminal-transition flushes don't claim durability
        // they don't have. A FlushRunAsync waiter whose run had events in the failing batch must
        // see the original exception, not a silent success.
        var failure = new InvalidOperationException("schema drift");
        var store = new RecordingStore { FailWith = failure };
        await using var harness = await StartAsync(store);

        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 1), [], TestContext.Current.CancellationToken);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            harness.Writer.FlushRunAsync(RunA, TestContext.Current.CancellationToken));
        Assert.Same(failure, thrown);
    }

    [Fact]
    public async Task FlushRunAsync_TransientStoreErrors_RetriesUntilSuccess()
    {
        // Transient errors (deadlock, connection reset) must retry indefinitely with backoff —
        // FlushRunAsync should eventually return successfully and the events should be persisted.
        var store = new RecordingStore { FailNextTransientCount = 2 };
        await using var harness = await StartAsync(store);

        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 1), [], TestContext.Current.CancellationToken);
        await harness.Writer.FlushRunAsync(RunA, TestContext.Current.CancellationToken);

        Assert.Equal(1, store.AppendedCount);
        Assert.Equal(2, store.TransientFailuresApplied);
    }

    [Fact]
    public async Task StopAsync_DrainsQueuedEvents_WhenStoreIsHealthy()
    {
        // Graceful stop: the worker exits when the channel is completed AND drained, so every
        // enqueued event lands in the store before StopAsync returns.
        var store = new RecordingStore();
        var harness = await StartAsync(store);

        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 1), [], TestContext.Current.CancellationToken);
        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 2), [], TestContext.Current.CancellationToken);
        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 3), [], TestContext.Current.CancellationToken);

        await harness.DisposeAsync();

        Assert.Equal(3, store.AppendedCount);
    }

    [Fact]
    public async Task FlushRunAsync_WithNoEnqueues_ReturnsImmediately()
    {
        var store = new RecordingStore();
        await using var harness = await StartAsync(store);

        await harness.Writer.FlushRunAsync(RunA, TestContext.Current.CancellationToken);
        Assert.Equal(0, store.AppendedCount);
    }

    [Fact]
    public async Task FlushRunAsync_EarlierBatchFailedPermanently_LaterBatchSucceeded_ThrowsOriginalException()
    {
        // Durability regression guard: the sticky-within-run Error stays set across later batches
        // for the same run. A caller flushing after a later success still observes the earlier
        // failure so the terminal transition can't silently proceed with a hole in the event
        // stream.
        var failure = new InvalidOperationException("permanent");
        var store = new SequencedStore { FailOnFirst = failure };
        await using var harness = await StartAsync(store);
        var ct = TestContext.Current.CancellationToken;

        // First batch: enqueue + flush to force first-batch permanent failure.
        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 1), [], ct);
        await Assert.ThrowsAsync<InvalidOperationException>(() => harness.Writer.FlushRunAsync(RunA, ct));

        // Second batch succeeds, advancing FlushedSeq past the first failed batch's range.
        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 2), [], ct);

        // FlushRunAsync MUST still throw — the caller's target reaches past the first-batch
        // failure and that hole is permanent for this run.
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => harness.Writer.FlushRunAsync(RunA, ct));
        Assert.Same(failure, thrown);
    }

    [Fact]
    public async Task FlushRunAsync_SuccessfulFlush_DoesNotSpuriouslyThrow_WhenLaterBatchFails()
    {
        // Counterpart to the guard above: a durable signal given for events that flushed
        // BEFORE any failure must not be revoked retroactively. We flush once while the
        // store is still healthy, then make the store fail. The successful flush observed
        // no error. The subsequent flush (whose target reaches the failure) throws.
        var failure = new InvalidOperationException("permanent");
        var store = new SequencedStore { FailOnSecond = failure };
        await using var harness = await StartAsync(store);
        var ct = TestContext.Current.CancellationToken;

        // First batch succeeds (target=1, no error captured).
        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 1), [], ct);
        await harness.Writer.FlushRunAsync(RunA, ct);

        // Second batch fails permanently; target (2) reaches the failure.
        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 2), [], ct);
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => harness.Writer.FlushRunAsync(RunA, ct));
        Assert.Same(failure, thrown);
    }

    [Fact]
    public async Task FlushRunAsync_PermanentFailureForOneRun_DoesNotPoisonSiblingRuns()
    {
        // Cross-run isolation. One run's permanent event failure must not block a sibling run's
        // FlushRunAsync — the writer is a node-wide singleton, so a poisoned global flush cursor
        // would wedge every other run on the node until process restart.
        //
        // Batches are atomic at the store (all-or-nothing append), so to test cross-run isolation
        // we need the failing batch to contain ONLY RunA's events. We get that natural separation
        // by enqueuing + flushing RunA before any other run enqueues — the flush drains the
        // single-run batch and stamps RunA's error before RunB's event even enters the channel.
        var failure = new InvalidOperationException("runA-only failure");
        var store = new PerRunFailureStore { FailRunIds = { RunA }, Failure = failure };
        await using var harness = await StartAsync(store);
        var ct = TestContext.Current.CancellationToken;

        // RunA enqueues + flushes. Its single-run batch fails permanently; RunA's state is
        // stamped with the error.
        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 1), [], ct);
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            harness.Writer.FlushRunAsync(RunA, ct));
        Assert.Same(failure, thrown);

        // RunB enqueues AFTER RunA's failing batch has already been processed. Its events land
        // in a fresh batch that doesn't touch RunA. Prior to the per-run state fix this flush
        // would throw (global sticky error); with per-run state it completes cleanly.
        await harness.Writer.EnqueueAsync(MakeEvent(RunB, 1), [], ct);
        await harness.Writer.FlushRunAsync(RunB, ct);

        // A later RunA flush still throws — sticky-within-run error survives sibling traffic.
        var thrownAgain = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            harness.Writer.FlushRunAsync(RunA, ct));
        Assert.Same(failure, thrownAgain);

        // A third unrelated run also flushes cleanly, confirming no lingering node-wide poison.
        const string RunC = "run-c";
        await harness.Writer.EnqueueAsync(MakeEvent(RunC, 1), [], ct);
        await harness.Writer.FlushRunAsync(RunC, ct);

        Assert.Contains(RunB, store.AppendedRunIds);
        Assert.Contains(RunC, store.AppendedRunIds);
        Assert.DoesNotContain(RunA, store.AppendedRunIds);
    }

    [Fact]
    public async Task DropRunState_AfterSuccessfulFlush_AllowsStateToBeReclaimed()
    {
        // After a terminal transition, the caller must be able to drop the run's flush state so
        // the dictionary doesn't grow unbounded over a long-lived process. FlushRunAsync on a
        // dropped run returns immediately (nothing to wait on) instead of throwing.
        var store = new RecordingStore();
        await using var harness = await StartAsync(store);
        var ct = TestContext.Current.CancellationToken;

        await harness.Writer.EnqueueAsync(MakeEvent(RunA, 1), [], ct);
        await harness.Writer.FlushRunAsync(RunA, ct);

        harness.Writer.DropRunState(RunA);

        // Flushing a dropped run is a no-op (state was removed on terminal).
        await harness.Writer.FlushRunAsync(RunA, ct);
    }

    private static RunEvent MakeEvent(string runId, int payload) => new()
    {
        RunId = runId,
        EventType = RunEventType.Output,
        Payload = payload.ToString(),
        CreatedAt = DateTimeOffset.UtcNow,
        Attempt = 1
    };

    private static async Task<Harness> StartAsync(IJobStore store)
    {
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        // Deterministic backoff for tests: nextDouble returns 0 so jitter computes to 50% of base.
        var writer = new BatchedEventWriter(store, notifications, new(),
            TimeProvider.System, new(() => 0.0), NullLogger<BatchedEventWriter>.Instance);
        await writer.StartAsync(CancellationToken.None);
        return new(writer);
    }

    private sealed class Harness(BatchedEventWriter writer) : IAsyncDisposable
    {
        public BatchedEventWriter Writer { get; } = writer;
        public async ValueTask DisposeAsync() => await Writer.StopAsync(CancellationToken.None);
    }

    private sealed class RecordingStore : ThrowingJobStore
    {
        private int _appended;
        private int _transientApplied;
        public Exception? FailWith { get; init; }
        public int FailNextTransientCount { get; set; }

        public int AppendedCount => Volatile.Read(ref _appended);
        public int TransientFailuresApplied => Volatile.Read(ref _transientApplied);

        public override Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken ct = default)
        {
            if (FailWith is { } perm)
            {
                throw perm;
            }

            if (FailNextTransientCount > 0)
            {
                FailNextTransientCount--;
                Interlocked.Increment(ref _transientApplied);
                throw new TransientTestException();
            }

            Interlocked.Add(ref _appended, events.Count);
            return Task.CompletedTask;
        }

        public override bool IsTransientException(Exception ex) => ex is TransientTestException;
    }

    private sealed class TransientTestException : Exception;

    private sealed class SequencedStore : ThrowingJobStore
    {
        private int _appended;
        private int _callCount;
        public Exception? FailOnFirst { get; init; }
        public Exception? FailOnSecond { get; init; }
        public int AppendedCount => Volatile.Read(ref _appended);

        public override Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken ct = default)
        {
            var call = Interlocked.Increment(ref _callCount);
            if (call == 1 && FailOnFirst is { } first)
            {
                throw first;
            }

            if (call == 2 && FailOnSecond is { } second)
            {
                throw second;
            }

            Interlocked.Add(ref _appended, events.Count);
            return Task.CompletedTask;
        }

        public override bool IsTransientException(Exception ex) => false;
    }

    /// <summary>
    ///     Fails any batch that contains an event whose RunId is in <see cref="FailRunIds" />.
    ///     Used to simulate a per-run permanent failure (e.g. a payload-shape mismatch, a FK
    ///     violation scoped to one run's trace) while sibling runs' events persist normally.
    /// </summary>
    private sealed class PerRunFailureStore : ThrowingJobStore
    {
        private readonly List<string> _appendedRunIds = [];
        private readonly Lock _lock = new();
        public HashSet<string> FailRunIds { get; } = new(StringComparer.Ordinal);
        public required Exception Failure { get; init; }

        public IReadOnlyList<string> AppendedRunIds
        {
            get
            {
                lock (_lock)
                {
                    return [.. _appendedRunIds];
                }
            }
        }

        public override Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken ct = default)
        {
            foreach (var evt in events)
            {
                if (FailRunIds.Contains(evt.RunId))
                {
                    throw Failure;
                }
            }

            lock (_lock)
            {
                foreach (var evt in events)
                {
                    _appendedRunIds.Add(evt.RunId);
                }
            }

            return Task.CompletedTask;
        }

        public override bool IsTransientException(Exception ex) => false;
    }
}