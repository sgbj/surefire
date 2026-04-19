using Microsoft.Extensions.Logging.Abstractions;
using Surefire.Tests.Fakes;

namespace Surefire.Tests;

/// <summary>
///     Direct coverage of <see cref="BatchedEventWriter" /> behavior that the durability invariant
///     of <c>FlushAsync</c> rests on: events flush in order, FlushAsync only returns successfully
///     when the underlying store accepts the writes, transient errors retry transparently,
///     permanent errors propagate to FlushAsync waiters, and shutdown drains everything still
///     queued.
/// </summary>
public sealed class BatchedEventWriterTests
{
    [Fact]
    public async Task FlushAsync_AfterEnqueue_ReturnsOnceEventsArePersisted()
    {
        var store = new RecordingStore();
        await using var harness = await StartAsync(store);

        await harness.Writer.EnqueueAsync(MakeEvent(1), [], TestContext.Current.CancellationToken);
        await harness.Writer.EnqueueAsync(MakeEvent(2), [], TestContext.Current.CancellationToken);
        await harness.Writer.FlushAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, store.AppendedCount);
    }

    [Fact]
    public async Task FlushAsync_PermanentStoreError_ThrowsToCallerAtOrBelowSequence()
    {
        // Permanent errors must propagate so terminal-transition flushes don't claim durability
        // they don't have. A FlushAsync waiter whose target sequence is at or below the failing
        // batch must see the original exception, not a silent success.
        var failure = new InvalidOperationException("schema drift");
        var store = new RecordingStore { FailWith = failure };
        await using var harness = await StartAsync(store);

        await harness.Writer.EnqueueAsync(MakeEvent(1), [], TestContext.Current.CancellationToken);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            harness.Writer.FlushAsync(TestContext.Current.CancellationToken));
        Assert.Same(failure, thrown);
    }

    [Fact]
    public async Task FlushAsync_TransientStoreErrors_RetriesUntilSuccess()
    {
        // Transient errors (deadlock, connection reset) must retry indefinitely with backoff —
        // FlushAsync should eventually return successfully and the events should be persisted.
        var store = new RecordingStore { FailNextTransientCount = 2 };
        await using var harness = await StartAsync(store);

        await harness.Writer.EnqueueAsync(MakeEvent(1), [], TestContext.Current.CancellationToken);
        await harness.Writer.FlushAsync(TestContext.Current.CancellationToken);

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

        await harness.Writer.EnqueueAsync(MakeEvent(1), [], TestContext.Current.CancellationToken);
        await harness.Writer.EnqueueAsync(MakeEvent(2), [], TestContext.Current.CancellationToken);
        await harness.Writer.EnqueueAsync(MakeEvent(3), [], TestContext.Current.CancellationToken);

        await harness.DisposeAsync();

        Assert.Equal(3, store.AppendedCount);
    }

    [Fact]
    public async Task FlushAsync_WithNoEnqueues_ReturnsImmediately()
    {
        var store = new RecordingStore();
        await using var harness = await StartAsync(store);

        await harness.Writer.FlushAsync(TestContext.Current.CancellationToken);
        Assert.Equal(0, store.AppendedCount);
    }

    private static RunEvent MakeEvent(int payload) => new()
    {
        RunId = "run-1",
        EventType = RunEventType.Output,
        Payload = payload.ToString(),
        CreatedAt = DateTimeOffset.UtcNow,
        Attempt = 1
    };

    private static async Task<Harness> StartAsync(IJobStore store)
    {
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        // Deterministic backoff for tests: nextDouble returns 0 so jitter computes to 50% of base.
        var writer = new BatchedEventWriter(store, notifications, TimeProvider.System, new(() => 0.0),
            NullLogger<BatchedEventWriter>.Instance);
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
}