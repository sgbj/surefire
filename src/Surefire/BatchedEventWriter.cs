using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Surefire;

/// <summary>
///     Coalesces fire-and-forget event writes into batched <see cref="IJobStore.AppendEventsAsync" />
///     calls and deduplicates accompanying notification publishes. Events inside explicit store
///     transactions (status transitions, run creation, cancellation) remain transactional and
///     bypass this path; only standalone appends (progress reports, stream output, log events,
///     batch completion side-events) flow through here.
///     <para>
///         Flush durability is tracked per-run, not globally. A permanent failure on one run's
///         events poisons only that run's <see cref="FlushRunAsync" />. Sibling runs sharing the
///         same writer keep flushing normally. The sticky-within-run semantics stay intact: once a
///         run's events have permanently failed, every subsequent <see cref="FlushRunAsync" /> for
///         that run throws until <see cref="DropRunState" /> is called on terminal.
///     </para>
/// </summary>
internal sealed partial class BatchedEventWriter(
    IJobStore store,
    INotificationProvider notifications,
    SurefireOptions options,
    TimeProvider timeProvider,
    Backoff backoff,
    ILogger<BatchedEventWriter> logger)
{
    private const int MaxBatchSize = 1000;

    // Bounded so memory caps at a few MB. Wait mode applies real backpressure to producers
    // instead of silently growing unbounded when the store is slow or paused.
    private const int ChannelCapacity = MaxBatchSize * 16;

    // 5 ms caps waiter latency while amortizing one store round trip across ~50-500 events.
    private static readonly TimeSpan FlushInterval = TimeSpan.FromMilliseconds(5);

    private static readonly TimeSpan RetryInitial = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan RetryMax = TimeSpan.FromSeconds(30);

    private readonly Channel<EventRequest> _channel = Channel.CreateBounded<EventRequest>(
        new BoundedChannelOptions(ChannelCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

    // Keyed by run id so a permanent failure on one run's events can't block sibling runs.
    // Created lazily on first enqueue, dropped on terminal via DropRunState.
    private readonly ConcurrentDictionary<string, PerRunFlushState> _runStates = new(StringComparer.Ordinal);

    // Global monotonic sequence so per-run watermarks share one number space.
    private long _enqueueSeq;

    private CancellationTokenSource? _runCts;
    private Task? _runLoop;

    /// <summary>
    ///     Starts the background flush loop. Call once before any <see cref="EnqueueAsync" /> call.
    /// </summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _runCts = new();
        _runLoop = Task.Run(() => ExecuteAsync(_runCts.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Completes the write channel and drains any remaining events before returning. Callers
    ///     that have enqueued events can await <see cref="FlushRunAsync" /> before stop to observe
    ///     per-batch outcomes. The host's <paramref name="cancellationToken" /> is forwarded to the
    ///     worker so a host shutdown deadline breaks any indefinite transient-retry loop and lets
    ///     the host actually stop. Events still pending at that point surface their cancellation
    ///     to <see cref="FlushRunAsync" /> waiters via the per-run error capture.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Forward host cancellation onto the worker's CTS so an infinite transient-retry inside
        // FlushBatchAsync can't prevent shutdown from completing.
        await using var registration = cancellationToken.UnsafeRegister(static state =>
        {
            try
            {
                ((CancellationTokenSource?)state)?.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }, _runCts);

        _channel.Writer.TryComplete();
        if (_runLoop is { } loop)
        {
            try
            {
                await loop;
            }
            catch (OperationCanceledException)
            {
            }
        }

        _runCts?.Dispose();
        _runCts = null;
        _runLoop = null;
    }

    /// <summary>
    ///     Schedules an event for batched persistence. Returns as soon as the event is accepted by
    ///     the bounded channel; the background worker coalesces it with sibling events into a
    ///     batched <see cref="IJobStore.AppendEventsAsync" /> call and deduplicates notifications.
    ///     Callers that need the event persisted before proceeding must await
    ///     <see cref="FlushRunAsync" /> afterward. Applies backpressure if the channel is full.
    /// </summary>
    public ValueTask EnqueueAsync(RunEvent evt, IReadOnlyList<NotificationPublish> channels,
        CancellationToken cancellationToken = default)
    {
        var sequence = Interlocked.Increment(ref _enqueueSeq);
        var state = _runStates.GetOrAdd(evt.RunId, static _ => new());
        lock (state.Lock)
        {
            if (sequence > state.EnqueuedSeq)
            {
                state.EnqueuedSeq = sequence;
            }
        }

        return _channel.Writer.WriteAsync(new(evt, channels, sequence), cancellationToken);
    }

    /// <summary>
    ///     Waits until every event enqueued for <paramref name="runId" /> before this call has been
    ///     persisted. Callers that need ordering guarantees (e.g. before transitioning the run to a
    ///     terminal status) should await this so readers never observe the terminal state missing
    ///     prior events. Throws the original exception if any of the run's own events permanently
    ///     failed. Sibling runs' failures do not propagate here.
    /// </summary>
    public async Task FlushRunAsync(string runId, CancellationToken cancellationToken = default)
    {
        if (!_runStates.TryGetValue(runId, out var state))
        {
            return;
        }

        long target;
        lock (state.Lock)
        {
            target = state.EnqueuedSeq;
        }

        while (true)
        {
            TaskCompletionSource tcs;
            lock (state.Lock)
            {
                // Sticky-within-run: a failure in the middle of a run's event stream means
                // downstream readers can never see a consistent prefix, so every subsequent flush
                // for this run observes the error until the state is dropped on terminal.
                if (state.Error is { } err)
                {
                    err.Throw();
                }

                if (state.FlushedSeq >= target)
                {
                    return;
                }

                tcs = state.Tcs;
            }

            await tcs.Task.WaitAsync(cancellationToken);
        }
    }

    /// <summary>
    ///     Drops the per-run flush bookkeeping. Must be called after a terminal transition, once
    ///     no further events can be enqueued for the run. Symmetric to
    ///     <see cref="SurefireLogEventPump.DropRunState" />.
    /// </summary>
    public void DropRunState(string runId) => _runStates.TryRemove(runId, out _);

    private async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var batch = new List<EventRequest>(MaxBatchSize);
        var reader = _channel.Reader;

        try
        {
            while (true)
            {
                try
                {
                    if (!await reader.WaitToReadAsync(stoppingToken))
                    {
                        return;
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }

                // Coalesce bursts into one flush via a short idle window.
                var deadline = Environment.TickCount64 + (long)FlushInterval.TotalMilliseconds;
                while (batch.Count < MaxBatchSize)
                {
                    if (reader.TryRead(out var item))
                    {
                        batch.Add(item);
                        continue;
                    }

                    var remaining = deadline - Environment.TickCount64;
                    if (remaining <= 0 || batch.Count == 0)
                    {
                        break;
                    }

                    using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                    timeoutCts.CancelAfter(TimeSpan.FromMilliseconds(remaining));
                    try
                    {
                        if (!await reader.WaitToReadAsync(timeoutCts.Token))
                        {
                            break;
                        }
                    }
                    catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }

                if (batch.Count > 0)
                {
                    await FlushBatchAsync(batch, stoppingToken);
                    batch.Clear();
                }
            }
        }
        finally
        {
            // Complete the writer (unblocks producers waiting on capacity) then drain queued
            // events under a fresh ShutdownTimeout-bounded CTS, independent of stoppingToken, so
            // accepted events aren't dropped just because the host shutdown deadline already fired.
            // A wedged store burns through the budget and FlushBatchAsync surfaces the timeout
            // per-run via the sticky error capture.
            _channel.Writer.TryComplete();
            using var drainCts = new CancellationTokenSource(options.ShutdownTimeout);
            while (reader.TryRead(out var item))
            {
                batch.Add(item);
                if (batch.Count >= MaxBatchSize)
                {
                    await FlushBatchAsync(batch, drainCts.Token);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await FlushBatchAsync(batch, drainCts.Token);
            }
        }
    }

    private async Task FlushBatchAsync(List<EventRequest> batch, CancellationToken cancellationToken)
    {
        if (batch.Count == 0)
        {
            return;
        }

        var events = new RunEvent[batch.Count];
        for (var i = 0; i < batch.Count; i++)
        {
            events[i] = batch[i].Event;
        }

        // Retry transient errors indefinitely; permanent errors propagate to FlushRunAsync callers
        // so terminal transitions never proceed with events unpersisted.
        var attempt = 0;
        while (true)
        {
            try
            {
                await store.AppendEventsAsync(events, cancellationToken);
                break;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                SignalBatchCompleted(batch,
                    ExceptionDispatchInfo.Capture(new OperationCanceledException(cancellationToken)));
                return;
            }
            catch (Exception ex) when (store.IsTransientException(ex))
            {
                Log.EventFlushRetrying(logger, ex, events.Length, attempt);
                try
                {
                    await Task.Delay(backoff.NextDelay(attempt++, RetryInitial, RetryMax), timeProvider,
                        cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    SignalBatchCompleted(batch,
                        ExceptionDispatchInfo.Capture(new OperationCanceledException(cancellationToken)));
                    return;
                }
            }
            catch (Exception ex)
            {
                Log.EventFlushFailed(logger, ex, events.Length);
                SignalBatchCompleted(batch, ExceptionDispatchInfo.Capture(ex));
                return;
            }
        }

        // Dedupe notifications by channel across the whole batch (a 500-event burst on the same
        // run produces one publish, not 500). Notifications are idempotent wake-ups, so individual
        // failures are logged but never fail the batch: the store write already succeeded.
        HashSet<string>? published = null;
        foreach (var request in batch)
        {
            foreach (var pub in request.Notifications)
            {
                published ??= new(StringComparer.Ordinal);
                if (!published.Add(pub.Channel))
                {
                    continue;
                }

                try
                {
                    await notifications.PublishAsync(pub.Channel, pub.Message, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    SignalBatchCompleted(batch, null);
                    return;
                }
                catch (Exception ex)
                {
                    Log.NotificationPublishFailed(logger, ex, pub.Channel);
                }
            }
        }

        SignalBatchCompleted(batch, null);
    }

    private void SignalBatchCompleted(List<EventRequest> batch, ExceptionDispatchInfo? error)
    {
        // Group by runId so we advance/stamp each run's state exactly once per call.
        var perRun = new Dictionary<string, (long MinSeq, long MaxSeq)>(StringComparer.Ordinal);
        foreach (var request in batch)
        {
            var runId = request.Event.RunId;
            if (perRun.TryGetValue(runId, out var range))
            {
                if (request.Sequence < range.MinSeq)
                {
                    range.MinSeq = request.Sequence;
                }

                if (request.Sequence > range.MaxSeq)
                {
                    range.MaxSeq = request.Sequence;
                }

                perRun[runId] = range;
            }
            else
            {
                perRun[runId] = (request.Sequence, request.Sequence);
            }
        }

        foreach (var (runId, range) in perRun)
        {
            // TryGetValue, not GetOrAdd: if state was dropped mid-flight the run already
            // terminated, so re-materializing would just leak with no waiter to observe it.
            if (!_runStates.TryGetValue(runId, out var state))
            {
                continue;
            }

            TaskCompletionSource completed;
            lock (state.Lock)
            {
                if (error is null)
                {
                    if (range.MaxSeq > state.FlushedSeq)
                    {
                        state.FlushedSeq = range.MaxSeq;
                    }
                }
                else if (state.Error is null)
                {
                    // First-failure wins; subsequent batch failures don't overwrite. ErrorMinSeq
                    // records the earliest lost sequence for diagnostics.
                    state.Error = error;
                    state.ErrorMinSeq = range.MinSeq;
                }

                completed = state.Tcs;
                state.Tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            completed.TrySetResult();
        }
    }

    internal readonly record struct NotificationPublish(string Channel, string? Message);

    private readonly record struct EventRequest(
        RunEvent Event,
        IReadOnlyList<NotificationPublish> Notifications,
        long Sequence);

    private sealed class PerRunFlushState
    {
        public readonly Lock Lock = new();
        public long EnqueuedSeq;
        public ExceptionDispatchInfo? Error;
        public long ErrorMinSeq;
        public long FlushedSeq;
        public TaskCompletionSource Tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1801, Level = LogLevel.Error,
            Message = "Failed to flush {Count} batched events to the store (permanent failure).")]
        public static partial void EventFlushFailed(ILogger logger, Exception exception, int count);

        [LoggerMessage(EventId = 1802, Level = LogLevel.Warning,
            Message = "Failed to publish batched notification on channel {Channel}.")]
        public static partial void NotificationPublishFailed(ILogger logger, Exception exception, string channel);

        [LoggerMessage(EventId = 1803, Level = LogLevel.Warning,
            Message = "Transient failure flushing {Count} batched events (attempt {Attempt}); retrying.")]
        public static partial void EventFlushRetrying(ILogger logger, Exception exception, int count, int attempt);
    }
}
