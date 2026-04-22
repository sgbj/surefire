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
/// </summary>
internal sealed partial class BatchedEventWriter(
    IJobStore store,
    INotificationProvider notifications,
    TimeProvider timeProvider,
    Backoff backoff,
    ILogger<BatchedEventWriter> logger)
{
    private const int MaxBatchSize = 1000;

    // Bounded at 16× the flush batch size so memory is O(MaxBatchSize * 16 * sizeof(event)) — a
    // few MB ceiling. Wait mode applies real backpressure to producers instead of silently growing
    // unbounded when the store is slow or paused.
    private const int ChannelCapacity = MaxBatchSize * 16;

    // A tight stream-output loop can emit thousands of events per second on one run. Flushing
    // every 5 ms caps the end-to-end latency waiters see while still amortizing one store round
    // trip across ~50-500 events. The 1000-event cap prevents a huge backlog from becoming a
    // single giant INSERT that stalls the writer.
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

    // FlushAsync ordering: each Enqueue stamps a monotonic sequence. After a batch flushes
    // successfully the worker advances _flushedSeq to the batch's max sequence and signals any
    // waiters. On a permanent failure the exception is captured along with the failing batch's
    // MIN sequence so FlushAsync callers whose target has reached or passed that earliest known
    // failure observe the captured exception. Tracking the min (not the max) keeps the guard
    // correct even after later batches succeed: the min bounds `[1, min]` as "some earlier event
    // permanently failed," which is the condition a FlushAsync caller must see. Transient errors
    // never reach this field; the worker retries indefinitely with backoff until the store recovers.
    private readonly Lock _flushGate = new();
    private TaskCompletionSource _batchFlushedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long _enqueueSeq;
    private ExceptionDispatchInfo? _error;
    private long _errorMinSeq;
    private long _flushedSeq;

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
    ///     that have enqueued events can await <see cref="FlushAsync" /> before stop to observe
    ///     per-batch outcomes. The host's <paramref name="cancellationToken" /> is forwarded to the
    ///     worker so a host shutdown deadline breaks any indefinite transient-retry loop and lets
    ///     the host actually stop. Events still pending at that point surface their cancellation
    ///     to <see cref="FlushAsync" /> waiters via the per-sequence error capture.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Forward host cancellation onto the worker's CTS. Without this, an infinite
        // transient-error retry inside FlushBatchAsync would prevent shutdown from completing.
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
    ///     <see cref="FlushAsync" /> afterward. Applies backpressure if the channel is full.
    /// </summary>
    public ValueTask EnqueueAsync(RunEvent evt, IReadOnlyList<NotificationPublish> channels,
        CancellationToken cancellationToken = default)
    {
        var sequence = Interlocked.Increment(ref _enqueueSeq);
        return _channel.Writer.WriteAsync(new(evt, channels, sequence), cancellationToken);
    }

    /// <summary>
    ///     Waits until every event enqueued before this call has been persisted. Callers that need
    ///     ordering guarantees — e.g. before transitioning a run to a terminal status — should
    ///     await this so readers never observe the terminal state missing prior events. Throws the
    ///     original exception if the store write permanently failed for any event at or before the
    ///     caller's enqueue point.
    /// </summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        var target = Interlocked.Read(ref _enqueueSeq);

        while (true)
        {
            TaskCompletionSource tcs;
            lock (_flushGate)
            {
                // Throw when the caller's target has reached or passed the first permanently-
                // failed batch's MIN sequence. That condition means the caller's range `[1, target]`
                // overlaps — at minimum — the earliest known failure, so claiming durability
                // would be a lie. Tracking min (not max) stays correct after later batches succeed
                // and advance _flushedSeq past the failure.
                if (_error is { } err && target >= _errorMinSeq)
                {
                    err.Throw();
                }

                if (_flushedSeq >= target)
                {
                    return;
                }

                tcs = _batchFlushedTcs;
            }

            await tcs.Task.WaitAsync(cancellationToken);
        }
    }

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

                // Short idle window so a burst of events coalesces into one flush instead of
                // producing many small flushes as each event arrives.
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
                        // Stopping — drain below.
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
            // Complete the writer so producers waiting on capacity unblock, then drain everything
            // already queued. The drain uses stoppingToken so that — if the host's shutdown
            // deadline cancelled it — FlushBatchAsync's transient-retry loop sees the cancellation
            // and surfaces it via _error rather than retrying forever. On a graceful stop where
            // stoppingToken stays uncancelled, the drain proceeds normally to completion.
            _channel.Writer.TryComplete();
            while (reader.TryRead(out var item))
            {
                batch.Add(item);
                if (batch.Count >= MaxBatchSize)
                {
                    await FlushBatchAsync(batch, stoppingToken);
                    batch.Clear();
                }
            }

            if (batch.Count > 0)
            {
                await FlushBatchAsync(batch, stoppingToken);
            }
        }
    }

    private async Task FlushBatchAsync(List<EventRequest> batch, CancellationToken cancellationToken)
    {
        // All call sites guard with `batch.Count > 0`; the check here makes the invariant
        // explicit and lets SignalBatchCompleted drop its own maxSeq == 0 short-circuit.
        if (batch.Count == 0)
        {
            return;
        }

        var minSeq = batch[0].Sequence;
        var maxSeq = batch[0].Sequence;
        for (var i = 1; i < batch.Count; i++)
        {
            var seq = batch[i].Sequence;
            if (seq < minSeq)
            {
                minSeq = seq;
            }

            if (seq > maxSeq)
            {
                maxSeq = seq;
            }
        }

        var events = new RunEvent[batch.Count];
        for (var i = 0; i < batch.Count; i++)
        {
            events[i] = batch[i].Event;
        }

        // Retry transient errors indefinitely — stores define IsTransientException to cover only
        // deadlock, connection reset, and similar conditions that clear on a retry. Permanent
        // errors (schema, auth, constraint) propagate to FlushAsync callers so terminal
        // transitions never proceed with events unpersisted.
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
                SignalBatchCompleted(minSeq, maxSeq,
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
                    SignalBatchCompleted(minSeq, maxSeq,
                        ExceptionDispatchInfo.Capture(new OperationCanceledException(cancellationToken)));
                    return;
                }
            }
            catch (Exception ex)
            {
                Log.EventFlushFailed(logger, ex, events.Length);
                SignalBatchCompleted(minSeq, maxSeq, ExceptionDispatchInfo.Capture(ex));
                return;
            }
        }

        // Deduplicate notifications by channel across the whole flushed batch. A 500-event burst
        // on the same run produces one RunEvent(run.Id) publish instead of 500. Notifications are
        // idempotent wake-up signals, so individual failures are logged but never fail the batch:
        // the store write already succeeded and FlushAsync's durability guarantee is satisfied.
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
                    SignalBatchCompleted(minSeq, maxSeq, null);
                    return;
                }
                catch (Exception ex)
                {
                    Log.NotificationPublishFailed(logger, ex, pub.Channel);
                }
            }
        }

        SignalBatchCompleted(minSeq, maxSeq, null);
    }

    private void SignalBatchCompleted(long minSeq, long maxSeq, ExceptionDispatchInfo? error)
    {
        TaskCompletionSource completed;
        lock (_flushGate)
        {
            if (error is null)
            {
                if (maxSeq > _flushedSeq)
                {
                    _flushedSeq = maxSeq;
                }
            }
            else if (_error is null)
            {
                // Sticky first-failure capture. Record the failing batch's MIN sequence so the
                // FlushAsync guard can correctly observe: any caller whose target has reached or
                // passed this min has a `[1, target]` range that overlaps the permanently-failed
                // range. Later permanent failures never replace the first one — the earliest
                // known failure is what bounds durability.
                _error = error;
                _errorMinSeq = minSeq;
            }

            completed = _batchFlushedTcs;
            _batchFlushedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        completed.TrySetResult();
    }

    internal readonly record struct NotificationPublish(string Channel, string? Message);

    private readonly record struct EventRequest(
        RunEvent Event,
        IReadOnlyList<NotificationPublish> Notifications,
        long Sequence);

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