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
///         events poisons only that run's <see cref="FlushRunAsync" /> — sibling runs sharing the
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

    // Per-run flush state. Each entry is created lazily on first enqueue and dropped on terminal
    // via DropRunState. Keyed by RunEvent.RunId so that a permanent failure on one run's events
    // can't block sibling runs — every state has its own Lock, Tcs, FlushedSeq, and sticky Error.
    private readonly ConcurrentDictionary<string, PerRunFlushState> _runStates = new(StringComparer.Ordinal);

    // Global monotonic sequence — assigned to each enqueued event so batches stay in enqueue
    // order and so per-run watermarks can be expressed in a single shared number space.
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
    ///     persisted. Callers that need ordering guarantees — e.g. before transitioning the run to
    ///     a terminal status — should await this so readers never observe the terminal state
    ///     missing prior events. Throws the original exception if any of the run's own events
    ///     permanently failed. Sibling runs' failures do not propagate here.
    /// </summary>
    public async Task FlushRunAsync(string runId, CancellationToken cancellationToken = default)
    {
        if (!_runStates.TryGetValue(runId, out var state))
        {
            // Nothing was ever enqueued for this run (or its state was already dropped). Either
            // way, there is nothing to wait on and nothing the caller can fail to observe.
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
                // Sticky-within-run: once a permanent failure has been stamped for this run, every
                // subsequent flush observes it until the state is dropped on terminal. The error
                // binds the whole run because a failure in the middle of a run's event stream
                // means downstream readers can never see a consistent prefix — claiming durability
                // would be a lie regardless of whether later batches for the same run succeed.
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
            // already queued. The drain uses a fresh CTS bounded by ShutdownTimeout — independent
            // of stoppingToken — so events that were accepted with backpressure aren't dropped just
            // because the host's shutdown deadline cancelled stoppingToken first. A healthy store
            // finishes the drain in one or two round trips well under the budget. A wedged store
            // burns through the budget and FlushBatchAsync surfaces the timeout per-run via the
            // sticky error capture, matching the per-run-error semantics of the running case.
            // Same pattern as SurefireLogEventPump.ExecuteAsync's drain at L98-109.
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
        // All call sites guard with `batch.Count > 0`; the check here makes the invariant
        // explicit.
        if (batch.Count == 0)
        {
            return;
        }

        var events = new RunEvent[batch.Count];
        for (var i = 0; i < batch.Count; i++)
        {
            events[i] = batch[i].Event;
        }

        // Retry transient errors indefinitely — stores define IsTransientException to cover only
        // deadlock, connection reset, and similar conditions that clear on a retry. Permanent
        // errors (schema, auth, constraint) propagate to FlushRunAsync callers so terminal
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

        // Deduplicate notifications by channel across the whole flushed batch. A 500-event burst
        // on the same run produces one RunEvent(run.Id) publish instead of 500. Notifications are
        // idempotent wake-up signals, so individual failures are logged but never fail the batch:
        // the store write already succeeded and FlushRunAsync's durability guarantee is satisfied.
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
        // Group the batch by runId so we advance/stamp each run's state exactly once per call.
        // Tiny allocations here are amortized against a full store round trip; the batch has at
        // most MaxBatchSize entries and typically a handful of distinct runs.
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
            // Success path uses TryGetValue (not GetOrAdd): if the state was dropped while this
            // batch was in flight, the run has already terminated and no waiter needs signalling.
            // Failure path uses TryGetValue too — a dropped run cannot observe an error either,
            // so re-materializing the state would just leak.
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
                    // Sticky first-failure capture, per run. Record the failing batch's MIN
                    // sequence for this run so debugging/metrics can identify the earliest lost
                    // event; FlushRunAsync only reads the Error field (presence), not the min.
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