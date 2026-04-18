using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class SurefireLogEventPump(
    IJobStore store,
    INotificationProvider notifications,
    SurefireOptions options,
    SurefireInstrumentation instrumentation,
    TimeProvider timeProvider,
    Backoff backoff,
    ILogger<SurefireLogEventPump> logger) : BackgroundService
{
    private static readonly TimeSpan BackoffInitial = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(10);

    private readonly Channel<LogEntry> _channel = Channel.CreateBounded<LogEntry>(new BoundedChannelOptions(8192)
    {
        FullMode = BoundedChannelFullMode.DropWrite,
        SingleReader = true,
        SingleWriter = false
    });

    private readonly ConcurrentDictionary<string, RunFlushState> _runFlushStates = new(StringComparer.Ordinal);

    public bool TryEnqueue(LogEntry entry)
    {
        if (string.IsNullOrWhiteSpace(entry.RunId) || string.IsNullOrWhiteSpace(entry.Message))
        {
            return false;
        }

        var state = _runFlushStates.GetOrAdd(entry.RunId, static _ => new());
        if (!state.TryEnqueue(entry, _channel.Writer))
        {
            instrumentation.RecordLogEntryDropped("channel_full");
            return false;
        }

        return true;
    }

    public async ValueTask FlushRunAsync(string runId, CancellationToken cancellationToken = default)
    {
        if (!_runFlushStates.TryGetValue(runId, out var state))
        {
            return;
        }

        await state.WaitForFlushAsync(cancellationToken);
    }

    /// <summary>
    ///     Drops the per-run flush bookkeeping. Must be called after a successful terminal transition,
    ///     once no further log entries can be enqueued for the run.
    /// </summary>
    public void DropRunState(string runId) => _runFlushStates.TryRemove(runId, out _);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var buffer = new List<LogEntry>(128);
        var backoffAttempt = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var hasItem = await _channel.Reader.WaitToReadAsync(stoppingToken);
                if (!hasItem)
                {
                    break;
                }

                await ReadAndFlushAsync(buffer, stoppingToken);
                backoffAttempt = 0;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                if (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                Log.PumpLoopFailed(logger, ex);
                instrumentation.RecordStoreRetry("pump");
                await Task.Delay(backoff.NextDelay(backoffAttempt++, BackoffInitial, BackoffMax), timeProvider,
                    stoppingToken);
            }
        }

        // Drain remaining items on shutdown so final job logs aren't lost.
        // Bound the drain to ShutdownTimeout so a hanging store doesn't block process exit.
        _channel.Writer.TryComplete();
        try
        {
            using var drainCts = new CancellationTokenSource(options.ShutdownTimeout);
            await ReadAndFlushAsync(buffer, drainCts.Token);
        }
        catch (Exception ex)
        {
            Log.PumpDrainFailed(logger, ex);
        }
    }

    private async Task ReadAndFlushAsync(List<LogEntry> buffer, CancellationToken cancellationToken)
    {
        buffer.Clear();
        while (buffer.Count < 128 && _channel.Reader.TryRead(out var entry))
        {
            buffer.Add(entry);
        }

        if (buffer.Count == 0)
        {
            return;
        }

        try
        {
            var events = buffer.Select(entry => new RunEvent
                {
                    RunId = entry.RunId,
                    EventType = RunEventType.Log,
                    Payload = JsonSerializer.Serialize(new()
                    {
                        Timestamp = entry.Timestamp,
                        Category = entry.Category,
                        Level = (int)entry.Level,
                        EventId = entry.EventId,
                        EventName = entry.EventName,
                        Message = entry.Message,
                        Exception = entry.Exception
                    }, SurefireJsonContext.Default.LogEventPayload),
                    CreatedAt = entry.Timestamp,
                    Attempt = entry.Attempt
                })
                .ToList();

            await store.AppendEventsAsync(events, cancellationToken);
            MarkRunsFlushed(buffer);

            foreach (var runId in buffer.Select(e => e.RunId).Distinct(StringComparer.Ordinal))
            {
                try
                {
                    await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, cancellationToken);
                }
                catch (Exception ex)
                {
                    Log.NotificationFailed(logger, ex, runId);
                }
            }
        }
        catch (Exception ex)
        {
            for (var i = 0; i < buffer.Count; i++)
            {
                instrumentation.RecordLogEntryDropped("flush_failed");
            }

            FailRunsFlushed(buffer, ex);
            throw;
        }
    }

    private void MarkRunsFlushed(List<LogEntry> buffer)
    {
        foreach (var group in buffer.GroupBy(static entry => entry.RunId, StringComparer.Ordinal))
        {
            if (_runFlushStates.TryGetValue(group.Key, out var state))
            {
                state.MarkFlushed(group.Max(static entry => entry.Sequence));
            }
        }
    }

    private void FailRunsFlushed(List<LogEntry> buffer, Exception ex)
    {
        foreach (var group in buffer.GroupBy(static entry => entry.RunId, StringComparer.Ordinal))
        {
            if (_runFlushStates.TryGetValue(group.Key, out var state))
            {
                state.FailFlush(group.Max(static entry => entry.Sequence), ex);
            }
        }
    }

    internal readonly record struct LogEntry(
        string RunId,
        long Sequence,
        int Attempt,
        DateTimeOffset Timestamp,
        string Category,
        LogLevel Level,
        int EventId,
        string? EventName,
        string Message,
        string? Exception);

    private sealed class RunFlushState
    {
        private readonly Lock _gate = new();
        private readonly SortedDictionary<long, List<TaskCompletionSource<bool>>> _waiters = [];
        private long _enqueuedSequence;
        private long _flushedSequence;

        public bool TryEnqueue(LogEntry entry, ChannelWriter<LogEntry> writer)
        {
            lock (_gate)
            {
                var nextSequence = _enqueuedSequence + 1;
                if (!writer.TryWrite(entry with { Sequence = nextSequence }))
                {
                    return false;
                }

                _enqueuedSequence = nextSequence;
                return true;
            }
        }

        public async Task WaitForFlushAsync(CancellationToken cancellationToken)
        {
            TaskCompletionSource<bool>? waiter = null;

            lock (_gate)
            {
                var targetSequence = _enqueuedSequence;
                if (_flushedSequence >= targetSequence)
                {
                    return;
                }

                waiter = new(TaskCreationOptions.RunContinuationsAsynchronously);
                if (!_waiters.TryGetValue(targetSequence, out var targetWaiters))
                {
                    targetWaiters = [];
                    _waiters[targetSequence] = targetWaiters;
                }

                targetWaiters.Add(waiter);
            }

            using var cancellationRegistration = cancellationToken.CanBeCanceled
                ? cancellationToken.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetCanceled(),
                    waiter)
                : default;

            await waiter.Task;
        }

        public void MarkFlushed(long flushedSequence)
        {
            List<TaskCompletionSource<bool>>? ready = null;

            lock (_gate)
            {
                if (flushedSequence > _flushedSequence)
                {
                    _flushedSequence = flushedSequence;
                }

                MoveReadyWaiters(_flushedSequence, ref ready);
            }

            CompleteWaiters(ready, static waiter => waiter.TrySetResult(true));
        }

        public void FailFlush(long failedSequence, Exception ex)
        {
            List<TaskCompletionSource<bool>>? failed = null;

            lock (_gate)
            {
                MoveReadyWaiters(failedSequence, ref failed);
            }

            CompleteWaiters(failed, waiter => waiter.TrySetException(ex));
        }

        private void MoveReadyWaiters(long completedSequence, ref List<TaskCompletionSource<bool>>? destination)
        {
            if (_waiters.Count == 0)
            {
                return;
            }

            foreach (var (sequence, waiters) in _waiters.TakeWhile(pair => pair.Key <= completedSequence).ToArray())
            {
                destination ??= [];
                destination.AddRange(waiters);
                _waiters.Remove(sequence);
            }
        }

        private static void CompleteWaiters(List<TaskCompletionSource<bool>>? waiters,
            Action<TaskCompletionSource<bool>> complete)
        {
            if (waiters is null)
            {
                return;
            }

            foreach (var waiter in waiters)
            {
                complete(waiter);
            }
        }
    }
}

internal sealed partial class SurefireLogEventPump
{
    private static partial class Log
    {
        [LoggerMessage(EventId = 1601, Level = LogLevel.Error, Message = "Log event pump loop failed.")]
        public static partial void PumpLoopFailed(ILogger logger, Exception exception);

        [LoggerMessage(EventId = 1602, Level = LogLevel.Error, Message = "Log event pump drain failed on shutdown.")]
        public static partial void PumpDrainFailed(ILogger logger, Exception exception);

        [LoggerMessage(EventId = 1603, Level = LogLevel.Warning,
            Message = "Log event notification failed for run {RunId}.")]
        public static partial void NotificationFailed(ILogger logger, Exception exception, string runId);
    }
}

internal sealed class LogEventPayload
{
    public required DateTimeOffset Timestamp { get; init; }
    public required string Category { get; init; }
    public required int Level { get; init; }
    public required int EventId { get; init; }
    public string? EventName { get; init; }
    public required string Message { get; init; }
    public string? Exception { get; init; }
}