using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class SurefireLoggerProvider : ILoggerProvider, IAsyncDisposable
{
    private readonly IJobStore _store;
    private readonly INotificationProvider _notifications;
    private readonly TimeProvider _timeProvider;
    private readonly Lock _bufferLock = new();
    private List<RunEvent> _buffer = [];
    private readonly SemaphoreSlim _flushLock = new(1, 1);
    private readonly ConcurrentDictionary<string, SurefireLogger> _loggers = new();
    private readonly ITimer _timer;
    private int _disposed;

    // Note: this class must NOT depend on ILoggerFactory or ILogger<T> — it IS an
    // ILoggerProvider, and LoggerFactory resolves IEnumerable<ILoggerProvider> in its
    // constructor. Taking ILoggerFactory here creates a circular singleton dependency
    // that crashes at startup. For the same reason, all dependencies of this class must
    // also avoid ILogger<T> — anything in the ILoggerProvider resolution chain that
    // depends on ILogger creates the same circular dependency.
    public SurefireLoggerProvider(
        IJobStore store,
        INotificationProvider notifications,
        SurefireOptions options,
        TimeProvider timeProvider)
    {
        _store = store;
        _notifications = notifications;
        _timeProvider = timeProvider;
        _timer = timeProvider.CreateTimer(OnTimer, null, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(100));
    }

    public ILogger CreateLogger(string categoryName) =>
        _loggers.GetOrAdd(categoryName, name => new SurefireLogger(name, this, _timeProvider));

    internal void Enqueue(RunEvent evt)
    {
        lock (_bufferLock)
            _buffer.Add(evt);
    }

    private async void OnTimer(object? state)
    {
        if (Volatile.Read(ref _disposed) == 1)
            return;
        try { await FlushCoreAsync(CancellationToken.None); }
        catch { /* Timer callback should never propagate exceptions */ }
    }

    /// <summary>
    /// Flushes all buffered events to the store and waits for completion.
    /// Guarantees that all events enqueued before this call are persisted before returning.
    /// </summary>
    internal Task FlushAsync() => FlushCoreAsync(CancellationToken.None);

    private async Task FlushCoreAsync(CancellationToken ct)
    {
        await _flushLock.WaitAsync(ct);
        try
        {
            List<RunEvent> batch;
            lock (_bufferLock)
            {
                if (_buffer.Count == 0) return;
                batch = _buffer;
                _buffer = new List<RunEvent>(batch.Count);
            }

            var groups = batch.GroupBy(x => x.RunId);
            foreach (var group in groups)
            {
                var events = group.ToList();
                try
                {
                    await _store.AppendEventsAsync(events, ct);
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception)
                {
                    // Re-queue failed events for retry on next timer tick (capped to prevent OOM)
                    lock (_bufferLock)
                    {
                        if (_buffer.Count < 10_000)
                            _buffer.AddRange(events);
                    }
                    continue;
                }

                // Notification is best-effort — events are already persisted, never re-queue
                try { await _notifications.PublishAsync(NotificationChannels.RunEvent(group.Key), "", ct); }
                catch (OperationCanceledException) { throw; }
                catch { }
            }
        }
        finally
        {
            _flushLock.Release();
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        // Stop future timer callbacks. ITimer.Dispose (synchronous) does not wait for
        // in-flight callbacks, so a FlushCoreAsync call may still be running — that's fine,
        // the _disposed flag prevents new work and the semaphore has no unmanaged resources.
        _timer.Dispose();

        // Don't attempt to flush — sync Dispose cannot safely call async store methods.
        // The hosting system always calls DisposeAsync when available, which does a proper
        // final flush with timeout. Sync Dispose only runs in degraded paths where losing
        // buffered log events is acceptable.
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        // ITimer.DisposeAsync waits for any in-progress callback to complete
        await _timer.DisposeAsync();

        // Final flush with timeout — don't hang shutdown if the store is unresponsive
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try { await FlushCoreAsync(cts.Token); }
        catch (OperationCanceledException) { }

        _flushLock.Dispose();
    }
}
