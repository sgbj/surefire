using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using Npgsql;

namespace Surefire.PostgreSql;

// This class must NOT depend on ILogger<T>. It is resolved as a dependency of
// SurefireLoggerProvider (an ILoggerProvider), and ILogger<T> requires LoggerFactory,
// which resolves all ILoggerProviders — creating a circular dependency at startup.
internal sealed class PostgreSqlNotificationProvider(PostgreSqlOptions pgOptions) : INotificationProvider, IAsyncDisposable
{
    private readonly NpgsqlDataSource _dataSource = pgOptions.DataSource;
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, Func<string, Task>>> _subscriptions = new();
    private readonly Channel<string> _pendingListens = Channel.CreateUnbounded<string>();
    private readonly Channel<string> _pendingUnlistens = Channel.CreateUnbounded<string>();
    private readonly HashSet<string> _listenedChannels = new();
    private readonly Lock _lock = new();
    private readonly CancellationTokenSource _disposeCts = new();
    private Task? _loopTask;
    private CancellationTokenSource? _waitCts;
    private int _disposed;

    public async Task PublishAsync(string channel, string message, CancellationToken cancellationToken = default)
    {
        // pg_notify() accepts any string as channel — no sanitization needed
        await using var cmd = _dataSource.CreateCommand("SELECT pg_notify(@channel, @payload)");
        cmd.Parameters.Add(new NpgsqlParameter<string>("channel", channel));
        cmd.Parameters.Add(new NpgsqlParameter<string>("payload", message));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public Task<IAsyncDisposable> SubscribeAsync(string channel, Func<string, Task> handler, CancellationToken cancellationToken = default)
    {
        var id = Guid.CreateVersion7();
        bool needsListen;
        lock (_lock)
        {
            var subscribers = _subscriptions.GetOrAdd(channel, _ => new ConcurrentDictionary<Guid, Func<string, Task>>());
            subscribers[id] = handler;
            needsListen = _listenedChannels.Add(channel);
        }

        if (needsListen)
            EnqueueListen(channel);

        IAsyncDisposable disposable = new Subscription(this, channel, id);
        return Task.FromResult(disposable);
    }

    private void EnqueueListen(string channel)
    {
        _pendingListens.Writer.TryWrite(channel);

        // Interrupt current WaitAsync so the loop picks up the new channel.
        // Cancel outside the lock to avoid deadlock from synchronous continuations.
        CancellationTokenSource? toCancel;
        lock (_lock)
        {
            toCancel = _waitCts;
        }
        try { toCancel?.Cancel(); } catch (ObjectDisposedException) { }

        EnsureLoopStarted();
    }

    private void EnsureLoopStarted()
    {
        lock (_lock)
        {
            _loopTask ??= Task.Run(ListenLoopAsync);
        }
    }

    private async Task ListenLoopAsync()
    {
        var backoff = TimeSpan.FromSeconds(1);
        var maxBackoff = TimeSpan.FromSeconds(30);

        while (_disposed == 0)
        {
            NpgsqlConnection? conn = null;
            try
            {
                conn = await _dataSource.OpenConnectionAsync(_disposeCts.Token);
                conn.Notification += OnNotification;
                backoff = TimeSpan.FromSeconds(1); // Reset on successful connect

                // Re-LISTEN all channels on reconnect
                string[] channelsToListen;
                lock (_lock)
                {
                    channelsToListen = [.. _listenedChannels];
                }
                foreach (var ch in channelsToListen)
                {
                    await using var listenCmd = new NpgsqlCommand($"LISTEN {QuoteIdentifier(ch)}", conn);
                    await listenCmd.ExecuteNonQueryAsync(_disposeCts.Token);
                }

                // Drain stale unlistens — those channels were either re-LISTENed above
                // or correctly omitted. Do NOT drain pending listens: channels subscribed
                // during the reconnect window are in _pendingListens and must flow into
                // the inner loop to get a LISTEN issued. Duplicate LISTENs are harmless no-ops.
                while (_pendingUnlistens.Reader.TryRead(out _)) { }

                while (_disposed == 0)
                {
                    // Drain all pending LISTEN commands
                    while (_pendingListens.Reader.TryRead(out var pgChannel))
                    {
                        await using var cmd = new NpgsqlCommand($"LISTEN {QuoteIdentifier(pgChannel)}", conn);
                        await cmd.ExecuteNonQueryAsync(_disposeCts.Token);
                    }

                    // Drain all pending UNLISTEN commands.
                    // Check _listenedChannels before issuing UNLISTEN: a concurrent SubscribeAsync
                    // may have re-added the channel between the unsubscribe and now.
                    while (_pendingUnlistens.Reader.TryRead(out var unlistenChannel))
                    {
                        bool stillRemoved;
                        lock (_lock) { stillRemoved = !_listenedChannels.Contains(unlistenChannel); }
                        if (stillRemoved)
                        {
                            await using var cmd = new NpgsqlCommand($"UNLISTEN {QuoteIdentifier(unlistenChannel)}", conn);
                            await cmd.ExecuteNonQueryAsync(_disposeCts.Token);
                        }
                    }

                    // Wait for notifications, interruptible by new LISTEN requests
                    CancellationTokenSource cts;
                    lock (_lock)
                    {
                        cts = _waitCts = new CancellationTokenSource();
                    }

                    try
                    {
                        await conn.WaitAsync(TimeSpan.FromSeconds(30), cts.Token);
                    }
                    catch (OperationCanceledException) { }
                    finally
                    {
                        lock (_lock)
                        {
                            if (_waitCts == cts)
                                _waitCts = null;
                        }
                        cts.Dispose();
                    }
                }
            }
            catch when (_disposed != 0) { break; }
            catch (Exception ex)
            {
                Trace.TraceWarning("PostgreSQL notification listen loop error: {0}", ex.Message);
            }
            finally
            {
                if (conn is not null)
                {
                    conn.Notification -= OnNotification;
                    await conn.DisposeAsync();
                }
            }

            if (_disposed != 0) break;

            // Exponential backoff before reconnecting (cancellable on dispose)
            try { await Task.Delay(backoff, _disposeCts.Token); }
            catch (OperationCanceledException) { break; }

            backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, maxBackoff.Ticks));
        }
    }

    private void OnNotification(object sender, NpgsqlNotificationEventArgs e)
    {
        if (_subscriptions.TryGetValue(e.Channel, out var subscribers))
        {
            foreach (var handler in subscribers.Values)
                _ = InvokeHandlerAsync(handler, e.Payload);
        }
    }

    private static async Task InvokeHandlerAsync(Func<string, Task> handler, string message)
    {
        try { await handler(message); }
        catch { /* Handlers are responsible for their own error handling */ }
    }

    internal void RemoveSubscription(string channel, Guid id)
    {
        bool shouldUnlisten = false;
        lock (_lock)
        {
            if (_subscriptions.TryGetValue(channel, out var subscribers))
            {
                subscribers.TryRemove(id, out _);
                if (subscribers.IsEmpty)
                {
                    _subscriptions.TryRemove(channel, out _);
                    _listenedChannels.Remove(channel);
                    shouldUnlisten = true;
                }
            }
        }
        if (shouldUnlisten)
        {
            _pendingUnlistens.Writer.TryWrite(channel);

            CancellationTokenSource? toCancel;
            lock (_lock) { toCancel = _waitCts; }
            try { toCancel?.Cancel(); } catch (ObjectDisposedException) { }
        }
    }

    /// <summary>
    /// Quotes a PostgreSQL identifier for use in LISTEN/UNLISTEN commands.
    /// Double-quotes are escaped by doubling them per SQL standard.
    /// </summary>
    private static string QuoteIdentifier(string identifier) =>
        $"\"{identifier.Replace("\"", "\"\"")}\"";

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _pendingListens.Writer.TryComplete();
        _pendingUnlistens.Writer.TryComplete();
        try { _disposeCts.Cancel(); } catch { }

        CancellationTokenSource? toCancel;
        lock (_lock)
        {
            toCancel = _waitCts;
        }
        try { toCancel?.Cancel(); } catch (ObjectDisposedException) { }

        if (_loopTask is not null)
        {
            try { await _loopTask; }
            catch { }
        }

        _disposeCts.Dispose();
        // NpgsqlDataSource is owned by PostgreSqlOptions and disposed via DI.
    }
}

file sealed class Subscription(
    PostgreSqlNotificationProvider provider,
    string channel,
    Guid id) : IAsyncDisposable
{
    public ValueTask DisposeAsync()
    {
        provider.RemoveSubscription(channel, id);
        return ValueTask.CompletedTask;
    }
}
