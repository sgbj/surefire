using System.Collections.Concurrent;
using System.Data;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Surefire.PostgreSql;

/// <summary>
///     PostgreSQL notification provider using LISTEN/NOTIFY.
/// </summary>
internal sealed class PostgreSqlNotificationProvider(
    NpgsqlDataSource dataSource,
    TimeProvider timeProvider,
    Backoff backoff,
    ILogger<PostgreSqlNotificationProvider> logger)
    : INotificationProvider, IAsyncDisposable
{
    private const int MaxPublishBatch = 500;
    private const int OutgoingQueueCapacity = MaxPublishBatch * 16;
    private static readonly TimeSpan ReconnectBackoffInitial = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan ReconnectBackoffMax = TimeSpan.FromSeconds(30);

    // PublishAsync enqueues instead of executing. A background loop drains, dedupes
    // (channel, message) pairs within a 2 ms idle window, and emits the survivors as one
    // multi-pg_notify round trip.
    private static readonly TimeSpan PublishFlushInterval = TimeSpan.FromMilliseconds(2);

    private readonly Channel<DispatchWorkItem> _dispatchQueue =
        Channel.CreateUnbounded<DispatchWorkItem>(new()
        {
            SingleReader = true,
            SingleWriter = false
        });

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, Func<string?, Task>>> _handlers = new();
    private readonly Lock _lock = new();

    // Bounded+Wait applies backpressure to producers if PG publishing stalls, instead of the
    // queue growing unbounded.
    private readonly Channel<OutgoingPublish> _outgoingQueue =
        Channel.CreateBounded<OutgoingPublish>(new BoundedChannelOptions(OutgoingQueueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

    private readonly ConcurrentQueue<string> _pendingListens = new();
    private readonly ConcurrentQueue<string> _pendingUnlistens = new();
    private CancellationTokenSource? _cts;
    private Task? _dispatchLoop;
    private long _lastPublishFailureUnixTicks;
    private NpgsqlConnection? _listenConnection;
    private CancellationTokenSource? _listenInterrupt;
    private Task? _listenLoop;

    private long _publishFailureCount;
    private Task? _publishLoop;
    private int _reconnectAttempt;

    public async ValueTask DisposeAsync()
    {
        // Complete the writer first so the publish loop drains naturally; cancelling the shared
        // CTS before the drain would lose queued notifications.
        _outgoingQueue.Writer.TryComplete();
        if (_publishLoop is { } publishLoop)
        {
            try
            {
                await publishLoop;
            }
            catch (OperationCanceledException)
            {
            }
        }

        if (_cts is { })
        {
            await _cts.CancelAsync();
        }

        try
        {
            _listenInterrupt?.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        if (_listenLoop is { })
        {
            try
            {
                await _listenLoop;
            }
            catch (OperationCanceledException)
            {
            }
        }

        _dispatchQueue.Writer.TryComplete();
        if (_dispatchLoop is { })
        {
            try
            {
                await _dispatchLoop;
            }
            catch (OperationCanceledException)
            {
            }
        }

        _cts?.Dispose();
        _listenInterrupt?.Dispose();

        if (_listenConnection is { })
        {
            await _listenConnection.DisposeAsync();
        }
    }

    public NotificationPublishHealth GetPublishHealth()
    {
        var ticks = Interlocked.Read(ref _lastPublishFailureUnixTicks);
        return new(
            Interlocked.Read(ref _publishFailureCount),
            ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero));
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        _cts = new();
        _listenConnection = await dataSource.OpenConnectionAsync(cancellationToken);
        RegisterNotificationHandler(_listenConnection);
        _dispatchLoop = DispatchLoopAsync(_cts.Token);
        _listenLoop = ListenLoopAsync(_cts.Token);
        // Runs under CancellationToken.None so DisposeAsync drains via channel completion.
        // Cancelling via _cts would leave queued notifications unflushed.
        _publishLoop = PublishLoopAsync(CancellationToken.None);
    }

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        await cmd.ExecuteScalarAsync(cancellationToken);
    }

    public Task PublishAsync(string channel, string? message = null,
        CancellationToken cancellationToken = default)
    {
        NotificationChannels.ValidateChannel(channel);
        return _outgoingQueue.Writer.WriteAsync(new(channel, message), cancellationToken).AsTask();
    }

    public Task<IAsyncDisposable> SubscribeAsync(string channel, Func<string?, Task> handler,
        CancellationToken cancellationToken = default)
    {
        NotificationChannels.ValidateChannel(channel);

        lock (_lock)
        {
            var handlers = _handlers.GetOrAdd(channel, _ => new());
            var subscriptionId = Guid.NewGuid();
            handlers[subscriptionId] = handler;

            _pendingListens.Enqueue(channel);
            _listenInterrupt?.Cancel();

            return Task.FromResult<IAsyncDisposable>(new Subscription(this, channel, subscriptionId));
        }
    }

    private async Task PublishLoopAsync(CancellationToken cancellationToken)
    {
        var reader = _outgoingQueue.Reader;
        var batch = new List<OutgoingPublish>(MaxPublishBatch);
        var seen = new HashSet<(string Channel, string? Message)>();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (!await reader.WaitToReadAsync(cancellationToken))
                {
                    return;
                }

                var deadline = Environment.TickCount64 + (long)PublishFlushInterval.TotalMilliseconds;
                while (batch.Count < MaxPublishBatch)
                {
                    if (reader.TryRead(out var item))
                    {
                        if (seen.Add((item.Channel, item.Message)))
                        {
                            batch.Add(item);
                        }

                        continue;
                    }

                    var remaining = deadline - Environment.TickCount64;
                    if (remaining <= 0 || batch.Count == 0)
                    {
                        break;
                    }

                    using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    timeoutCts.CancelAfter(TimeSpan.FromMilliseconds(remaining));
                    try
                    {
                        if (!await reader.WaitToReadAsync(timeoutCts.Token))
                        {
                            break;
                        }
                    }
                    catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                }

                if (batch.Count > 0)
                {
                    await FlushPublishesAsync(batch, cancellationToken);
                    batch.Clear();
                    seen.Clear();
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task FlushPublishesAsync(List<OutgoingPublish> batch, CancellationToken cancellationToken)
    {
        try
        {
            await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
            await using var cmd = conn.CreateCommand();

            // One SELECT with N pg_notify() calls: single round trip, N channel activations.
            // PG also coalesces identical (channel, payload) pairs within a transaction, so
            // anything past our HashSet dedup is still collapsed server-side.
            var sql = new StringBuilder("SELECT ");
            for (var i = 0; i < batch.Count; i++)
            {
                if (i > 0)
                {
                    sql.Append(", ");
                }

                var channelParam = $"@c{i}";
                cmd.Parameters.AddWithValue(channelParam, batch[i].Channel);
                if (batch[i].Message is { } message)
                {
                    var payloadParam = $"@p{i}";
                    cmd.Parameters.AddWithValue(payloadParam, message);
                    sql.Append("pg_notify(").Append(channelParam).Append(", ").Append(payloadParam).Append(')');
                }
                else
                {
                    sql.Append("pg_notify(").Append(channelParam).Append(", NULL)");
                }
            }

            cmd.CommandText = sql.ToString();
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _publishFailureCount);
            Interlocked.Exchange(ref _lastPublishFailureUnixTicks, timeProvider.GetUtcNow().UtcTicks);
            logger.LogWarning(ex, "Failed to flush {Count} coalesced PostgreSQL notifications.", batch.Count);
        }
    }

    private async Task ListenLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (_listenConnection is not { State: ConnectionState.Open })
                {
                    if (_listenConnection is { })
                    {
                        await _listenConnection.DisposeAsync();
                    }

                    _listenConnection = await dataSource.OpenConnectionAsync(cancellationToken);
                    if (_reconnectAttempt > 0)
                    {
                        logger.LogInformation(
                            "PostgreSQL notification listener reconnected after {Attempts} attempt(s).",
                            _reconnectAttempt);
                    }

                    _reconnectAttempt = 0;

                    RegisterNotificationHandler(_listenConnection);

                    // Drop pending listens accumulated before the connection was ready.
                    while (_pendingListens.TryDequeue(out _))
                    {
                    }

                    foreach (var channel in _handlers.Keys)
                    {
                        await using var cmd = _listenConnection.CreateCommand();
                        cmd.CommandText = $"LISTEN \"{channel}\"";
                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                    }
                }

                while (!cancellationToken.IsCancellationRequested)
                {
                    CancellationTokenSource interrupt;
                    lock (_lock)
                    {
                        var old = _listenInterrupt;
                        interrupt = _listenInterrupt = new();
                        old?.Dispose();
                    }

                    while (_pendingListens.TryDequeue(out var pendingChannel))
                    {
                        NotificationChannels.ValidateChannel(pendingChannel);

                        await using var cmd = _listenConnection.CreateCommand();
                        cmd.CommandText = $"LISTEN \"{pendingChannel}\"";
                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                    }

                    while (_pendingUnlistens.TryDequeue(out var unlChannel))
                    {
                        NotificationChannels.ValidateChannel(unlChannel);

                        await using var unlCmd = _listenConnection.CreateCommand();
                        unlCmd.CommandText = $"UNLISTEN \"{unlChannel}\"";
                        await unlCmd.ExecuteNonQueryAsync(cancellationToken);
                    }

                    using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                        cancellationToken, interrupt.Token);

                    try
                    {
                        await _listenConnection.WaitAsync(linked.Token);
                    }
                    catch (OperationCanceledException) when (interrupt.IsCancellationRequested
                                                             && !cancellationToken.IsCancellationRequested)
                    {
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                if (IsExpectedListenInterruption(ex, cancellationToken))
                {
                    continue;
                }

                var delay = backoff.NextDelay(_reconnectAttempt, ReconnectBackoffInitial, ReconnectBackoffMax);
                _reconnectAttempt++;
                logger.LogWarning(ex,
                    "PostgreSQL notification listen loop failed (attempt {Attempt}). Retrying in {DelayMs}ms.",
                    _reconnectAttempt,
                    (int)delay.TotalMilliseconds);
                await Task.Delay(delay, timeProvider, cancellationToken);
            }
        }
    }

    private void RegisterNotificationHandler(NpgsqlConnection connection)
    {
        connection.Notification += (_, args) =>
        {
            if (_handlers.TryGetValue(args.Channel, out var handlers))
            {
                var payload = args.Payload == "" ? null : args.Payload;

                foreach (var handler in handlers.Values)
                {
                    _dispatchQueue.Writer.TryWrite(new(handler, payload, args.Channel));
                }
            }
        };
    }

    private async Task DispatchLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _dispatchQueue.Reader.WaitToReadAsync(cancellationToken))
            {
                while (_dispatchQueue.Reader.TryRead(out var workItem))
                {
                    await InvokeHandlerAsync(workItem.Handler, workItem.Payload, workItem.Channel);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task InvokeHandlerAsync(Func<string?, Task> handler, string? payload, string channel)
    {
        try
        {
            await handler(payload);
        }
        catch (ObjectDisposedException)
        {
            // Handlers can outlive local sync primitives during teardown.
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "PostgreSQL notification handler failed on channel {Channel}.", channel);
        }
    }

    private static bool IsExpectedListenInterruption(Exception ex, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return true;
        }

        return ex is NpgsqlException
        {
            InnerException: IOException
            {
                InnerException: SocketException
                {
                    SocketErrorCode: SocketError.OperationAborted
                }
            }
        };
    }

    internal Task UnsubscribeAsync(string channel, Guid subscriptionId)
    {
        lock (_lock)
        {
            if (!_handlers.TryGetValue(channel, out var handlers))
            {
                return Task.CompletedTask;
            }

            handlers.TryRemove(subscriptionId, out _);

            if (handlers.IsEmpty && _handlers.TryRemove(channel, out _))
            {
                _pendingUnlistens.Enqueue(channel);
                _listenInterrupt?.Cancel();
            }
        }

        return Task.CompletedTask;
    }

    private readonly record struct OutgoingPublish(string Channel, string? Message);

    private readonly record struct DispatchWorkItem(Func<string?, Task> Handler, string? Payload, string Channel);
}

file sealed class Subscription(
    PostgreSqlNotificationProvider provider,
    string channel,
    Guid subscriptionId) : IAsyncDisposable
{
    public async ValueTask DisposeAsync()
    {
        await provider.UnsubscribeAsync(channel, subscriptionId);
    }
}
