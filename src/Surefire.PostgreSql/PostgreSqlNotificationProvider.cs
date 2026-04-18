using System.Collections.Concurrent;
using System.Data;
using System.Net.Sockets;
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
    private static readonly TimeSpan ReconnectBackoffInitial = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan ReconnectBackoffMax = TimeSpan.FromSeconds(30);

    private readonly Channel<DispatchWorkItem> _dispatchQueue =
        Channel.CreateUnbounded<DispatchWorkItem>(new()
        {
            SingleReader = true,
            SingleWriter = false
        });

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, Func<string?, Task>>> _handlers = new();
    private readonly Lock _lock = new();
    private readonly ConcurrentQueue<string> _pendingListens = new();
    private readonly ConcurrentQueue<string> _pendingUnlistens = new();
    private CancellationTokenSource? _cts;
    private Task? _dispatchLoop;
    private NpgsqlConnection? _listenConnection;
    private CancellationTokenSource? _listenInterrupt;
    private Task? _listenLoop;
    private int _reconnectAttempt;

    public async ValueTask DisposeAsync()
    {
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

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        _cts = new();
        _listenConnection = await dataSource.OpenConnectionAsync(cancellationToken);
        RegisterNotificationHandler(_listenConnection);
        _dispatchLoop = DispatchLoopAsync(_cts.Token);
        _listenLoop = ListenLoopAsync(_cts.Token);
    }

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        await cmd.ExecuteScalarAsync(cancellationToken);
    }

    public async Task PublishAsync(string channel, string? message = null,
        CancellationToken cancellationToken = default)
    {
        NotificationChannels.ValidateChannel(channel);

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();

        if (message is null)
        {
            cmd.CommandText = "SELECT pg_notify(@channel, NULL)";
            cmd.Parameters.AddWithValue("channel", channel);
        }
        else
        {
            cmd.CommandText = "SELECT pg_notify(@channel, @payload)";
            cmd.Parameters.AddWithValue("channel", channel);
            cmd.Parameters.AddWithValue("payload", message);
        }

        await cmd.ExecuteNonQueryAsync(cancellationToken);
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

                    // Drain any pending listens that arrived before connection was ready
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
                        // Interrupted to process new pending listens; continue the loop
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
            // Handlers can legitimately outlive local synchronization primitives during teardown.
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