using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Surefire.Redis;

/// <summary>
///     Redis notification provider using pub/sub.
/// </summary>
internal sealed class RedisNotificationProvider(
    IConnectionMultiplexer connection,
    ILogger<RedisNotificationProvider> logger) : INotificationProvider, IAsyncDisposable
{
    private readonly Channel<DispatchWorkItem> _dispatchChannel =
        Channel.CreateUnbounded<DispatchWorkItem>(new()
        {
            SingleReader = true,
            SingleWriter = false
        });

    private CancellationTokenSource? _cts;
    private Task? _dispatchLoop;

    private ISubscriber? _subscriber;

    public async ValueTask DisposeAsync()
    {
        if (_cts is { })
        {
            await _cts.CancelAsync();
        }

        _dispatchChannel.Writer.TryComplete();
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
    }

    public Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        _subscriber = connection.GetSubscriber();
        _cts = new();
        _dispatchLoop = DispatchLoopAsync(_cts.Token);
        return Task.CompletedTask;
    }

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        await connection.GetDatabase().PingAsync();
    }

    public async Task PublishAsync(string channel, string? message = null,
        CancellationToken cancellationToken = default)
    {
        NotificationChannels.ValidateChannel(channel);

        if (_subscriber is null)
        {
            throw new InvalidOperationException("Not initialized. Call InitializeAsync first.");
        }

        await _subscriber.PublishAsync(RedisChannel.Literal(channel), message ?? "");
    }

    public async Task<IAsyncDisposable> SubscribeAsync(string channel, Func<string?, Task> handler,
        CancellationToken cancellationToken = default)
    {
        NotificationChannels.ValidateChannel(channel);

        if (_subscriber is null)
        {
            throw new InvalidOperationException("Not initialized. Call InitializeAsync first.");
        }

        var redisChannel = RedisChannel.Literal(channel);
        Action<RedisChannel, RedisValue> redisHandler = (_, msg) =>
        {
            var payload = msg.IsNullOrEmpty ? null : msg.ToString();
            _dispatchChannel.Writer.TryWrite(new(handler, payload == "" ? null : payload, channel));
        };

        await _subscriber.SubscribeAsync(redisChannel, redisHandler);

        return new Subscription(_subscriber, redisChannel, redisHandler);
    }

    private async Task DispatchLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _dispatchChannel.Reader.WaitToReadAsync(cancellationToken))
            {
                while (_dispatchChannel.Reader.TryRead(out var workItem))
                {
                    try
                    {
                        await workItem.Handler(workItem.Payload);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex,
                            "Redis notification handler failed for channel '{Channel}'.",
                            workItem.Channel);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private readonly record struct DispatchWorkItem(Func<string?, Task> Handler, string? Payload, string Channel);
}

file sealed class Subscription(
    ISubscriber subscriber,
    RedisChannel channel,
    Action<RedisChannel, RedisValue> handler) : IAsyncDisposable
{
    public async ValueTask DisposeAsync()
    {
        await subscriber.UnsubscribeAsync(channel, handler);
    }
}