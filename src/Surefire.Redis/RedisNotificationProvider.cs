using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Surefire.Redis;

/// <summary>
///     Redis notification provider using pub/sub.
/// </summary>
internal sealed class RedisNotificationProvider(
    IConnectionMultiplexer connection,
    TimeProvider timeProvider,
    ILogger<RedisNotificationProvider> logger) : INotificationProvider, IAsyncDisposable
{
    private const int MaxPublishBatch = 500;

    private const int OutgoingQueueCapacity = MaxPublishBatch * 16;

    // Outgoing publish coalescing: a burst of identical (channel, message) pairs issued by a
    // batch of completions collapses to a single PUBLISH within a 2 ms window, and the surviving
    // publishes are dispatched in a single pipeline via IDatabase.CreateBatch so round-trip cost
    // is one socket write regardless of batch size.
    private static readonly TimeSpan PublishFlushInterval = TimeSpan.FromMilliseconds(2);

    private readonly Channel<DispatchWorkItem> _dispatchChannel =
        Channel.CreateUnbounded<DispatchWorkItem>(new()
        {
            SingleReader = true,
            SingleWriter = false
        });

    // Bounded with Wait — producers apply real backpressure if Redis publishing stalls rather
    // than the queue growing unbounded.
    private readonly Channel<OutgoingPublish> _outgoingChannel =
        Channel.CreateBounded<OutgoingPublish>(new BoundedChannelOptions(OutgoingQueueCapacity)
        {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

    private CancellationTokenSource? _cts;
    private Task? _dispatchLoop;
    private long _lastPublishFailureUnixTicks;
    private long _publishFailureCount;
    private Task? _publishLoop;

    private ISubscriber? _subscriber;

    public async ValueTask DisposeAsync()
    {
        // Drain outgoing publishes before cancelling dispatch. Completing the writer causes the
        // publish loop to exit naturally when the queue empties, preserving every enqueued
        // notification. Cancellation comes after so in-flight flushes continue through their
        // best-effort error handling.
        _outgoingChannel.Writer.TryComplete();
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
        // Publish loop runs under CancellationToken.None so DisposeAsync can drain it purely via
        // channel completion without losing queued publishes.
        _publishLoop = PublishLoopAsync(CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        await connection.GetDatabase().PingAsync();
    }

    public Task PublishAsync(string channel, string? message = null,
        CancellationToken cancellationToken = default)
    {
        NotificationChannels.ValidateChannel(channel);

        if (_subscriber is null)
        {
            throw new InvalidOperationException("Not initialized. Call InitializeAsync first.");
        }

        return _outgoingChannel.Writer.WriteAsync(new(channel, message), cancellationToken).AsTask();
    }

    public NotificationPublishHealth GetPublishHealth()
    {
        var ticks = Interlocked.Read(ref _lastPublishFailureUnixTicks);
        return new(
            Interlocked.Read(ref _publishFailureCount),
            ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero));
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

    private async Task PublishLoopAsync(CancellationToken cancellationToken)
    {
        var reader = _outgoingChannel.Reader;
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

                    using var timeoutCts = cancellationToken.CanBeCanceled
                        ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                        : new();
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
                    await FlushPublishesAsync(batch);
                    batch.Clear();
                    seen.Clear();
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task FlushPublishesAsync(List<OutgoingPublish> batch)
    {
        var subscriber = _subscriber;
        if (subscriber is null)
        {
            return;
        }

        // Pipeline all publishes onto a single multiplexer connection via IBatch. Tasks are
        // collected eagerly; Execute flushes them together; WhenAll completes when every publish
        // has been acknowledged. One network round trip amortized across the whole batch.
        var redisBatch = connection.GetDatabase().CreateBatch();
        var tasks = new Task[batch.Count];
        for (var i = 0; i < batch.Count; i++)
        {
            tasks[i] = redisBatch.PublishAsync(RedisChannel.Literal(batch[i].Channel), batch[i].Message ?? "");
        }

        redisBatch.Execute();

        try
        {
            await Task.WhenAll(tasks);
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _publishFailureCount);
            Interlocked.Exchange(ref _lastPublishFailureUnixTicks, timeProvider.GetUtcNow().UtcTicks);
            logger.LogWarning(ex, "Failed to publish {Count} coalesced Redis notifications.", batch.Count);
        }
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

    private readonly record struct OutgoingPublish(string Channel, string? Message);

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