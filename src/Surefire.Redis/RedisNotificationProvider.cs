using StackExchange.Redis;

namespace Surefire.Redis;

/// <summary>
///     Redis notification provider using pub/sub.
/// </summary>
internal sealed class RedisNotificationProvider(RedisOptions options) : INotificationProvider, IAsyncDisposable
{
    private ISubscriber? _subscriber;

    public ValueTask DisposeAsync() => options.DisposeAsync();

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var mux = await options.GetConnectionAsync();
        _subscriber = mux.GetSubscriber();
    }

    public async Task PublishAsync(string channel, string? message = null,
        CancellationToken cancellationToken = default)
    {
        NotificationChannels.ValidateChannel(channel);

        if (_subscriber is null)
        {
            return;
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
        await _subscriber.SubscribeAsync(redisChannel, async (_, msg) =>
        {
            try
            {
                var payload = msg.IsNullOrEmpty ? null : msg.ToString();
                await handler(payload == "" ? null : payload);
            }
            catch
            {
                // Best-effort delivery
            }
        });

        return new Subscription(_subscriber, redisChannel);
    }

    private sealed class Subscription(ISubscriber subscriber, RedisChannel channel) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await subscriber.UnsubscribeAsync(channel);
        }
    }
}