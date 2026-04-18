using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class InMemoryNotificationProvider(
    ILogger<InMemoryNotificationProvider> logger) : INotificationProvider
{
    private readonly Lock _lock = new();
    private readonly Dictionary<string, Dictionary<Guid, Func<string?, Task>>> _subscriptions = new();

    public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task PublishAsync(string channel, string? message = null,
        CancellationToken cancellationToken = default)
    {
        NotificationChannels.ValidateChannel(channel);

        Func<string?, Task>[] handlers;
        lock (_lock)
        {
            if (!_subscriptions.TryGetValue(channel, out var subscribers))
            {
                return Task.CompletedTask;
            }

            handlers = [.. subscribers.Values];
        }

        if (handlers.Length == 0)
        {
            return Task.CompletedTask;
        }

        var tasks = handlers.Select(handler => InvokeHandlerAsync(handler, message, channel));
        return Task.WhenAll(tasks);
    }

    public Task<IAsyncDisposable> SubscribeAsync(string channel, Func<string?, Task> handler,
        CancellationToken cancellationToken = default)
    {
        NotificationChannels.ValidateChannel(channel);

        var id = Guid.CreateVersion7();
        lock (_lock)
        {
            if (!_subscriptions.TryGetValue(channel, out var subscribers))
            {
                subscribers = new();
                _subscriptions[channel] = subscribers;
            }

            subscribers[id] = handler;
        }

        IAsyncDisposable disposable = new Subscription(this, channel, id);
        return Task.FromResult(disposable);
    }

    private async Task InvokeHandlerAsync(Func<string?, Task> handler, string? message, string channel)
    {
        try
        {
            await handler(message);
        }
        catch (Exception ex)
        {
            Log.NotificationHandlerFailed(logger, ex, channel);
        }
    }

    internal void RemoveSubscription(string channel, Guid id)
    {
        lock (_lock)
        {
            if (_subscriptions.TryGetValue(channel, out var subscribers))
            {
                subscribers.Remove(id);
                if (subscribers.Count == 0)
                {
                    _subscriptions.Remove(channel);
                }
            }
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1701, Level = LogLevel.Warning,
            Message = "Notification handler failed on channel {Channel}.")]
        public static partial void NotificationHandlerFailed(ILogger logger, Exception exception, string channel);
    }
}

file class Subscription(
    InMemoryNotificationProvider provider,
    string channel,
    Guid id) : IAsyncDisposable
{
    public ValueTask DisposeAsync()
    {
        provider.RemoveSubscription(channel, id);
        return ValueTask.CompletedTask;
    }
}