using System.Diagnostics;

namespace Surefire;

// WARNING: Do NOT add ILogger<T> as a dependency here.
// This type is a dependency of SurefireLoggerProvider (an ILoggerProvider), while
// ILogger<T> depends on LoggerFactory, which resolves all ILoggerProviders at startup.
// Injecting ILogger<T> here would create a circular dependency.
internal sealed class InMemoryNotificationProvider : INotificationProvider
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

        foreach (var handler in handlers)
        {
            _ = InvokeHandlerAsync(handler, message, channel);
        }

        return Task.CompletedTask;
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

    private static async Task InvokeHandlerAsync(Func<string?, Task> handler, string? message, string channel)
    {
        try
        {
            await handler(message);
        }
        catch (Exception ex)
        {
            Trace.TraceWarning("Notification handler failed on channel {0}: {1}", channel, ex.Message);
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