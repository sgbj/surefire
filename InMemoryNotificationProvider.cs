using System.Collections.Concurrent;
using System.Diagnostics;

namespace Surefire;

// This class must NOT depend on ILogger<T>. It is resolved as a dependency of
// SurefireLoggerProvider (an ILoggerProvider), and ILogger<T> requires LoggerFactory,
// which resolves all ILoggerProviders — creating a circular dependency at startup.
internal sealed class InMemoryNotificationProvider : INotificationProvider
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, Func<string, Task>>> _subscriptions = new();
    private readonly Lock _lock = new();

    public Task PublishAsync(string channel, string message, CancellationToken cancellationToken = default)
    {
        if (!_subscriptions.TryGetValue(channel, out var subscribers))
            return Task.CompletedTask;

        foreach (var handler in subscribers.Values)
            _ = InvokeHandlerAsync(handler, message, channel);

        return Task.CompletedTask;
    }

    private async Task InvokeHandlerAsync(Func<string, Task> handler, string message, string channel)
    {
        try { await handler(message); }
        catch (Exception ex) { Trace.TraceWarning("Notification handler failed on channel {0}: {1}", channel, ex.Message); }
    }

    public Task<IAsyncDisposable> SubscribeAsync(string channel, Func<string, Task> handler, CancellationToken cancellationToken = default)
    {
        var id = Guid.CreateVersion7();
        lock (_lock)
        {
            var subscribers = _subscriptions.GetOrAdd(channel, _ => new ConcurrentDictionary<Guid, Func<string, Task>>());
            subscribers[id] = handler;
        }

        IAsyncDisposable disposable = new Subscription(this, channel, id);
        return Task.FromResult(disposable);
    }

    internal void RemoveSubscription(string channel, Guid id)
    {
        lock (_lock)
        {
            if (_subscriptions.TryGetValue(channel, out var subscribers))
            {
                subscribers.TryRemove(id, out _);
                if (subscribers.IsEmpty)
                    _subscriptions.TryRemove(channel, out _);
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
