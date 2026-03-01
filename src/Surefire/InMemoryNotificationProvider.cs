using System.Collections.Concurrent;
using System.Diagnostics;

namespace Surefire;

internal sealed class InMemoryNotificationProvider : INotificationProvider
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<Guid, Func<string, Task>>> _subscriptions = new();

    public Task PublishAsync(string channel, string message, CancellationToken cancellationToken = default)
    {
        if (!_subscriptions.TryGetValue(channel, out var subscribers))
            return Task.CompletedTask;

        foreach (var handler in subscribers.Values)
            _ = InvokeHandlerAsync(handler, message, channel);

        return Task.CompletedTask;
    }

    private static async Task InvokeHandlerAsync(Func<string, Task> handler, string message, string channel)
    {
        try { await handler(message); }
        catch (Exception ex) { Debug.WriteLine($"[Surefire] Notification handler failed on channel {channel}: {ex}"); }
    }

    public Task<IAsyncDisposable> SubscribeAsync(string channel, Func<string, Task> handler, CancellationToken cancellationToken = default)
    {
        var id = Guid.CreateVersion7();
        var subscribers = _subscriptions.GetOrAdd(channel, _ => new ConcurrentDictionary<Guid, Func<string, Task>>());
        subscribers[id] = handler;

        IAsyncDisposable disposable = new Subscription(_subscriptions, channel, subscribers, id);
        return Task.FromResult(disposable);
    }
}

file class Subscription(
    ConcurrentDictionary<string, ConcurrentDictionary<Guid, Func<string, Task>>> subscriptions,
    string channel,
    ConcurrentDictionary<Guid, Func<string, Task>> subscribers,
    Guid id) : IAsyncDisposable
{
    public ValueTask DisposeAsync()
    {
        subscribers.TryRemove(id, out _);
        if (subscribers.IsEmpty)
            subscriptions.TryRemove(channel, out _);
        return ValueTask.CompletedTask;
    }
}
