namespace Surefire;

public interface INotificationProvider
{
    Task PublishAsync(string channel, string message, CancellationToken cancellationToken = default);
    Task<IAsyncDisposable> SubscribeAsync(string channel, Func<string, Task> handler, CancellationToken cancellationToken = default);
}
