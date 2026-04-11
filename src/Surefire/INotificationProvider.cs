using System.ComponentModel;

namespace Surefire;

/// <summary>
///     Provides pub/sub notifications for inter-node communication. Used to wake claim loops
///     when runs are created and to deliver run events to waiting callers.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface INotificationProvider
{
    /// <summary>
    ///     Establishes the connection required for pub/sub operations. Called once at startup
    ///     by <c>SurefireInitializationService</c> after <see cref="IJobStore.MigrateAsync" />.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Publishes a notification to the specified channel. All active subscribers on any
    ///     node receive the message.
    /// </summary>
    /// <param name="channel">The channel name to publish to.</param>
    /// <param name="message">An optional message payload. Null for signal-only notifications.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task PublishAsync(string channel, string? message = null, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Subscribes to notifications on the specified channel. The handler is invoked for each
    ///     message received. Disposing the returned <see cref="IAsyncDisposable" /> unsubscribes.
    /// </summary>
    /// <param name="channel">The channel name to subscribe to.</param>
    /// <param name="handler">
    ///     The callback invoked for each received message. The parameter is the message payload,
    ///     which may be null for signal-only notifications.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the subscription setup.</param>
    /// <returns>A disposable that unsubscribes from the channel when disposed.</returns>
    Task<IAsyncDisposable> SubscribeAsync(string channel, Func<string?, Task> handler,
        CancellationToken cancellationToken = default);
}
