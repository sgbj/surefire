using System.Globalization;

namespace Surefire;

/// <summary>
///     Provides context for a job execution, including run metadata, cancellation, and progress reporting.
/// </summary>
public sealed class JobContext
{
    private static readonly AsyncLocal<JobContext?> CurrentContext = new();

    internal static JobContext? Current => CurrentContext.Value;

    /// <summary>
    ///     Gets the unique identifier of the current run.
    /// </summary>
    public required string RunId { get; init; }

    /// <summary>
    ///     Gets the run ID of the root ancestor in the current execution hierarchy.
    /// </summary>
    public required string RootRunId { get; init; }

    /// <summary>
    ///     Gets the name of the job being executed.
    /// </summary>
    public required string JobName { get; init; }

    /// <summary>
    ///     Gets the cancellation token that is triggered when the run is cancelled or the node is shutting down.
    /// </summary>
    public required CancellationToken CancellationToken { get; init; }

    /// <summary>
    ///     Gets the current attempt number for this run.
    /// </summary>
    public int Attempt { get; init; }

    /// <summary>
    ///     Gets the batch ID if this run is part of a batch.
    /// </summary>
    public string? BatchId { get; init; }

    /// <summary>
    ///     Gets or sets the result produced by the job handler. Populated before lifecycle callbacks.
    /// </summary>
    public object? Result { get; internal set; }

    /// <summary>
    ///     Gets or sets the exception thrown by the job handler. Populated before lifecycle callbacks.
    /// </summary>
    public Exception? Exception { get; internal set; }

    /// <summary>
    ///     Gets a key-value bag for passing data between filters in the pipeline.
    /// </summary>
    public IDictionary<string, object?> Items { get; } = new Dictionary<string, object?>();

    internal IJobStore Store { get; init; } = null!;

    internal INotificationProvider Notifications { get; init; } = null!;

    internal TimeProvider TimeProvider { get; init; } = null!;

    internal string NodeName { get; init; } = null!;

    internal static IDisposable EnterScope(JobContext context)
    {
        var previous = CurrentContext.Value;
        CurrentContext.Value = context;
        return new Scope(previous);
    }

    /// <summary>
    ///     Reports execution progress to the store and connected clients.
    /// </summary>
    /// <param name="progress">A value between 0.0 and 1.0 inclusive.</param>
    /// <returns>A task that completes when the progress has been persisted.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     Thrown when <paramref name="progress" /> is less than 0.0 or greater than 1.0.
    /// </exception>
    public Task ReportProgressAsync(double progress)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(progress, 0.0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(progress, 1.0);
        return PersistProgressAsync(progress);
    }

    private async Task PersistProgressAsync(double progress)
    {
        var now = TimeProvider.GetUtcNow();
        await Store.UpdateRunAsync(new()
        {
            Id = RunId,
            JobName = JobName,
            NodeName = NodeName,
            Progress = progress,
            LastHeartbeatAt = now
        }, CancellationToken);

        await Store.AppendEventsAsync([
            new()
            {
                RunId = RunId,
                EventType = RunEventType.Progress,
                Payload = progress.ToString(CultureInfo.InvariantCulture),
                CreatedAt = now,
                Attempt = Attempt
            }
        ], CancellationToken);

        await Notifications.PublishAsync(NotificationChannels.RunEvent(RunId), RunId, CancellationToken);
    }

    private sealed class Scope(JobContext? previous) : IDisposable
    {
        public void Dispose() => CurrentContext.Value = previous;
    }
}
