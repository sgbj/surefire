using System.Collections.Concurrent;
using System.Globalization;

namespace Surefire;

/// <summary>
///     Provides context for a job execution, including run metadata, cancellation, and progress reporting.
/// </summary>
public sealed class JobContext
{
    private static readonly AsyncLocal<JobContext?> CurrentContext = new();

    // Leading + trailing throttle. Terminal transitions await _inFlightTrailingFlush so progress
    // events never land after the terminal status.
    private static readonly TimeSpan MinProgressInterval = TimeSpan.FromMilliseconds(100);
    private readonly Lock _progressGate = new();
    private bool _hasReportedProgress;
    private Task? _inFlightTrailingFlush;
    private long _lastFlushedTicksUtc;
    private ITimer? _pendingTimer;
    private double? _pendingValue;

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
    ///     Gets the cancellation token that is triggered when the run is Canceled or the node is shutting down.
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
    ///     Gets a thread-safe key-value bag for passing data between filters in the pipeline.
    /// </summary>
    public IDictionary<string, object?> Items { get; } = new ConcurrentDictionary<string, object?>();

    internal IJobStore Store { get; init; } = null!;

    internal INotificationProvider Notifications { get; init; } = null!;

    internal BatchedEventWriter EventWriter { get; init; } = null!;

    internal TimeProvider TimeProvider { get; init; } = null!;

    internal string NodeName { get; init; } = null!;

    internal static IDisposable EnterScope(JobContext context)
    {
        var previous = CurrentContext.Value;
        CurrentContext.Value = context;
        return new Scope(previous);
    }

    /// <summary>
    ///     Reports execution progress to the store and connected clients. Successive calls within a
    ///     100 ms window are coalesced: the first is flushed immediately, and the last one in the
    ///     window is flushed at the window's end. The terminal value is always persisted.
    /// </summary>
    /// <param name="progress">A value between 0.0 and 1.0 inclusive.</param>
    /// <returns>
    ///     A task that completes when the progress has been persisted for leading-edge reports, or
    ///     immediately when the report was coalesced into a pending trailing flush.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     Thrown when <paramref name="progress" /> is less than 0.0 or greater than 1.0.
    /// </exception>
    public Task ReportProgressAsync(double progress)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(progress, 0.0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(progress, 1.0);

        lock (_progressGate)
        {
            var nowTicks = TimeProvider.GetUtcNow().UtcTicks;
            var elapsedTicks = nowTicks - _lastFlushedTicksUtc;
            var leading = !_hasReportedProgress || elapsedTicks >= MinProgressInterval.Ticks;

            if (leading)
            {
                _hasReportedProgress = true;
                _lastFlushedTicksUtc = nowTicks;
                _pendingValue = null;
                DisposePendingTimer();
                return PersistProgressAsync(progress);
            }

            _pendingValue = progress;
            if (_pendingTimer is null)
            {
                var delay = TimeSpan.FromTicks(MinProgressInterval.Ticks - elapsedTicks);
                _pendingTimer = TimeProvider.CreateTimer(FlushPendingCallback, null, delay, Timeout.InfiniteTimeSpan);
            }

            return Task.CompletedTask;
        }
    }

    /// <summary>
    ///     Flushes any pending trailing-edge progress value, awaiting any flush already started by
    ///     the timer callback. Called by the executor before transitioning the run to a terminal
    ///     status so the last coalesced value lands and never arrives after the terminal status.
    /// </summary>
    internal async Task FlushPendingProgressAsync(CancellationToken cancellationToken)
    {
        Task? inFlight;
        double? value;
        lock (_progressGate)
        {
            // Holding the lock guarantees a concurrent timer callback has either already
            // published its task to _inFlightTrailingFlush, or will find _pendingValue null
            // and return without persisting.
            inFlight = _inFlightTrailingFlush;
            value = _pendingValue;
            _pendingValue = null;
            DisposePendingTimer();
            if (value is { })
            {
                _lastFlushedTicksUtc = TimeProvider.GetUtcNow().UtcTicks;
            }
        }

        if (inFlight is { })
        {
            try
            {
                await inFlight;
            }
            catch (OperationCanceledException)
            {
            }
        }

        if (value is { } v)
        {
            await PersistProgressAsync(v, cancellationToken);
        }
    }

    private void FlushPendingCallback(object? _)
    {
        lock (_progressGate)
        {
            if (_pendingValue is not { } v)
            {
                return;
            }

            _pendingValue = null;
            _lastFlushedTicksUtc = TimeProvider.GetUtcNow().UtcTicks;
            DisposePendingTimer();
            // Publish under lock so FlushPendingProgressAsync never observes a running persist
            // with a null field.
            _inFlightTrailingFlush = PersistProgressAsync(v);
        }
    }

    private void DisposePendingTimer()
    {
        _pendingTimer?.Dispose();
        _pendingTimer = null;
    }

    private Task PersistProgressAsync(double progress) => PersistProgressAsync(progress, CancellationToken);

    private async Task PersistProgressAsync(double progress, CancellationToken cancellationToken)
    {
        var now = TimeProvider.GetUtcNow();
        await Store.UpdateRunAsync(new()
        {
            Id = RunId,
            JobName = JobName,
            NodeName = NodeName,
            Progress = progress,
            LastHeartbeatAt = now
        }, cancellationToken);

        await EventWriter.EnqueueAsync(
            new()
            {
                RunId = RunId,
                EventType = RunEventType.Progress,
                Payload = progress.ToString(CultureInfo.InvariantCulture),
                CreatedAt = now,
                Attempt = Attempt
            },
            [new(NotificationChannels.RunEvent(RunId), RunId)],
            cancellationToken);
    }

    private sealed class Scope(JobContext? previous) : IDisposable
    {
        public void Dispose() => CurrentContext.Value = previous;
    }
}
