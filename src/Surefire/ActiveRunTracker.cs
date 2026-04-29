using System.Collections.Concurrent;

namespace Surefire;

internal sealed class ActiveRunTracker
{
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _activeRuns =
        new(StringComparer.Ordinal);

    public void Add(string runId, CancellationTokenSource cts) => _activeRuns[runId] = cts;

    public void Remove(string runId) => _activeRuns.TryRemove(runId, out _);

    public IReadOnlyCollection<string> Snapshot() => [.. _activeRuns.Keys];

    /// <summary>
    ///     Signals cancellation for the given run if it is currently active on this node.
    ///     No-op if the run is not tracked or its cancellation source has already been disposed.
    /// </summary>
    public void TryRequestCancel(string runId)
    {
        if (!_activeRuns.TryGetValue(runId, out var cts))
        {
            return;
        }

        try
        {
            cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
    }
}
