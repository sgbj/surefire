using System.Collections.Concurrent;

namespace Surefire;

internal sealed class ActiveRunTracker
{
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _activeRuns =
        new(StringComparer.Ordinal);

    public void Add(string runId, CancellationTokenSource cts) => _activeRuns[runId] = cts;

    public void Remove(string runId) => _activeRuns.TryRemove(runId, out _);

    /// <summary>
    ///     Removes the run only if it is still registered to <paramref name="cts" />. A durable
    ///     suspend->resume cycle re-claims the same run id on a fresh execution task; this
    ///     compare-and-remove keeps the prior task's teardown from evicting the successor's
    ///     registration (and thus its cooperative-cancellation wiring).
    /// </summary>
    public void Remove(string runId, CancellationTokenSource cts) =>
        ((ICollection<KeyValuePair<string, CancellationTokenSource>>)_activeRuns)
            .Remove(new(runId, cts));

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
