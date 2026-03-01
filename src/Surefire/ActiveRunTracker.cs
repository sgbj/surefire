using System.Collections.Concurrent;

namespace Surefire;

internal sealed class ActiveRunTracker
{
    private readonly ConcurrentDictionary<string, byte> _activeRunIds =
        new(StringComparer.Ordinal);

    public void Add(string runId) => _activeRunIds[runId] = 0;

    public void Remove(string runId) => _activeRunIds.TryRemove(runId, out _);

    public IReadOnlyCollection<string> Snapshot() => [.. _activeRunIds.Keys];
}