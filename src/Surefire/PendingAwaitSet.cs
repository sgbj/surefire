using System.Collections.Concurrent;

namespace Surefire;

/// <summary>
///     Thread-safe collector for run / batch ids a durable orchestrator is waiting on.
/// </summary>
internal sealed class PendingAwaitSet
{
    private readonly ConcurrentDictionary<string, byte> _batches = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, byte> _runs = new(StringComparer.Ordinal);

    public bool IsEmpty => _runs.IsEmpty && _batches.IsEmpty;

    public void AddRun(string runId) => _runs.TryAdd(runId, 0);

    public void AddBatch(string batchId) => _batches.TryAdd(batchId, 0);

    public (IReadOnlyCollection<string> AwaitedRunIds, IReadOnlyCollection<string> AwaitedBatchIds) Snapshot()
    {
        var runs = _runs.Count == 0 ? (IReadOnlyCollection<string>)Array.Empty<string>() : _runs.Keys.ToArray();
        var batches = _batches.Count == 0
            ? (IReadOnlyCollection<string>)Array.Empty<string>()
            : _batches.Keys.ToArray();
        return (runs, batches);
    }
}
