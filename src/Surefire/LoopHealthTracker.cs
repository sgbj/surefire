using System.Collections.Concurrent;

namespace Surefire;

/// <summary>
///     Per-loop health snapshot consumed by <see cref="SurefireHealthCheck" />. Each background
///     loop (executor, scheduler, maintenance, retention) calls <see cref="Register" /> once at
///     <c>ExecuteAsync</c> entry to declare its expected tick cadence, then <see cref="RecordSuccess" />
///     on a successful tick and <see cref="RecordFailure" /> when an exception escapes the tick
///     body. The health check flags the host Degraded when any loop accumulates too many
///     consecutive failures or has not had a successful tick inside a budget that scales with
///     the loop's own cadence — fast loops are caught quickly, slow loops are given proportional
///     room.
///     <para>
///         Concurrency contract: each loop name has <b>exactly one writer</b>. A loop's own
///         <c>ExecuteAsync</c> is the only site that calls <see cref="Register" />,
///         <see cref="RecordSuccess" />, or <see cref="RecordFailure" /> for that loop's name,
///         and <see cref="Register" /> always runs before the first record. Under that contract
///         the record path's read-modify-write on <c>_states[loop]</c> never races another writer.
///         Readers (<see cref="Snapshot" />) are safe against writers via
///         <see cref="ConcurrentDictionary{TKey,TValue}" />'s built-in enumerator guarantees.
///     </para>
/// </summary>
internal sealed class LoopHealthTracker(TimeProvider timeProvider)
{
    private readonly ConcurrentDictionary<string, LoopState> _states = new(StringComparer.Ordinal);

    /// <summary>
    ///     Declares a loop's expected tick cadence so the health check can compute a staleness
    ///     threshold proportional to the loop's own cadence instead of a one-size-fits-all bound.
    /// </summary>
    public void Register(string loop, TimeSpan expectedCadence) =>
        _states[loop] = new(timeProvider.GetUtcNow(), null, 0, expectedCadence);

    public void RecordSuccess(string loop) =>
        _states[loop] = _states[loop] with { LastSuccessAt = timeProvider.GetUtcNow(), ConsecutiveFailures = 0 };

    public void RecordFailure(string loop)
    {
        var prev = _states[loop];
        _states[loop] = prev with { ConsecutiveFailures = prev.ConsecutiveFailures + 1 };
    }

    /// <summary>
    ///     Returns a point-in-time copy so the health check reasons about a stable view across
    ///     all loops in one evaluation pass.
    /// </summary>
    public IReadOnlyDictionary<string, LoopState> Snapshot() =>
        new Dictionary<string, LoopState>(_states, StringComparer.Ordinal);

    public sealed record LoopState(
        DateTimeOffset RegisteredAt,
        DateTimeOffset? LastSuccessAt,
        int ConsecutiveFailures,
        TimeSpan ExpectedCadence);
}