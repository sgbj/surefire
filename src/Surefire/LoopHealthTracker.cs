using System.Collections.Concurrent;

namespace Surefire;

/// <summary>
///     Per-loop health snapshot consumed by <see cref="SurefireHealthCheck" />. Each background
///     loop calls <see cref="Register" /> once at <c>ExecuteAsync</c> entry, then
///     <see cref="RecordSuccess" /> per successful tick and <see cref="RecordFailure" /> when an
///     exception escapes. The health check uses these to flag stalled or wedged loops.
///     <para>
///         Concurrency contract: each loop name has exactly one writer (its own
///         <c>ExecuteAsync</c>), so the read-modify-write on <c>_states[loop]</c> never races.
///         Readers are safe via <see cref="ConcurrentDictionary{TKey,TValue}" />'s enumerator
///         guarantees.
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
