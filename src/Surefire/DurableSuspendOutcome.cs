namespace Surefire;

/// <summary>
///     Outcome of <see cref="IJobStore.TrySuspendRunAsync" />.
/// </summary>
public enum DurableSuspendOutcome
{
    /// <summary>
    ///     The CAS on (<c>status = Running</c>, <c>lease_epoch = expectedLeaseEpoch</c>) did not match,
    ///     so the orchestrator was not moved out of Running (typically because a concurrent cancel
    ///     reached the store first).
    /// </summary>
    NotTransitioned,

    /// <summary>
    ///     The orchestrator was parked in <see cref="JobStatus.Suspended" /> because at least one
    ///     awaited entity was still non-terminal at suspend time.
    /// </summary>
    Suspended,

    /// <summary>
    ///     Every awaited entity was already terminal at suspend time, so the orchestrator was
    ///     transitioned directly back to <see cref="JobStatus.Pending" /> with <c>NotBefore = now</c>
    ///     and will be re-claimed on the next sweep.
    /// </summary>
    ImmediatePending
}
