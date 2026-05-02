namespace Surefire;

/// <summary>
///     A run that transitioned to <see cref="JobStatus.Canceled" /> as part of a subtree cancellation.
/// </summary>
/// <param name="RunId">The cancelled run's ID.</param>
/// <param name="BatchId">The batch ID, if the run belongs to a batch; <c>null</c> otherwise.</param>
public readonly record struct CanceledRun(string RunId, string? BatchId);

/// <summary>
///     Result of a subtree cancellation: every run that transitioned to <see cref="JobStatus.Canceled" />
///     in the same atomic operation, plus any batches that completed as a side effect because
///     their remaining non-terminal runs were cancelled.
///     <para>
///         Returned by <see cref="IJobStore.CancelRunSubtreeAsync" /> and
///         <see cref="IJobStore.CancelBatchSubtreeAsync" /> with all bookkeeping (events, job/queue/batch
///         counters, batch completion) already persisted. <see cref="Found" /> indicates whether the
///         root run or batch existed in the same transaction or script as the cancel walk.
///     </para>
/// </summary>
/// <param name="Runs">The runs that transitioned to <see cref="JobStatus.Canceled" />.</param>
/// <param name="CompletedBatches">Batches that reached a terminal status as a side effect.</param>
public sealed record SubtreeCancellation(
    IReadOnlyList<CanceledRun> Runs,
    IReadOnlyList<BatchCompletionInfo> CompletedBatches)
{
    /// <summary>
    ///     Whether the root run or batch existed at the time of the operation. <c>false</c> only when
    ///     the addressed entity was not found in the store.
    /// </summary>
    public bool Found { get; init; } = true;

    /// <summary>The root entity existed but had no runs to cancel (already terminal, no children, etc.).</summary>
    public static readonly SubtreeCancellation Empty = new([], []);

    /// <summary>The root entity does not exist in the store.</summary>
    public static readonly SubtreeCancellation NotFound = new([], []) { Found = false };
}
