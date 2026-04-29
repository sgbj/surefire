namespace Surefire;

/// <summary>
///     The result of a <see cref="IJobStore.TryTransitionRunAsync" /> call. Indicates whether the
///     transition was applied and, for batch children reaching a terminal state, whether the
///     batch itself completed as a result.
/// </summary>
public readonly record struct RunTransitionResult(bool Transitioned, BatchCompletionInfo? BatchCompletion = null)
{
    /// <summary>A transition that was not applied (CAS mismatch or validation failure).</summary>
    public static readonly RunTransitionResult NotApplied = new(false);

    /// <summary>A transition that was applied with no batch side-effects.</summary>
    public static readonly RunTransitionResult Applied = new(true);
}

/// <summary>
///     Returned when a batch child's terminal transition caused the batch to complete.
/// </summary>
/// <param name="BatchId">The ID of the completed batch.</param>
/// <param name="BatchStatus">The terminal status of the batch (Succeeded, Failed, or Cancelled).</param>
/// <param name="CompletedAt">The timestamp recorded on the batch.</param>
public sealed record BatchCompletionInfo(string BatchId, JobStatus BatchStatus, DateTimeOffset CompletedAt);
