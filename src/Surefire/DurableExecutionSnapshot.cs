namespace Surefire;

/// <summary>
///     Snapshot of an orchestrator's recorded children and replay counters, captured at claim
///     time and served from memory during replay.
/// </summary>
/// <param name="Children">
///     Direct children of the orchestrator (those whose <c>ParentRunId</c> equals the orchestrator
///     id), keyed by run id.
/// </param>
/// <param name="ChildBatches">
///     Direct child batches of the orchestrator (those whose <c>ParentRunId</c> equals the
///     orchestrator id), keyed by batch id.
/// </param>
/// <param name="Records">Recorded values keyed by durable step.</param>
/// <param name="HighestRecordedStep">Snapshot of the orchestrator's highest_recorded_step at claim time.</param>
public sealed record DurableExecutionSnapshot(
    IReadOnlyDictionary<string, JobRun> Children,
    IReadOnlyDictionary<string, JobBatch> ChildBatches,
    IReadOnlyDictionary<int, DurableRecord> Records,
    int HighestRecordedStep);
