namespace Surefire;

/// <summary>
///     Carries the orchestrator run id and step index that a child creation should atomically
///     record on the orchestrator row. Drives the executor's replay boundary on re-claim.
/// </summary>
/// <param name="OrchestratorRunId">Run id of the durable orchestrator whose step counter advanced.</param>
/// <param name="Step">Monotonic step index produced by <c>JobContext.AllocateStep()</c>.</param>
public readonly record struct DurableStepRecord(string OrchestratorRunId, int Step);
