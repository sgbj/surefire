namespace Surefire;

/// <summary>
///     Persisted value recorded by a durable orchestrator step so handler replay can return the
///     same value without re-running non-deterministic code.
/// </summary>
/// <param name="OrchestratorRunId">The durable orchestrator run that owns the record.</param>
/// <param name="Step">The monotonic durable step index allocated by the orchestrator.</param>
/// <param name="Kind">The recorded operation kind, such as <c>record</c> or <c>utc-now</c>.</param>
/// <param name="Name">
///     Optional diagnostic identity. Generic records use the name supplied to
///     <see cref="JobContext.RecordAsync{T}" />; built-in helpers leave this empty.
/// </param>
/// <param name="Payload">JSON payload text. JSON <c>null</c> is stored as the literal string <c>null</c>.</param>
/// <param name="CreatedAt">The time the value was recorded.</param>
public sealed record DurableRecord(
    string OrchestratorRunId,
    int Step,
    string Kind,
    string? Name,
    string Payload,
    DateTimeOffset CreatedAt);
