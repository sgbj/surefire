namespace Surefire;

/// <summary>
///     Thrown when durable orchestrator code no longer matches the persisted replay history.
/// </summary>
public sealed class DurableReplayMismatchException : Exception
{
    /// <summary>Initializes a new instance scoped to a durable replay step.</summary>
    /// <param name="orchestratorRunId">The durable orchestrator run being replayed.</param>
    /// <param name="step">The durable step where the mismatch was detected.</param>
    /// <param name="message">A description of the mismatch.</param>
    public DurableReplayMismatchException(string orchestratorRunId, int step, string message)
        : base($"Durable replay mismatch for run '{orchestratorRunId}' at step {step}: {message}")
    {
        OrchestratorRunId = orchestratorRunId;
        Step = step;
    }

    /// <summary>The durable orchestrator run being replayed.</summary>
    public string OrchestratorRunId { get; }

    /// <summary>The durable step where the mismatch was detected.</summary>
    public int Step { get; }
}
