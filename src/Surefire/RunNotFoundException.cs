namespace Surefire;

/// <summary>
///     Thrown when a requested run no longer exists.
/// </summary>
public sealed class RunNotFoundException(string runId)
    : InvalidOperationException($"Run '{runId}' was not found.")
{
    /// <summary>
    ///     The missing run identifier.
    /// </summary>
    public string RunId { get; } = runId;
}
