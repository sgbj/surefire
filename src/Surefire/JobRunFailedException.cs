namespace Surefire;

/// <summary>
///     Exception thrown when awaiting a job run that failed.
/// </summary>
/// <param name="runId">The identifier of the failed run.</param>
/// <param name="error">The error message from the failed run, or null.</param>
public sealed class JobRunFailedException(string runId, string? error)
    : Exception(error ?? $"Job run '{runId}' failed.")
{
    /// <summary>The identifier of the failed run.</summary>
    public string RunId => runId;
}