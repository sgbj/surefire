namespace Surefire;

public class JobRunFailedException(string runId, string? retryRunId, string? error)
    : Exception(error ?? $"Job run '{runId}' failed.")
{
    public string RunId => runId;
    public string? RetryRunId => retryRunId;
}
