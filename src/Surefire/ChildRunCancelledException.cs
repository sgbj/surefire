namespace Surefire;

/// <summary>
/// Thrown when a child run that this job depends on was cancelled.
/// The executor treats this as cancellation of the current run.
/// </summary>
public class ChildRunCancelledException(string runId)
    : OperationCanceledException($"Job run '{runId}' was cancelled.")
{
    public string RunId => runId;
}
