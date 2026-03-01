namespace Surefire;

/// <summary>
///     Thrown when a run operation is rejected due to current run state or constraints.
/// </summary>
public sealed class RunConflictException(string message)
    : InvalidOperationException(message);