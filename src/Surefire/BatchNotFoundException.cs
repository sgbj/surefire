namespace Surefire;

/// <summary>
///     Thrown when a requested batch no longer exists.
/// </summary>
public sealed class BatchNotFoundException(string batchId)
    : InvalidOperationException($"Batch '{batchId}' was not found.")
{
    /// <summary>
    ///     The missing batch identifier.
    /// </summary>
    public string BatchId { get; } = batchId;
}
