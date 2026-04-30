using System.Text.Json.Serialization;

namespace Surefire;

/// <summary>Immutable snapshot of a batch of job runs submitted together.</summary>
public sealed record JobBatch
{
    /// <summary>Gets the unique identifier of the batch.</summary>
    public required string Id { get; init; }

    /// <summary>Gets when the batch was created.</summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>Gets the total number of runs in the batch.</summary>
    public int Total { get; init; }

    /// <summary>Gets the current status of the batch.</summary>
    public JobStatus Status { get; init; }

    /// <summary>Gets the number of succeeded runs so far.</summary>
    public int Succeeded { get; init; }

    /// <summary>Gets the number of failed runs so far.</summary>
    public int Failed { get; init; }

    /// <summary>Gets the number of canceled runs so far.</summary>
    public int Canceled { get; init; }

    /// <summary>Gets when the batch completed, failed, or was canceled. Null while still in progress.</summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>Gets whether the batch has reached a terminal status.</summary>
    [JsonIgnore]
    public bool IsTerminal => Status.IsTerminal;

    /// <summary>Gets whether all runs in the batch completed successfully.</summary>
    [JsonIgnore]
    public bool IsSuccess => Status == JobStatus.Succeeded;
}
