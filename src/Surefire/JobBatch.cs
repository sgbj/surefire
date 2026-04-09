namespace Surefire;

/// <summary>Represents a batch of job runs submitted together.</summary>
public sealed class JobBatch
{
    private readonly IJobClientInternal? _client;

    internal JobBatch() { }
    internal JobBatch(IJobClientInternal client) => _client = client;

    /// <summary>Gets the batch coordinator run ID.</summary>
    public required string Id { get; init; }

    /// <summary>Gets the job name for the batch coordinator run.</summary>
    public required string JobName { get; init; }

    /// <summary>Gets the current status of the batch coordinator run.</summary>
    public JobStatus Status { get; init; }

    /// <summary>Gets the total number of runs in the batch.</summary>
    public int Total { get; init; }

    /// <summary>Gets the number of succeeded runs so far.</summary>
    public int Succeeded { get; init; }

    /// <summary>Gets the number of failed runs so far.</summary>
    public int Failed { get; init; }

    /// <summary>Gets when the batch was created.</summary>
    public DateTimeOffset CreatedAt { get; init; }

    internal static JobBatch FromRun(JobRun run) => new()
    {
        Id = run.Id, JobName = run.JobName, Status = run.Status,
        Total = run.BatchTotal ?? 0, Succeeded = run.BatchCompleted ?? 0, Failed = run.BatchFailed ?? 0,
        CreatedAt = run.CreatedAt
    };

    /// <summary>Gets whether the batch has reached a terminal status.</summary>
    public bool IsTerminal => Status.IsTerminal;

    /// <summary>Gets whether all runs in the batch completed successfully.</summary>
    public bool IsSuccess => Status == JobStatus.Succeeded;
}
