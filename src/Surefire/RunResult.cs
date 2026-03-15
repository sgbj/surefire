using System.Text.Json;

namespace Surefire;

public sealed class RunResult
{
    public required string RunId { get; init; }
    public required string JobName { get; init; }
    public required JobStatus Status { get; init; }
    public string? Error { get; init; }
    public string? RetryRunId { get; init; }
    internal string? ResultJson { get; init; }
    internal JsonSerializerOptions? SerializerOptions { get; init; }

    public bool IsSuccess => Status == JobStatus.Completed;
    public bool IsFailed => Status is JobStatus.Failed or JobStatus.DeadLetter;
    public bool IsCancelled => Status == JobStatus.Cancelled;

    public T? GetResult<T>(JsonSerializerOptions? options = null) =>
        ResultJson is not null
            ? JsonSerializer.Deserialize<T>(ResultJson, options ?? SerializerOptions ?? JsonSerializerOptions.Default)
            : default;
}
