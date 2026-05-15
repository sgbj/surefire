using System.Text.Json.Serialization;

namespace Surefire;

/// <summary>
///     Source-generated JSON context for Surefire's internal payloads.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    Converters = [typeof(TimeSpanTicksConverter), typeof(DateTimeOffsetUnixMsConverter)])]
[JsonSerializable(typeof(LogEventPayload))]
[JsonSerializable(typeof(RunFailureEnvelope))]
[JsonSerializable(typeof(InputDeclarationEnvelope))]
[JsonSerializable(typeof(InputEnvelope))]
[JsonSerializable(typeof(string[]))]
[JsonSerializable(typeof(RetryPolicy))]
[JsonSerializable(typeof(JobBatch))]
[JsonSerializable(typeof(JobRun))]
[JsonSerializable(typeof(List<JobRun>))]
[JsonSerializable(typeof(RunEvent))]
[JsonSerializable(typeof(List<RunEvent>))]
[JsonSerializable(typeof(List<UpsertJobPayload>))]
[JsonSerializable(typeof(List<UpsertQueuePayload>))]
[JsonSerializable(typeof(List<UpsertRateLimitPayload>))]
[JsonSerializable(typeof(BatchCompletionPayload))]
[JsonSerializable(typeof(CancelExpiredRunsPayload))]
[JsonSerializable(typeof(SubtreeCancellationPayload))]
internal sealed partial class SurefireJsonContext : JsonSerializerContext;

/// <summary>
///     Bulk upsert payload for jobs.
/// </summary>
internal sealed record UpsertJobPayload
{
    public string Name { get; init; } = string.Empty;
    public string? Description { get; init; }
    public string[] Tags { get; init; } = [];
    public string? CronExpression { get; init; }
    public string? TimeZoneId { get; init; }
    public TimeSpan? Timeout { get; init; }
    public int? MaxConcurrency { get; init; }
    public int Priority { get; init; }
    public RetryPolicy RetryPolicy { get; init; } = new();
    public bool IsContinuous { get; init; }
    public string? Queue { get; init; }
    public string? RateLimitName { get; init; }
    public bool IsEnabled { get; init; }
    public int MisfirePolicy { get; init; }
    public int? FireAllLimit { get; init; }
    public string? ArgumentsSchema { get; init; }
}

/// <summary>Bulk upsert payload for queues.</summary>
internal sealed record UpsertQueuePayload
{
    public string Name { get; init; } = string.Empty;
    public int Priority { get; init; }
    public int? MaxConcurrency { get; init; }
    public bool IsPaused { get; init; }
    public string? RateLimitName { get; init; }
}

/// <summary>Bulk upsert payload for rate limits.</summary>
internal sealed record UpsertRateLimitPayload
{
    public string Name { get; init; } = string.Empty;
    public int Type { get; init; }
    public int MaxPermits { get; init; }
    public TimeSpan Window { get; init; }
}

/// <summary>
///     Batch-completion notification payload.
/// </summary>
internal sealed record BatchCompletionPayload
{
    public string BatchId { get; init; } = string.Empty;
    public int BatchStatus { get; init; }
    public DateTimeOffset CompletedAt { get; init; }
}

/// <summary>
///     Paged result payload for canceling expired runs.
/// </summary>
internal sealed record CancelExpiredRunsPayload
{
    public CanceledRunPayload[]? Runs { get; init; }
    public BatchCompletionPayload[]? CompletedBatches { get; init; }
    public int Cleaned { get; init; }
    public int Skipped { get; init; }
}

/// <summary>
///     Result payload for canceling a run subtree.
/// </summary>
internal sealed record SubtreeCancellationPayload
{
    public bool Found { get; init; } = true;
    public CanceledRunPayload[]? Runs { get; init; }
    public BatchCompletionPayload[]? CompletedBatches { get; init; }
}

/// <summary>Per-run entry inside <see cref="SubtreeCancellationPayload" />.</summary>
internal sealed record CanceledRunPayload
{
    public string RunId { get; init; } = string.Empty;
    public string? BatchId { get; init; }
}
