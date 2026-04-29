using System.Text.Json.Serialization;

namespace Surefire;

/// <summary>
///     Source-generated JSON context for every internal wire payload (Redis run/event/batch
///     shapes, bulk-upsert payloads, AOT-safe tag and name lists, failure/input envelopes).
///     <para>
///         Separate from <see cref="SurefireOptions.SerializerOptions" />, which is user-facing
///         and runtime-configurable. This context is Surefire's internal protocol and must stay
///         stable regardless of caller configuration.
///     </para>
///     <para>
///         <see cref="System.TimeSpan" /> serializes as ticks; <see cref="System.DateTimeOffset" />
///         as unix milliseconds, so Lua scripts and SQL JSON paths can compare them numerically.
///     </para>
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
internal sealed partial class SurefireJsonContext : JsonSerializerContext;

/// <summary>
///     Bulk <c>UpsertJobsAsync</c> wire record. A list of these is serialized to a single JSON
///     string parameter and consumed server-side by each store's native JSON primitive
///     (PG <c>jsonb_array_elements</c>, SQL Server <c>OPENJSON WITH</c>, SQLite
///     <c>json_each</c>, Redis <c>cjson.decode</c>).
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

/// <summary>Bulk <c>UpsertQueuesAsync</c> wire record. See <see cref="UpsertJobPayload" />.</summary>
internal sealed record UpsertQueuePayload
{
    public string Name { get; init; } = string.Empty;
    public int Priority { get; init; }
    public int? MaxConcurrency { get; init; }
    public bool IsPaused { get; init; }
    public string? RateLimitName { get; init; }
}

/// <summary>Bulk <c>UpsertRateLimitsAsync</c> wire record. See <see cref="UpsertJobPayload" />.</summary>
internal sealed record UpsertRateLimitPayload
{
    public string Name { get; init; } = string.Empty;
    public int Type { get; init; }
    public int MaxPermits { get; init; }
    public TimeSpan Window { get; init; }
}

/// <summary>
///     Wire record for the batch-completion notification returned from Redis Lua scripts and
///     surfaced to <c>BatchCompletionHandler</c>. Emitted when a terminal transition closes out
///     the last pending run of a batch.
/// </summary>
internal sealed record BatchCompletionPayload
{
    public string BatchId { get; init; } = string.Empty;
    public int BatchStatus { get; init; }
    public DateTimeOffset CompletedAt { get; init; }
}

/// <summary>
///     Wire record for the paged result of Redis's cancel-expired-runs Lua script: the ids it
///     cancelled this page, the count of orphan pending entries it cleaned up, and the count of
///     entries skipped because they'd already been handled by another node.
/// </summary>
internal sealed record CancelExpiredRunsPayload
{
    public string[] Cancelled { get; init; } = [];
    public int Cleaned { get; init; }
    public int Skipped { get; init; }
}
