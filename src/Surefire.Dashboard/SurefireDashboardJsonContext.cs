using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Mvc;

namespace Surefire.Dashboard;

/// <summary>
///     Source-generated JSON context for every request and response shape served by
///     <c>MapSurefireDashboard</c>. Auto-registered into the consuming app's HTTP JSON options
///     by <c>AddSurefireDashboard</c>, so users never need to declare these types in their own
///     <see cref="JsonSerializerContext" />.
/// </summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(DashboardStatsResponse))]
[JsonSerializable(typeof(RunResponse))]
[JsonSerializable(typeof(IReadOnlyList<RunResponse>))]
[JsonSerializable(typeof(List<RunResponse>))]
[JsonSerializable(typeof(RunTreeResponse))]
[JsonSerializable(typeof(PagedResponse<RunResponse>))]
[JsonSerializable(typeof(JobResponse))]
[JsonSerializable(typeof(IReadOnlyList<JobResponse>))]
[JsonSerializable(typeof(List<JobResponse>))]
[JsonSerializable(typeof(JobStatsResponse))]
[JsonSerializable(typeof(TimelineBucketResponse))]
[JsonSerializable(typeof(NodeResponse))]
[JsonSerializable(typeof(IReadOnlyList<NodeResponse>))]
[JsonSerializable(typeof(List<NodeResponse>))]
[JsonSerializable(typeof(QueueResponse))]
[JsonSerializable(typeof(IReadOnlyList<QueueResponse>))]
[JsonSerializable(typeof(List<QueueResponse>))]
[JsonSerializable(typeof(TriggerJobRequest))]
[JsonSerializable(typeof(UpdateJobRequest))]
[JsonSerializable(typeof(UpdateQueueRequest))]
[JsonSerializable(typeof(RunLookupRequest))]
[JsonSerializable(typeof(LogPageResponse))]
[JsonSerializable(typeof(RunIdResponse))]
[JsonSerializable(typeof(ProblemDetails))]
internal sealed partial class SurefireDashboardJsonContext : JsonSerializerContext;
