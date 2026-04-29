using System.Text.Json;

namespace Surefire;

/// <summary>
///     Builds JSON payloads for each store's bulk-upsert entry point. Centralizing the mapping
///     guarantees every store sees the same wire format.
///     <para>
///         Dedupes by name (last-write-wins) before emitting. Both PostgreSQL's
///         <c>INSERT ... ON CONFLICT DO UPDATE</c> and SQL Server's <c>MERGE</c> refuse to touch
///         the same row twice in one statement, so this is a correctness requirement.
///     </para>
///     <para>
///         Emits rows in name-sorted order to match the canonical lock-acquisition order used
///         by mutating paths (jobs, queues, rate_limits, runs). Any other ordering would deadlock
///         against concurrent claim/transition/create paths.
///     </para>
/// </summary>
internal static class UpsertPayloadFactory
{
    public static string SerializeJobs(IReadOnlyList<JobDefinition> jobs)
    {
        var byName = new Dictionary<string, UpsertJobPayload>(jobs.Count, StringComparer.Ordinal);
        foreach (var job in jobs)
        {
            byName[job.Name] = new()
            {
                Name = job.Name,
                Description = job.Description,
                Tags = job.Tags,
                CronExpression = job.CronExpression,
                TimeZoneId = job.TimeZoneId,
                Timeout = job.Timeout,
                MaxConcurrency = job.MaxConcurrency,
                Priority = job.Priority,
                RetryPolicy = job.RetryPolicy,
                IsContinuous = job.IsContinuous,
                Queue = job.Queue,
                RateLimitName = job.RateLimitName,
                IsEnabled = job.IsEnabled,
                MisfirePolicy = (int)job.MisfirePolicy,
                FireAllLimit = job.FireAllLimit,
                ArgumentsSchema = job.ArgumentsSchema
            };
        }

        var ordered = byName.Values
            .OrderBy(p => p.Name, StringComparer.Ordinal)
            .ToList();
        return JsonSerializer.Serialize(ordered, SurefireJsonContext.Default.ListUpsertJobPayload);
    }

    public static string SerializeQueues(IReadOnlyList<QueueDefinition> queues)
    {
        var byName = new Dictionary<string, UpsertQueuePayload>(queues.Count, StringComparer.Ordinal);
        foreach (var queue in queues)
        {
            byName[queue.Name] = new()
            {
                Name = queue.Name,
                Priority = queue.Priority,
                MaxConcurrency = queue.MaxConcurrency,
                IsPaused = queue.IsPaused,
                RateLimitName = queue.RateLimitName
            };
        }

        var ordered = byName.Values
            .OrderBy(p => p.Name, StringComparer.Ordinal)
            .ToList();
        return JsonSerializer.Serialize(ordered, SurefireJsonContext.Default.ListUpsertQueuePayload);
    }

    public static string SerializeRateLimits(IReadOnlyList<RateLimitDefinition> rateLimits)
    {
        var byName = new Dictionary<string, UpsertRateLimitPayload>(rateLimits.Count, StringComparer.Ordinal);
        foreach (var rl in rateLimits)
        {
            byName[rl.Name] = new()
            {
                Name = rl.Name,
                Type = (int)rl.Type,
                MaxPermits = rl.MaxPermits,
                Window = rl.Window
            };
        }

        var ordered = byName.Values
            .OrderBy(p => p.Name, StringComparer.Ordinal)
            .ToList();
        return JsonSerializer.Serialize(ordered, SurefireJsonContext.Default.ListUpsertRateLimitPayload);
    }
}
