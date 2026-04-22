using System.Text.Json;

namespace Surefire;

/// <summary>
///     Builds the JSON payload strings passed to each store's bulk-upsert entry point.
///     Centralizing the <see cref="JobDefinition" /> / <see cref="QueueDefinition" /> /
///     <see cref="RateLimitDefinition" /> → payload mapping keeps the per-store code free of
///     serialization boilerplate and guarantees every store sees the same wire format for a
///     given definition.
///     <para>
///         Deduplicates by name at the payload boundary (last-write-wins). Every store's
///         bulk-upsert statement expects unique keys per input row — PostgreSQL's
///         <c>INSERT ... ON CONFLICT DO UPDATE</c> and SQL Server's <c>MERGE</c> both refuse
///         to touch the same row twice in a single statement, so deduping here is a correctness
///         requirement, not a pre-optimization.
///     </para>
///     <para>
///         Emits rows in name-sorted order. Every mutating path obeys a canonical
///         name-sorted lock-acquisition order for config rows (jobs → queues → rate_limits → runs);
///         a bulk upsert that serialized rows in registration/insertion order would acquire row
///         locks in a different order than concurrent claim/transition/create paths and deadlock
///         against them. Sorting here makes the upsert participate in the same invariant.
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