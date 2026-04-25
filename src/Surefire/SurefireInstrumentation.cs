using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Surefire;

internal sealed class SurefireInstrumentation : IDisposable
{
    private readonly Meter _meter;

    public SurefireInstrumentation(IMeterFactory meterFactory)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);

        ActivitySource = new(SurefireDiagnostics.ActivitySourceName);
        _meter = meterFactory.Create(SurefireDiagnostics.MeterName);

        RunsClaimed = _meter.CreateCounter<long>("surefire.runs.claimed",
            description: "Runs claimed by this node from the store.");
        RunsSucceeded = _meter.CreateCounter<long>("surefire.runs.completed",
            description: "Runs that reached the Succeeded terminal state.");
        RunsFailed = _meter.CreateCounter<long>("surefire.runs.failed",
            description: "Runs that reached the Failed terminal state, tagged by dead-letter reason.");
        RunsCancelled = _meter.CreateCounter<long>("surefire.runs.cancelled",
            description: "Runs that reached the Cancelled terminal state.");
        RunDurationMs = _meter.CreateHistogram<double>("surefire.runs.duration.ms",
            "ms",
            "Time from run start (claim) to terminal transition.");
        SchedulerLagMs = _meter.CreateHistogram<double>("surefire.scheduler.lag.ms",
            "ms",
            "Time between a run's NotBefore timestamp and when it was actually claimed. " +
            "A growing lag is the canonical signal that the cluster is undersized for the workload.");
        StoreOperationDurationMs = _meter.CreateHistogram<double>("surefire.store.operation.ms",
            "ms",
            "Time spent in a single IJobStore operation, tagged by operation name.");
        StoreOperationsFailed = _meter.CreateCounter<long>("surefire.store.operation.failed",
            description: "IJobStore operations that completed with a non-transient exception.");
        LogEntriesDropped = _meter.CreateCounter<long>("surefire.log_entries.dropped",
            description: "Log entries dropped before reaching the store, tagged by drop reason.");
        StoreRetries = _meter.CreateCounter<long>("surefire.store.retries",
            description: "Transient IJobStore failures the caller decided to retry.");
        LoopErrors = _meter.CreateCounter<long>("surefire.loop.errors",
            description: "Background loop tick failures, tagged by loop name.");
    }

    public ActivitySource ActivitySource { get; }

    public Counter<long> RunsClaimed { get; }

    public Counter<long> RunsSucceeded { get; }

    public Counter<long> RunsFailed { get; }

    public Counter<long> RunsCancelled { get; }

    public Histogram<double> RunDurationMs { get; }

    public Histogram<double> SchedulerLagMs { get; }

    public Histogram<double> StoreOperationDurationMs { get; }

    public Counter<long> StoreOperationsFailed { get; }

    public Counter<long> LogEntriesDropped { get; }

    public Counter<long> StoreRetries { get; }

    public Counter<long> LoopErrors { get; }

    public void Dispose()
    {
        ActivitySource.Dispose();
        _meter.Dispose();
    }

    public void RecordRunClaimed(string jobName, DateTimeOffset notBefore, DateTimeOffset claimedAt)
    {
        var tags = new TagList { { "surefire.job.name", jobName } };
        RunsClaimed.Add(1, tags);

        // Scheduler lag — claimedAt minus the run's NotBefore. ClaimRunsAsync filters on
        // NotBefore <= now, so this should always be >= 0; clamp defensively against clock
        // skew between the scheduling node and the claiming node so the histogram domain
        // stays non-negative.
        var lagMs = Math.Max(0, (claimedAt - notBefore).TotalMilliseconds);
        SchedulerLagMs.Record(lagMs, tags);
    }

    public void RecordRunCompleted(string jobName, DateTimeOffset? startedAt, DateTimeOffset completedAt)
    {
        var tags = new TagList { { "surefire.job.name", jobName } };
        RunsSucceeded.Add(1, tags);
        if (startedAt is { } started)
        {
            RunDurationMs.Record((completedAt - started).TotalMilliseconds, tags);
        }
    }

    public void RecordRunFailed(string jobName, DateTimeOffset? startedAt, DateTimeOffset completedAt,
        DeadLetterReason reason)
    {
        var tags = new TagList
        {
            { "surefire.job.name", jobName },
            { "surefire.dead_letter.reason", reason.ToTagValue() }
        };
        RunsFailed.Add(1, tags);
        if (startedAt is { } started)
        {
            // Duration histogram doesn't need the reason cardinality — keep it on a slimmer
            // tag set so chart slicing by job stays cheap.
            var durationTags = new TagList { { "surefire.job.name", jobName } };
            RunDurationMs.Record((completedAt - started).TotalMilliseconds, durationTags);
        }
    }

    public void RecordRunCancelled(string jobName, DateTimeOffset? startedAt, DateTimeOffset cancelledAt)
    {
        var tags = new TagList { { "surefire.job.name", jobName } };
        RunsCancelled.Add(1, tags);
        if (startedAt is { } started)
        {
            RunDurationMs.Record((cancelledAt - started).TotalMilliseconds, tags);
        }
    }

    public void RecordLogEntryDropped(string reason)
    {
        var tags = new TagList { { "surefire.drop.reason", reason } };
        LogEntriesDropped.Add(1, tags);
    }

    public void RecordStoreRetry(string service)
    {
        var tags = new TagList { { "surefire.service", service } };
        StoreRetries.Add(1, tags);
    }

    public void RecordStoreOperation(string operation, double elapsedMs)
    {
        var tags = new TagList { { "surefire.store.operation", operation } };
        StoreOperationDurationMs.Record(elapsedMs, tags);
    }

    public void RecordStoreOperationFailed(string operation)
    {
        var tags = new TagList { { "surefire.store.operation", operation } };
        StoreOperationsFailed.Add(1, tags);
    }

    public void RecordLoopError(string loop)
    {
        var tags = new TagList { { "surefire.loop", loop } };
        LoopErrors.Add(1, tags);
    }
}

/// <summary>
///     Reason a run reached the Failed terminal state. Surfaced as the
///     <c>surefire.dead_letter.reason</c> tag on <c>surefire.runs.failed</c> so operators can
///     distinguish exhausted-retry dead-letters from cause-specific ones (no handler, shutdown
///     interruption, stale recovery).
/// </summary>
internal enum DeadLetterReason
{
    /// <summary>Run reached its retry policy's <c>MaxRetries</c> ceiling and was dead-lettered.</summary>
    RetriesExhausted,

    /// <summary>No handler was registered for the job name on the claiming node.</summary>
    NoHandlerRegistered,

    /// <summary>Host shutdown interrupted the run mid-attempt and it was finalized to dead-letter.</summary>
    ShutdownInterrupted,

    /// <summary>Run heartbeat lapsed past <c>InactiveThreshold</c> and stale recovery dead-lettered it.</summary>
    StaleRecovery
}

internal static class DeadLetterReasonExtensions
{
    /// <summary>
    ///     Renders the reason as the snake_case string emitted on the
    ///     <c>surefire.dead_letter.reason</c> OTel tag. Snake_case matches OpenTelemetry semantic
    ///     convention recommendations for tag values; this helper avoids relying on
    ///     <see cref="object.ToString" /> (which would emit the PascalCase enum name) and keeps
    ///     the mapping explicit so reviewers see exactly which strings flow into telemetry.
    /// </summary>
    public static string ToTagValue(this DeadLetterReason reason) => reason switch
    {
        DeadLetterReason.RetriesExhausted => "retries_exhausted",
        DeadLetterReason.NoHandlerRegistered => "no_handler_registered",
        DeadLetterReason.ShutdownInterrupted => "shutdown_interrupted",
        DeadLetterReason.StaleRecovery => "stale_recovery",
        _ => "unknown"
    };
}