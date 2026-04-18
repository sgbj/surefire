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

        RunsClaimed = _meter.CreateCounter<long>("surefire.runs.claimed");
        RunsSucceeded = _meter.CreateCounter<long>("surefire.runs.completed");
        RunsFailed = _meter.CreateCounter<long>("surefire.runs.failed");
        RunsCancelled = _meter.CreateCounter<long>("surefire.runs.cancelled");
        RunDurationMs = _meter.CreateHistogram<double>("surefire.runs.duration.ms");
        StoreOperationDurationMs = _meter.CreateHistogram<double>("surefire.store.operation.ms");
        StoreOperationsFailed = _meter.CreateCounter<long>("surefire.store.operation.failed");
        LogEntriesDropped = _meter.CreateCounter<long>("surefire.log_entries.dropped");
        StoreRetries = _meter.CreateCounter<long>("surefire.store.retries");
    }

    public ActivitySource ActivitySource { get; }

    public Counter<long> RunsClaimed { get; }

    public Counter<long> RunsSucceeded { get; }

    public Counter<long> RunsFailed { get; }

    public Counter<long> RunsCancelled { get; }

    public Histogram<double> RunDurationMs { get; }

    public Histogram<double> StoreOperationDurationMs { get; }

    public Counter<long> StoreOperationsFailed { get; }

    public Counter<long> LogEntriesDropped { get; }

    public Counter<long> StoreRetries { get; }

    public void Dispose()
    {
        ActivitySource.Dispose();
        _meter.Dispose();
    }

    public void RecordRunClaimed(string jobName)
    {
        var tags = new TagList { { "surefire.job.name", jobName } };
        RunsClaimed.Add(1, tags);
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

    public void RecordRunFailed(string jobName, DateTimeOffset? startedAt, DateTimeOffset completedAt)
    {
        var tags = new TagList { { "surefire.job.name", jobName } };
        RunsFailed.Add(1, tags);
        if (startedAt is { } started)
        {
            RunDurationMs.Record((completedAt - started).TotalMilliseconds, tags);
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
}