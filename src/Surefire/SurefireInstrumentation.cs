using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Surefire;

internal sealed class SurefireInstrumentation : IDisposable
{
    private readonly Meter _meter;

    public SurefireInstrumentation(IMeterFactory? meterFactory = null)
    {
        ActivitySource = new("Surefire");
        _meter = meterFactory?.Create("Surefire") ?? new Meter("Surefire");

        RunsClaimed = _meter.CreateCounter<long>("surefire.runs.claimed");
        RunsCompleted = _meter.CreateCounter<long>("surefire.runs.completed");
        RunsFailed = _meter.CreateCounter<long>("surefire.runs.failed");
        RunsCancelled = _meter.CreateCounter<long>("surefire.runs.cancelled");
        RunDurationMs = _meter.CreateHistogram<double>("surefire.runs.duration.ms");
    }

    public ActivitySource ActivitySource { get; }

    public Counter<long> RunsClaimed { get; }

    public Counter<long> RunsCompleted { get; }

    public Counter<long> RunsFailed { get; }

    public Counter<long> RunsCancelled { get; }

    public Histogram<double> RunDurationMs { get; }

    public void Dispose()
    {
        ActivitySource.Dispose();
        _meter.Dispose();
    }

    public void RecordRunClaimed() => RunsClaimed.Add(1);

    public void RecordRunCompleted(DateTimeOffset? startedAt, DateTimeOffset completedAt)
    {
        RunsCompleted.Add(1);
        if (startedAt is { } started)
        {
            RunDurationMs.Record((completedAt - started).TotalMilliseconds);
        }
    }

    public void RecordRunFailed(DateTimeOffset? startedAt, DateTimeOffset completedAt)
    {
        RunsFailed.Add(1);
        if (startedAt is { } started)
        {
            RunDurationMs.Record((completedAt - started).TotalMilliseconds);
        }
    }

    public void RecordRunCancelled(DateTimeOffset? startedAt, DateTimeOffset cancelledAt)
    {
        RunsCancelled.Add(1);
        if (startedAt is { } started)
        {
            RunDurationMs.Record((cancelledAt - started).TotalMilliseconds);
        }
    }
}