using System.Diagnostics.Metrics;

namespace Surefire;

internal static class SurefireMetrics
{
    private static readonly Meter Meter = new("Surefire");

    public static readonly Counter<long> JobsExecuted = Meter.CreateCounter<long>("surefire.jobs.executed");
    public static readonly Counter<long> JobsFailed = Meter.CreateCounter<long>("surefire.jobs.failed");
    public static readonly Counter<long> JobsRetried = Meter.CreateCounter<long>("surefire.jobs.retried");
    public static readonly Counter<long> JobsDeadLettered = Meter.CreateCounter<long>("surefire.jobs.dead_lettered");
    public static readonly Counter<long> JobsCancelled = Meter.CreateCounter<long>("surefire.jobs.cancelled");
    public static readonly Histogram<double> JobDuration = Meter.CreateHistogram<double>("surefire.jobs.duration", "ms");
    public static readonly UpDownCounter<long> ActiveRuns = Meter.CreateUpDownCounter<long>("surefire.runs.active");
}
