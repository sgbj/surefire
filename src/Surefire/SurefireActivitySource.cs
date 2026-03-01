using System.Diagnostics;

namespace Surefire;

internal static class SurefireActivitySource
{
    public static readonly ActivitySource Source = new("Surefire");

    public static Activity? StartJobExecution(string jobName, string runId, string nodeName)
    {
        var activity = Source.StartActivity("job.execute", ActivityKind.Internal);
        activity?.SetTag("surefire.job.name", jobName);
        activity?.SetTag("surefire.run.id", runId);
        activity?.SetTag("surefire.node.name", nodeName);
        return activity;
    }
}
