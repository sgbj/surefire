namespace Surefire;

public static class NotificationChannels
{
    public const string RunCreated = "surefire:run:created";

    public static string RunCancelled(string runId) => $"surefire:run:{runId}:cancelled";
    public static string RunCompleted(string runId) => $"surefire:run:{runId}:completed";
    public static string RunLog(string runId) => $"surefire:run:{runId}:log";
    public static string RunProgress(string runId) => $"surefire:run:{runId}:progress";
}
