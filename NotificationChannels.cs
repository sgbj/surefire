namespace Surefire;

public static class NotificationChannels
{
    public const string RunCreated = "surefire:run:created";

    public static string RunCancel(string runId) => $"surefire:run:{runId}:cancel";
    public static string RunCompleted(string runId) => $"surefire:run:{runId}:completed";
    public static string RunEvent(string runId) => $"surefire:run:{runId}:event";
    public static string RunInput(string runId) => $"surefire:run:{runId}:input";
}
