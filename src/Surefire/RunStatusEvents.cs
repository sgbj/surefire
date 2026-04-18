namespace Surefire;

internal static class RunStatusEvents
{
    public static RunEvent Create(string runId, int attempt, JobStatus status, DateTimeOffset createdAt) => new()
    {
        RunId = runId,
        EventType = RunEventType.Status,
        Payload = ((int)status).ToString(),
        CreatedAt = createdAt,
        Attempt = attempt
    };

    public static bool TryGetStatus(RunEvent @event, out JobStatus status)
    {
        if (@event.EventType == RunEventType.Status
            && short.TryParse(@event.Payload, out var raw)
            && Enum.IsDefined(typeof(JobStatus), (int)raw))
        {
            status = (JobStatus)raw;
            return true;
        }

        status = default;
        return false;
    }

    public static bool IsTerminal(RunEvent @event) =>
        TryGetStatus(@event, out var status) && status.IsTerminal;
}