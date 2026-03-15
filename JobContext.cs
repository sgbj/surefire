using System.Text.Json;

namespace Surefire;

public sealed class JobContext
{
    internal static readonly AsyncLocal<JobContext?> Current = new();

    public required string RunId { get; init; }
    public required string JobName { get; init; }
    public required CancellationToken CancellationToken { get; init; }
    public int Attempt { get; init; } = 1;
    public string? PlanRunId { get; init; }
    public object? Result { get; internal set; }
    public Exception? Exception { get; internal set; }
    public IDictionary<string, object?> Items { get; } = new Dictionary<string, object?>();

    internal IJobStore Store { get; set; } = null!;
    internal INotificationProvider Notifications { get; set; } = null!;
    internal JobRun Run { get; set; } = null!;
    internal TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    public async Task ReportProgressAsync(double progress)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(progress, 0.0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(progress, 1.0);
        Run.Progress = progress;
        await Store.UpdateRunAsync(Run, CancellationToken);

        var evt = new RunEvent
        {
            RunId = RunId,
            EventType = RunEventType.Progress,
            Payload = JsonSerializer.Serialize(new { value = progress }),
            CreatedAt = TimeProvider.GetUtcNow()
        };
        await Store.AppendEventAsync(evt, CancellationToken);
        await Notifications.PublishAsync(NotificationChannels.RunEvent(RunId), "", CancellationToken);
    }
}
