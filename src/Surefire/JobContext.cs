namespace Surefire;

public sealed class JobContext
{
    internal static readonly AsyncLocal<JobContext?> Current = new();

    public required string RunId { get; init; }
    public required string JobName { get; init; }
    public required CancellationToken CancellationToken { get; init; }
    public int Attempt { get; init; } = 1;
    public object? Result { get; internal set; }
    public Exception? Exception { get; internal set; }

    internal IJobStore Store { get; set; } = null!;
    internal INotificationProvider Notifications { get; set; } = null!;
    internal JobRun Run { get; set; } = null!;

    public async Task ReportProgressAsync(double progress)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(progress, 0.0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(progress, 1.0);
        Run.Progress = progress;
        await Store.UpdateRunAsync(Run, CancellationToken);
        await Notifications.PublishAsync(NotificationChannels.RunProgress(RunId), progress.ToString(), CancellationToken);
    }
}
