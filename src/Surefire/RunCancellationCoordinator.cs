namespace Surefire;

internal sealed class RunCancellationCoordinator(
    IJobStore store,
    INotificationProvider notifications,
    ActiveRunTracker activeRuns)
{
    public async Task<SubtreeCancellation> CancelRunAndDescendantsAsync(
        string runId,
        string reason,
        CancellationToken cancellationToken,
        bool cancelSelf = true)
    {
        var result = await store.CancelRunSubtreeAsync(runId, reason, includeRoot: cancelSelf,
            cancellationToken);
        await PublishSubtreeCancellationAsync(result, cancellationToken);
        return result;
    }

    public Task<SubtreeCancellation> CancelDescendantsAsync(
        string parentRunId,
        CancellationToken cancellationToken,
        string? fixedReason = null)
    {
        var reason = fixedReason ?? $"Canceled because parent run '{parentRunId}' was canceled.";
        return CancelRunAndDescendantsAsync(parentRunId, reason, cancellationToken, cancelSelf: false);
    }

    public async Task<SubtreeCancellation> CancelBatchSubtreeAsync(
        string batchId,
        string reason,
        CancellationToken cancellationToken)
    {
        var result = await store.CancelBatchSubtreeAsync(batchId, reason, cancellationToken);
        await PublishSubtreeCancellationAsync(result, cancellationToken);
        return result;
    }

    private async Task PublishSubtreeCancellationAsync(SubtreeCancellation result,
        CancellationToken cancellationToken)
    {
        if (result.Runs.Count == 0 && result.CompletedBatches.Count == 0)
        {
            return;
        }

        foreach (var (runId, _) in result.Runs)
        {
            activeRuns.TryRequestCancel(runId);
        }

        foreach (var entry in result.Runs)
        {
            var runId = entry.RunId;
            await notifications.PublishAsync(NotificationChannels.RunCancel(runId), null, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunAvailable, runId, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, cancellationToken);
            if (entry.BatchId is { } batchId)
            {
                await notifications.PublishAsync(NotificationChannels.RunEvent(batchId), runId, cancellationToken);
            }
        }

        foreach (var batch in result.CompletedBatches)
        {
            await notifications.PublishAsync(NotificationChannels.BatchTerminated(batch.BatchId), batch.BatchId,
                cancellationToken);
        }
    }
}
