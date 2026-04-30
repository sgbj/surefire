namespace Surefire;

internal sealed class RunCancellationCoordinator(
    IJobStore store,
    INotificationProvider notifications,
    ActiveRunTracker activeRuns)
{
    private const int CancellationTraversalPageSize = 100;

    public async Task CancelRunAndDescendantsAsync(
        string runId,
        string reason,
        CancellationToken cancellationToken,
        bool cancelSelf = true)
    {
        await CancelRunAndDescendantsAsync(
            runId,
            reason,
            new HashSet<string>(StringComparer.Ordinal),
            cancellationToken,
            cancelSelf);
    }

    public async Task CancelRunAndDescendantsAsync(
        string runId,
        string reason,
        ISet<string> visited,
        CancellationToken cancellationToken,
        bool cancelSelf = true)
    {
        if (!visited.Add(runId))
        {
            return;
        }

        if (cancelSelf)
        {
            var result = await store.TryCancelRunAsync(runId, reason: reason, cancellationToken: cancellationToken);
            if (result.Transitioned)
            {
                await PublishCancellationNotificationsAsync(runId, cancellationToken);
                if (result.BatchCompletion is { } bc)
                {
                    await notifications.PublishAsync(NotificationChannels.BatchTerminated(bc.BatchId), bc.BatchId,
                        cancellationToken);
                }
            }
        }

        await CancelDirectChildrenAndDescendantsAsync(runId, visited, cancellationToken);
    }

    public async Task CancelDescendantsAsync(
        string parentRunId,
        CancellationToken cancellationToken,
        string? fixedReason = null)
    {
        await CancelDescendantsAsync(
            parentRunId,
            new HashSet<string>(StringComparer.Ordinal),
            cancellationToken,
            fixedReason);
    }

    public async Task PublishCancellationNotificationsAsync(string runId, CancellationToken cancellationToken)
    {
        var run = await store.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            return;
        }

        activeRuns.TryRequestCancel(runId);
        await notifications.PublishAsync(NotificationChannels.RunCancel(runId), null, cancellationToken);
        if (run.Status.IsTerminal)
        {
            await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunAvailable, runId, cancellationToken);
        }

        await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, cancellationToken);
        if (run.BatchId is { } batchId)
        {
            await notifications.PublishAsync(NotificationChannels.RunEvent(batchId), runId, cancellationToken);
            var batch = await store.GetBatchAsync(batchId, cancellationToken);
            if (batch?.Status.IsTerminal is true)
            {
                await notifications.PublishAsync(NotificationChannels.BatchTerminated(batchId), batchId,
                    cancellationToken);
            }
        }
    }

    private async Task CancelDescendantsAsync(
        string parentRunId,
        ISet<string> visited,
        CancellationToken cancellationToken,
        string? fixedReason = null)
    {
        if (!visited.Add(parentRunId))
        {
            return;
        }

        await CancelDirectChildrenAndDescendantsAsync(parentRunId, visited, cancellationToken, fixedReason);
    }

    private async Task CancelDirectChildrenAndDescendantsAsync(
        string parentRunId,
        ISet<string> visited,
        CancellationToken cancellationToken,
        string? fixedReason = null)
    {
        var reason = fixedReason ?? $"Canceled because parent run '{parentRunId}' was canceled.";
        var canceledChildIds = await store.CancelChildRunsAsync(parentRunId, reason, cancellationToken);
        foreach (var childId in canceledChildIds.Distinct(StringComparer.Ordinal))
        {
            await PublishCancellationNotificationsAsync(childId, cancellationToken);
        }

        string? cursor = null;
        do
        {
            var page = await store.GetDirectChildrenAsync(
                parentRunId,
                afterCursor: cursor,
                take: CancellationTraversalPageSize,
                cancellationToken: cancellationToken);

            foreach (var child in page.Items)
            {
                await CancelDescendantsAsync(child.Id, visited, cancellationToken, fixedReason);
            }

            cursor = page.NextCursor;
        }
        while (cursor is { });
    }
}
