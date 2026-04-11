namespace Surefire;

internal static class WakeupSignal
{
    public static Task ReleaseAsync(SemaphoreSlim wakeup)
    {
        try
        {
            wakeup.Release();
        }
        catch (SemaphoreFullException)
        {
        }
        catch (ObjectDisposedException)
        {
        }

        return Task.CompletedTask;
    }

    public static Task WaitAsync(SemaphoreSlim wakeup, TimeSpan pollingInterval, CancellationToken cancellationToken) =>
        wakeup.WaitAsync(pollingInterval, cancellationToken);
}
