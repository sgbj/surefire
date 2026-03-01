namespace Surefire.Tests.Integration;

internal static class TestWait
{
    public static async Task<T> PollUntilAsync<T>(
        Func<Task<T?>> probe,
        Func<T, bool> done,
        TimeSpan timeout,
        TimeSpan interval,
        string timeoutMessage)
        where T : class
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var current = await probe();
            if (current is { } && done(current))
            {
                return current;
            }

            await Task.Delay(interval);
        }

        throw new TimeoutException(timeoutMessage);
    }
}