namespace Surefire;

internal static class CommandTimeouts
{
    public static int? ToSeconds(TimeSpan? timeout, string paramName)
    {
        if (timeout is null)
        {
            return null;
        }

        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(paramName, "Command timeout must be greater than zero.");
        }

        if (timeout > TimeSpan.FromSeconds(int.MaxValue))
        {
            throw new ArgumentOutOfRangeException(paramName, "Command timeout is too large.");
        }

        return (int)Math.Ceiling(timeout.Value.TotalSeconds);
    }
}