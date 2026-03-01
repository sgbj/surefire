namespace Surefire;

/// <summary>
///     Well-known notification channel names used for inter-node communication.
/// </summary>
public static class NotificationChannels
{
    private const int MaxChannelLength = 63;

    /// <summary>Broadcast channel for new run creation.</summary>
    public const string RunCreated = "surefire:run:created";

    /// <summary>Returns the channel name for cancelling a specific run.</summary>
    /// <param name="runId">The run identifier.</param>
    public static string RunCancel(string runId) => BuildRunChannel(runId, "cancel");

    /// <summary>Returns the channel name for run completion notifications.</summary>
    /// <param name="runId">The run identifier.</param>
    public static string RunCompleted(string runId) => BuildRunChannel(runId, "completed");

    /// <summary>Returns the channel name for run event streaming.</summary>
    /// <param name="runId">The run identifier.</param>
    public static string RunEvent(string runId) => BuildRunChannel(runId, "event");

    /// <summary>Returns the channel name for sending input to a running job.</summary>
    /// <param name="runId">The run identifier.</param>
    public static string RunInput(string runId) => BuildRunChannel(runId, "input");

    /// <summary>
    ///     Validates that a notification channel is safe and portable across providers.
    /// </summary>
    /// <param name="channel">The channel name.</param>
    /// <exception cref="ArgumentException">Thrown when the channel is empty, too long, or contains invalid characters.</exception>
    public static void ValidateChannel(string channel)
    {
        if (string.IsNullOrWhiteSpace(channel))
        {
            throw new ArgumentException("Notification channel cannot be null or whitespace.", nameof(channel));
        }

        if (channel.Length > MaxChannelLength)
        {
            throw new ArgumentException(
                $"Notification channel '{channel}' exceeds {MaxChannelLength} characters.", nameof(channel));
        }

        foreach (var ch in channel)
        {
            var isValid = (ch >= 'a' && ch <= 'z')
                          || (ch >= 'A' && ch <= 'Z')
                          || (ch >= '0' && ch <= '9')
                          || ch is ':' or '_' or '-' or '.';
            if (!isValid)
            {
                throw new ArgumentException(
                    $"Notification channel '{channel}' contains invalid character '{ch}'.",
                    nameof(channel));
            }
        }
    }

    private static string BuildRunChannel(string runId, string suffix)
    {
        if (string.IsNullOrWhiteSpace(runId))
        {
            throw new ArgumentException("Run ID cannot be null or whitespace.", nameof(runId));
        }

        var channel = $"surefire:run:{runId}:{suffix}";
        ValidateChannel(channel);
        return channel;
    }
}