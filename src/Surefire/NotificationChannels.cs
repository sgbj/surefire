using System.Security.Cryptography;
using System.Text;

namespace Surefire;

/// <summary>
///     Well-known notification channel names used for inter-node communication.
/// </summary>
public static class NotificationChannels
{
    private const int MaxChannelLength = 63;
    private const string RunPrefix = "surefire:run:";

    /// <summary>Broadcast channel for new run creation.</summary>
    public const string RunCreated = "surefire:run:created";

    /// <summary>Returns the channel name for cancelling a specific run.</summary>
    /// <param name="runId">The run identifier.</param>
    public static string RunCancel(string runId) => BuildRunChannel(runId, "cancel");

    /// <summary>Returns the channel name for run terminal notifications.</summary>
    /// <param name="runId">The run identifier.</param>
    public static string RunTerminated(string runId) => BuildRunChannel(runId, "terminal");

    /// <summary>Returns the channel name for run event streaming.</summary>
    /// <param name="runId">The run identifier.</param>
    public static string RunEvent(string runId) => BuildRunChannel(runId, "event");

    /// <summary>Returns the channel name for sending input to a running job.</summary>
    /// <param name="runId">The run identifier.</param>
    public static string RunInput(string runId) => BuildRunChannel(runId, "input");

    /// <summary>Returns the channel name for batch termination notifications.</summary>
    /// <param name="batchId">The batch identifier.</param>
    public static string BatchTerminated(string batchId) => BuildRunChannel(batchId, "batch-term");

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

        var channel = $"{RunPrefix}{runId}:{suffix}";
        if (channel.Length > MaxChannelLength)
        {
            var compactRunId = CompactRunId(runId, suffix);
            channel = $"{RunPrefix}{compactRunId}:{suffix}";
        }

        ValidateChannel(channel);
        return channel;
    }

    private static string CompactRunId(string runId, string suffix)
    {
        var maxRunIdLength = MaxChannelLength - RunPrefix.Length - suffix.Length - 1;
        if (maxRunIdLength <= 0)
        {
            throw new InvalidOperationException("Notification channel format does not allow run-scoped channels.");
        }

        var digest = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(runId))).ToLowerInvariant();
        return digest[..Math.Min(digest.Length, maxRunIdLength)];
    }
}