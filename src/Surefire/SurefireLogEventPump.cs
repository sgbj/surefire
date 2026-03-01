using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class SurefireLogEventPump(
    IJobStore store,
    INotificationProvider notifications,
    SurefireOptions options) : BackgroundService
{
    private readonly Channel<LogEntry> _channel = Channel.CreateBounded<LogEntry>(new BoundedChannelOptions(1024)
    {
        FullMode = BoundedChannelFullMode.DropOldest,
        SingleReader = true,
        SingleWriter = false
    });

    public bool TryEnqueue(LogEntry entry)
    {
        if (string.IsNullOrWhiteSpace(entry.RunId) || string.IsNullOrWhiteSpace(entry.Message))
        {
            return false;
        }

        return _channel.Writer.TryWrite(entry);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var buffer = new List<LogEntry>(128);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var hasItem = await _channel.Reader.WaitToReadAsync(stoppingToken);
                if (!hasItem)
                {
                    break;
                }

                buffer.Clear();
                while (buffer.Count < 128 && _channel.Reader.TryRead(out var item))
                {
                    buffer.Add(item);
                }

                if (buffer.Count == 0)
                {
                    continue;
                }

                var events = buffer.Select(entry => new RunEvent
                    {
                        RunId = entry.RunId,
                        EventType = RunEventType.Log,
                        Payload = JsonSerializer.Serialize(new LogEventPayload
                        {
                            Timestamp = entry.Timestamp,
                            Category = entry.Category,
                            Level = (int)entry.Level,
                            EventId = entry.EventId,
                            EventName = entry.EventName,
                            Message = entry.Message,
                            Exception = entry.Exception
                        }, options.SerializerOptions),
                        CreatedAt = entry.Timestamp,
                        Attempt = entry.Attempt
                    })
                    .ToList();

                await store.AppendEventsAsync(events, stoppingToken);

                foreach (var runId in buffer.Select(e => e.RunId).Distinct(StringComparer.Ordinal))
                {
                    await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Trace.TraceWarning("Surefire log event pump failed: {0}", ex.Message);
                await Task.Delay(TimeSpan.FromMilliseconds(200), stoppingToken);
            }
        }
    }

    internal readonly record struct LogEntry(
        string RunId,
        int Attempt,
        DateTimeOffset Timestamp,
        string Category,
        LogLevel Level,
        int EventId,
        string? EventName,
        string Message,
        string? Exception);

    private sealed class LogEventPayload
    {
        public required DateTimeOffset Timestamp { get; init; }
        public required string Category { get; init; }
        public required int Level { get; init; }
        public required int EventId { get; init; }
        public string? EventName { get; init; }
        public required string Message { get; init; }
        public string? Exception { get; init; }
    }
}