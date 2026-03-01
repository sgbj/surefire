using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class SurefireLoggerProvider : ILoggerProvider, IAsyncDisposable
{
    private readonly IJobStore _store;
    private readonly INotificationProvider _notifications;
    private readonly TimeProvider _timeProvider;
    private readonly SurefireOptions _options;
    private readonly Channel<RunLogEntry> _channel;
    private readonly Task _flushTask;
    private readonly ConcurrentDictionary<string, SurefireLogger> _loggers = new();
    private int _disposed;

    public SurefireLoggerProvider(IJobStore store, INotificationProvider notifications, SurefireOptions options, TimeProvider timeProvider)
    {
        _store = store;
        _notifications = notifications;
        _options = options;
        _timeProvider = timeProvider;
        _channel = Channel.CreateUnbounded<RunLogEntry>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
        _flushTask = FlushLoopAsync();
    }

    public ILogger CreateLogger(string categoryName) =>
        _loggers.GetOrAdd(categoryName, name => new SurefireLogger(name, _channel.Writer, _timeProvider));

    private async Task FlushLoopAsync()
    {
        var buffer = new List<RunLogEntry>(64);

        while (await _channel.Reader.WaitToReadAsync())
        {
            buffer.Clear();
            while (buffer.Count < 64 && _channel.Reader.TryRead(out var item))
                buffer.Add(item);

            await FlushBufferAsync(buffer);
        }

        // Drain any remaining items after the channel is completed
        buffer.Clear();
        while (_channel.Reader.TryRead(out var item))
            buffer.Add(item);

        if (buffer.Count > 0)
            await FlushBufferAsync(buffer);
    }

    private async Task FlushBufferAsync(List<RunLogEntry> buffer)
    {
        var groups = buffer.GroupBy(x => x.RunId);
        foreach (var group in groups)
        {
            var entries = group.ToList();
            try
            {
                await _store.AddRunLogsAsync(entries);
                foreach (var entry in entries)
                {
                    await _notifications.PublishAsync(
                        NotificationChannels.RunLog(group.Key),
                        JsonSerializer.Serialize(entry, _options.SerializerOptions));
                }
            }
            catch (Exception ex) { Debug.WriteLine($"[Surefire] Failed to flush logs for run {group.Key}: {ex.Message}"); }
        }
    }

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        _channel.Writer.TryComplete();
        await _flushTask;
    }
}
