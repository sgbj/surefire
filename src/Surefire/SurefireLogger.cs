using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class SurefireLogger(string categoryName, ChannelWriter<RunLogEntry> writer, TimeProvider timeProvider) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None && JobContext.Current.Value is not null;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        var ctx = JobContext.Current.Value;
        if (ctx is null) return;

        var entry = new RunLogEntry
        {
            RunId = ctx.RunId,
            Timestamp = timeProvider.GetUtcNow(),
            Level = logLevel,
            Message = formatter(state, exception),
            Category = categoryName
        };

        writer.TryWrite(entry);
    }
}
