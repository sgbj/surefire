using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class SurefireLogger(string categoryName, SurefireLoggerProvider provider, TimeProvider timeProvider) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None && JobContext.Current.Value is not null;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;
        var ctx = JobContext.Current.Value!;

        try
        {
            var now = timeProvider.GetUtcNow();

            var payload = JsonSerializer.Serialize(new
            {
                timestamp = now,
                level = (int)logLevel,
                message = formatter(state, exception),
                category = categoryName
            });

            var evt = new RunEvent
            {
                RunId = ctx.RunId,
                EventType = RunEventType.Log,
                Payload = payload,
                CreatedAt = now
            };

            provider.Enqueue(evt);
        }
        catch
        {
            // ILogger.Log must never throw — silently drop the event on failure.
        }
    }
}
