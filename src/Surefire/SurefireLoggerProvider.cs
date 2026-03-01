using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class SurefireLoggerProvider(IServiceProvider services, TimeProvider timeProvider) : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new SurefireRunLogger(categoryName, services, timeProvider);

    public void Dispose()
    {
    }
}

file sealed class SurefireRunLogger(string categoryName, IServiceProvider services, TimeProvider timeProvider) : ILogger
{
    private SurefireLogEventPump? _pump;

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state,
        Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }

        var context = JobContext.Current;
        if (context is null)
        {
            return;
        }

        _pump ??= services.GetService<SurefireLogEventPump>();
        if (_pump is null)
        {
            return;
        }

        var message = formatter(state, exception);
        var entry = new SurefireLogEventPump.LogEntry(
            context.RunId,
            context.Attempt,
            timeProvider.GetUtcNow(),
            categoryName,
            logLevel,
            eventId.Id,
            eventId.Name,
            message,
            exception?.ToString());

        _pump.TryEnqueue(entry);
    }
}