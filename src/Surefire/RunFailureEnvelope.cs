namespace Surefire;

internal sealed class RunFailureEnvelope
{
    public required int Attempt { get; init; }
    public required DateTimeOffset OccurredAt { get; init; }
    public required string FailureSource { get; init; }
    public required string FailureCode { get; init; }
    public string? ExceptionType { get; init; }
    public required string Message { get; init; }
    public string? StackTrace { get; init; }

    public static RunFailureEnvelope FromException(int attempt, DateTimeOffset occurredAt, Exception exception,
        string failureSource, string failureCode = "exception") => new()
    {
        Attempt = attempt,
        OccurredAt = occurredAt,
        FailureSource = failureSource,
        FailureCode = failureCode,
        ExceptionType = exception.GetType().FullName ?? exception.GetType().Name,
        Message = exception.Message,
        StackTrace = exception.ToString()
    };

    public static RunFailureEnvelope FromMessage(int attempt, DateTimeOffset occurredAt, string failureSource,
        string failureCode, string message, string? exceptionType = null) => new()
    {
        Attempt = attempt,
        OccurredAt = occurredAt,
        FailureSource = failureSource,
        FailureCode = failureCode,
        ExceptionType = exceptionType,
        Message = message,
        StackTrace = null
    };
}