using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Surefire;

/// <summary>
///     Represents the outcome of a job run.
/// </summary>
public sealed record RunResult
{
    /// <summary>The unique identifier of the run.</summary>
    public required string RunId { get; init; }

    /// <summary>The name of the job that was executed.</summary>
    public required string JobName { get; init; }

    /// <summary>The final status of the run.</summary>
    public required JobStatus Status { get; init; }

    /// <summary>The error message if the run failed, or null if it succeeded.</summary>
    public string? Error { get; init; }

    /// <summary>The serialized JSON result, if any.</summary>
    internal string? ResultJson { get; init; }

    /// <summary>The serializer options used to deserialize the result.</summary>
    internal JsonSerializerOptions? SerializerOptions { get; init; }

    /// <summary>Gets whether the run completed successfully.</summary>
    public bool IsSuccess => Status is JobStatus.Completed;

    /// <summary>Gets whether the run failed permanently.</summary>
    public bool IsFailure => Status is JobStatus.DeadLetter;

    /// <summary>Gets whether the run was cancelled.</summary>
    public bool IsCancelled => Status is JobStatus.Cancelled;

    /// <summary>
    ///     Deserializes and returns the result value.
    /// </summary>
    /// <typeparam name="T">The type to deserialize the result as.</typeparam>
    /// <returns>The deserialized result.</returns>
    /// <exception cref="InvalidOperationException">The run did not produce a result.</exception>
    public T GetResult<T>()
    {
        if (ResultJson is null)
        {
            throw new InvalidOperationException(
                "Run did not produce a result. Check IsSuccess before calling GetResult<T>().");
        }

        return JsonSerializer.Deserialize<T>(ResultJson, SerializerOptions)!;
    }

    /// <summary>
    ///     Attempts to deserialize and return the result value.
    /// </summary>
    /// <typeparam name="T">The type to deserialize the result as.</typeparam>
    /// <param name="result">The deserialized result, if available.</param>
    /// <returns>True if the result was successfully deserialized; otherwise, false.</returns>
    public bool TryGetResult<T>([MaybeNullWhen(false)] out T result)
    {
        if (ResultJson is { } && Status is JobStatus.Completed)
        {
            result = JsonSerializer.Deserialize<T>(ResultJson, SerializerOptions)!;
            return true;
        }

        result = default;
        return false;
    }
}