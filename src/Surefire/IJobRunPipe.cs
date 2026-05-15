using System.Text.Json.Serialization.Metadata;

namespace Surefire;

/// <summary>
///     Provides streaming and materialization services for generated job handlers.
/// </summary>
/// <remarks>
///     This interface is used by Surefire-generated descriptors and is not intended for application code.
/// </remarks>
public interface IJobRunPipe
{
    /// <summary>
    ///     Writes every item from <paramref name="stream" /> as an Output event for
    ///     <paramref name="run" />, serializing each element with the supplied
    ///     <see cref="JsonTypeInfo{T}" />, and returns the serialized payloads.
    /// </summary>
    Task<IReadOnlyList<string>> WriteOutputStreamAsync<T>(
        IAsyncEnumerable<T> stream,
        JsonTypeInfo<T> typeInfo,
        JobRun run,
        CancellationToken cancellationToken);

    /// <summary>
    ///     Reads an input stream declared on <paramref name="run" />, yielding deserialized elements
    ///     as they arrive.
    /// </summary>
    IAsyncEnumerable<T> ReadInputStreamAsync<T>(
        JobRun run,
        string argumentName,
        JsonTypeInfo<T> typeInfo,
        CancellationToken cancellationToken);
}

/// <summary>
///     Materializes a handler's <see cref="IAsyncEnumerable{T}" /> return value by streaming each
///     element through <see cref="IJobRunPipe.WriteOutputStreamAsync{T}" /> with the closed
///     element type known at compile time. The runtime resolves <paramref name="typeInfo" /> from
///     the descriptor's <see cref="JobRegistrationDescriptor.OutputStreamElementJsonTypeInfoFactory" />
///     before invoking the delegate.
/// </summary>
public delegate Task<IReadOnlyList<string>> OutputStreamMaterializer(
    IJobRunPipe pipe,
    object stream,
    JsonTypeInfo typeInfo,
    JobRun run,
    CancellationToken cancellationToken);

/// <summary>
///     Binds a handler parameter whose value comes from an input stream declared on the run.
///     The delegate uses <see cref="IJobRunPipe.ReadInputStreamAsync{T}" /> with the closed
///     element type known at compile time and adapts the result to the parameter's declared shape
///     (<see cref="IAsyncEnumerable{T}" />, <see cref="List{T}" />, or <c>T[]</c>). The runtime
///     resolves <paramref name="typeInfo" /> from the descriptor's
///     <see cref="JobRegistrationDescriptor.StreamParameterJsonTypeInfoFactories" /> before
///     invoking the delegate.
/// </summary>
public delegate Task<object?> InputStreamBinder(
    IJobRunPipe pipe,
    JobRun run,
    string argumentName,
    JsonTypeInfo typeInfo,
    CancellationToken cancellationToken);
