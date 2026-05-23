using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Surefire;

/// <summary>
///     Pre-built metadata describing a job handler. Users normally register jobs via
///     <c>app.AddJob("Name", handler)</c>; this type is the lower-level entry point used by the
///     <see
///         cref="Microsoft.Extensions.Hosting.HostExtensions.AddJob(Microsoft.Extensions.Hosting.IHost, JobRegistrationDescriptor)" />
///     overload.
/// </summary>
public sealed class JobRegistrationDescriptor
{
    /// <summary>The registered job name.</summary>
    public required string Name { get; init; }

    /// <summary>The user-supplied handler delegate.</summary>
    public required Delegate Handler { get; init; }

    /// <summary>Parameter metadata, one entry per handler parameter, in declaration order.</summary>
    public required IReadOnlyList<ParameterDescriptor> Parameters { get; init; }

    /// <summary>
    ///     Invokes <see cref="Handler" /> with the bound arguments and returns its raw return
    ///     value (a <see cref="Task" />, <see cref="ValueTask" />, scalar,
    ///     <see cref="IAsyncEnumerable{T}" />, or <c>null</c>).
    /// </summary>
    public required Func<object?[], Delegate, object?> Invoke { get; init; }

    /// <summary>Classification of the handler's return shape.</summary>
    public required ReturnKind ReturnKind { get; init; }

    /// <summary>The declared return type of the handler.</summary>
    public required Type ReturnType { get; init; }

    /// <summary>
    ///     For <see cref="Surefire.ReturnKind.AsyncEnumerable" /> returns, the element type
    ///     (e.g. <c>int</c> for <c>IAsyncEnumerable&lt;int&gt;</c> or
    ///     <c>Task&lt;IAsyncEnumerable&lt;int&gt;&gt;</c>). <c>null</c> otherwise.
    /// </summary>
    public Type? AsyncEnumerableElementType { get; init; }

    /// <summary>
    ///     For <see cref="Surefire.ReturnKind.TaskOfT" /> and
    ///     <see cref="Surefire.ReturnKind.ValueTaskOfT" />, extracts the boxed result from a
    ///     completed <see cref="Task" />.
    /// </summary>
    public Func<Task, object?>? ExtractTaskResult { get; init; }

    /// <summary>
    ///     For <see cref="Surefire.ReturnKind.ValueTaskOfT" />, converts a boxed
    ///     <see cref="ValueTask{TResult}" /> into a <see cref="Task" /> for uniform awaiting.
    /// </summary>
    public Func<object, Task>? AsTask { get; init; }

    /// <summary>
    ///     For <see cref="Surefire.ReturnKind.AsyncEnumerable" /> returns, the materializer that
    ///     persists each yielded element.
    /// </summary>
    public OutputStreamMaterializer? Materializer { get; init; }

    /// <summary>
    ///     Indexed by parameter position. Non-null entries bind a
    ///     <see cref="ParameterKind.Stream" /> parameter from a declared input stream.
    /// </summary>
    public IReadOnlyList<InputStreamBinder?>? StreamBinders { get; init; }

    /// <summary>
    ///     Indexed by parameter position. Non-null entries bind a
    ///     <see cref="ParameterKind.Stream" /> parameter from an inline JSON value on the run's
    ///     <c>Arguments</c> (used when the caller passed an array literal instead of a streamed
    ///     input). Each delegate adapts the deserialized result to the parameter's declared shape
    ///     (<see cref="IAsyncEnumerable{T}" />, <see cref="List{T}" />, or <c>T[]</c>).
    /// </summary>
    public IReadOnlyList<Func<JsonElement, JsonSerializerOptions, object?>?>? InputJsonBinders { get; init; }

    /// <summary>
    ///     Returns the <see cref="JsonTypeInfo" /> used to serialize the handler's return value,
    ///     or <c>null</c> when the handler has no JSON-serialized result.
    /// </summary>
    public JsonTypeInfoFactory? ResultJsonTypeInfoFactory { get; init; }

    /// <summary>
    ///     Per-parameter <see cref="JsonTypeInfo" /> factories. Indexed by parameter position;
    ///     non-null entries correspond to parameters with <see cref="ParameterKind.Json" /> or
    ///     <see cref="ParameterKind.ServiceOrJson" />.
    /// </summary>
    public IReadOnlyList<JsonTypeInfoFactory?>? ParameterJsonTypeInfoFactories { get; init; }

    /// <summary>
    ///     Returns the <see cref="JsonTypeInfo" /> for the element type of an
    ///     <see cref="IAsyncEnumerable{T}" /> return, supplied to <see cref="Materializer" /> by
    ///     the runtime. Non-null when <see cref="AsyncEnumerableElementType" /> is set.
    /// </summary>
    public JsonTypeInfoFactory? OutputStreamElementJsonTypeInfoFactory { get; init; }

    /// <summary>
    ///     Per-parameter <see cref="JsonTypeInfo" /> factories for stream-kind parameters,
    ///     supplied to <see cref="StreamBinders" /> by the runtime. Indexed by parameter position;
    ///     non-null entries correspond to parameters with <see cref="ParameterKind.Stream" />.
    /// </summary>
    public IReadOnlyList<JsonTypeInfoFactory?>? StreamParameterJsonTypeInfoFactories { get; init; }

    /// <summary>
    ///     A JSON Schema describing the handler's bindable arguments, or <c>null</c> when the
    ///     handler takes no JSON-bindable parameters.
    /// </summary>
    public string? ArgumentsSchema { get; init; }

    /// <summary>
    ///     Source code captured for the job registration, or <c>null</c> when the source is unavailable.
    /// </summary>
    public string? SourceCode { get; init; }
}
