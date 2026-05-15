namespace Surefire;

/// <summary>Metadata describing a single job handler parameter.</summary>
/// <param name="Name">The parameter name as it appears in the handler.</param>
/// <param name="Type">The declared parameter type.</param>
/// <param name="Kind">How the parameter is sourced at run time.</param>
/// <param name="HasDefault">Whether the parameter has a default value.</param>
/// <param name="DefaultValue">The boxed default value when <paramref name="HasDefault" /> is <c>true</c>.</param>
/// <param name="IsNullable">
///     Whether the parameter's nullability annotation is nullable (a <c>?</c>-suffixed reference
///     type or a <see cref="Nullable{T}" /> value type). Determines the JSON-schema <c>required</c>
///     set.
/// </param>
/// <param name="StreamElementType">
///     For <see cref="ParameterKind.Stream" /> parameters, the element type of the underlying
///     stream (e.g. <c>int</c> for <c>IAsyncEnumerable&lt;int&gt;</c>). <c>null</c> otherwise.
/// </param>
/// <param name="StreamShape">
///     For <see cref="ParameterKind.Stream" /> parameters, the materialized collection shape.
///     <c>null</c> for non-stream parameters.
/// </param>
public sealed record ParameterDescriptor(
    string Name,
    Type Type,
    ParameterKind Kind,
    bool HasDefault = false,
    object? DefaultValue = null,
    bool IsNullable = false,
    Type? StreamElementType = null,
    StreamShape? StreamShape = null);
