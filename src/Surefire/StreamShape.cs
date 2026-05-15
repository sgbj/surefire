namespace Surefire;

/// <summary>
///     The materialized shape of a <see cref="ParameterKind.Stream" /> parameter. Stream-shaped
///     parameters bind from a declared input stream or from inline JSON; the shape determines
///     which collection type the binder produces.
/// </summary>
public enum StreamShape
{
    /// <summary><see cref="System.Collections.Generic.IAsyncEnumerable{T}" />.</summary>
    AsyncEnumerable,

    /// <summary>
    ///     <see cref="System.Collections.Generic.List{T}" /> or one of its read-only/enumerable
    ///     interfaces (<see cref="System.Collections.Generic.IList{T}" />,
    ///     <see cref="System.Collections.Generic.IReadOnlyList{T}" />,
    ///     <see cref="System.Collections.Generic.IEnumerable{T}" />).
    /// </summary>
    List,

    /// <summary>A single-rank array <c>T[]</c>.</summary>
    Array
}
