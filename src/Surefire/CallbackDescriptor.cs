namespace Surefire;

/// <summary>
///     Pre-built metadata describing a lifecycle callback (<c>OnSuccess</c>, <c>OnRetry</c>, or
///     <c>OnDeadLetter</c>) attached to a job or to global options.
/// </summary>
public sealed class CallbackDescriptor
{
    /// <summary>The user-supplied callback delegate.</summary>
    public required Delegate Handler { get; init; }

    /// <summary>Parameter metadata for the callback, in declaration order.</summary>
    public required IReadOnlyList<ParameterDescriptor> Parameters { get; init; }

    /// <summary>
    ///     Invokes <see cref="Handler" /> with the bound arguments and returns its raw return
    ///     value (a <see cref="System.Threading.Tasks.Task" />,
    ///     <see cref="System.Threading.Tasks.ValueTask" />,
    ///     <see cref="System.Threading.Tasks.ValueTask{TResult}" />, or <c>null</c> for void).
    /// </summary>
    public required Func<object?[], Delegate, object?> Invoke { get; init; }

    /// <summary>Classification of the callback's return shape.</summary>
    public required ReturnKind ReturnKind { get; init; }

    /// <summary>
    ///     For <see cref="Surefire.ReturnKind.ValueTaskOfT" /> callbacks, converts the boxed
    ///     <see cref="System.Threading.Tasks.ValueTask{TResult}" /> to a
    ///     <see cref="System.Threading.Tasks.Task" />.
    /// </summary>
    public Func<object, Task>? AsTask { get; init; }
}
