namespace Surefire;

/// <summary>
///     Describes the shape of a job handler's return value. The runtime uses this to await and
///     unwrap the handler's return without inspecting <see cref="System.Reflection.MethodInfo.ReturnType" />
///     on each invocation.
/// </summary>
public enum ReturnKind
{
    /// <summary>The handler returns <c>void</c> or has no result to capture.</summary>
    Void,

    /// <summary>The handler returns a non-generic <see cref="System.Threading.Tasks.Task" />.</summary>
    Task,

    /// <summary>The handler returns a non-generic <see cref="System.Threading.Tasks.ValueTask" />.</summary>
    ValueTask,

    /// <summary>The handler returns <see cref="System.Threading.Tasks.Task{TResult}" />.</summary>
    TaskOfT,

    /// <summary>The handler returns <see cref="System.Threading.Tasks.ValueTask{TResult}" />.</summary>
    ValueTaskOfT,

    /// <summary>
    ///     The handler returns <see cref="System.Collections.Generic.IAsyncEnumerable{T}" /> directly,
    ///     or <see cref="System.Threading.Tasks.Task{TResult}" /> /
    ///     <see cref="System.Threading.Tasks.ValueTask{TResult}" /> wrapping one.
    /// </summary>
    AsyncEnumerable,

    /// <summary>The handler returns a synchronous scalar value (anything else).</summary>
    Scalar
}
