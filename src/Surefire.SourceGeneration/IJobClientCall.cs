namespace Surefire.SourceGeneration;

/// <summary>One source generator record per intercepted <c>IJobClient</c> call site.</summary>
/// <param name="ResultElementTypeName">
///     When <see cref="ResultIsAsyncEnumerable" /> is true, the element type of the
///     <see cref="System.Collections.Generic.IAsyncEnumerable{T}" /> result (e.g. <c>int</c>
///     for <c>WaitEachAsync&lt;IAsyncEnumerable&lt;int&gt;&gt;</c>). The interceptor uses this
///     to construct per-child live inner streams without runtime generic instantiation.
/// </param>
internal sealed record IJobClientCall(
    string InterceptsLocationAttribute,
    IJobClientMethod Method,
    string ReceiverTypeName,
    string? ResultTypeName,
    bool ResultIsAsyncEnumerable,
    string? ResultElementTypeName,
    ArgsExpressionShape ArgsShape,
    EquatableArray<AnonArgProperty> AnonProperties,
    string? NamedArgsTypeName = null);

internal enum IJobClientMethod
{
    TriggerAsync,
    RunAsync,
    StreamAsync,
    WaitAsync,
    WaitStreamAsync,
    RunBatchAsync,
    StreamBatchAsync,
    TriggerBatchAsync,
    WaitBatchAsync,
    WaitEachAsync,

    /// <summary>
    ///     <c>Surefire.BatchItem.Create(string, object?, BatchRunOptions?)</c>. Not on
    ///     <see cref="Surefire.IJobClient" />, but follows the same shape-based interception model
    ///     as the single-run methods.
    /// </summary>
    BatchItemCreate
}

internal enum ArgsExpressionShape
{
    /// <summary>Method takes no args parameter (e.g., <c>WaitAsync&lt;T&gt;</c>).</summary>
    None,

    /// <summary>The args expression is the <c>null</c> literal.</summary>
    Null,

    /// <summary>The args expression is an anonymous-object initializer like <c>new { a, b }</c>.</summary>
    Anonymous,

    /// <summary>
    ///     The args expression is statically typed as a named class or record. The generator emits
    ///     <c>JsonSerializer.Serialize(args, opts.GetTypeInfo&lt;T&gt;())</c> at the interceptor.
    /// </summary>
    NamedType,

    /// <summary>The args expression is already a <c>RunArguments</c> instance; pass through.</summary>
    RunArguments
}

internal sealed record AnonArgProperty(
    string Name,
    string TypeName,
    bool IsStream,
    string? StreamElementTypeName);
