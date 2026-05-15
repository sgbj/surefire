namespace Surefire.SourceGeneration;

/// <summary>
///     One source generator record per callback registration call site. Targets
///     <c>JobBuilder.OnSuccess/OnRetry/OnDeadLetter(Delegate)</c> and the
///     <c>SurefireOptions</c> equivalents.
/// </summary>
internal sealed record CallbackCall(
    string InterceptsLocationAttribute,
    HandlerSignature Handler,
    CallbackTarget Target,
    CallbackKind Kind);

internal enum CallbackTarget
{
    JobBuilder,
    SurefireOptions
}

internal enum CallbackKind
{
    OnSuccess,
    OnRetry,
    OnDeadLetter
}
