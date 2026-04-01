namespace Surefire;

internal sealed class InputDeclarationEnvelope
{
    public required string[] Arguments { get; init; }
}

internal sealed class InputEnvelope
{
    public required string Argument { get; init; }
    public required long Sequence { get; init; }
    public string? Payload { get; init; }
    public required bool IsComplete { get; init; }
    public string? Error { get; init; }
}