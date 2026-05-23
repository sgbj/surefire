namespace Surefire.SourceGeneration;

/// <summary>One source generator record per <c>app.AddJob("Name", handler)</c> call site.</summary>
internal sealed record AddJobCall(
    string InterceptsLocationAttribute,
    HandlerSignature Handler,
    string? SourceCode);
