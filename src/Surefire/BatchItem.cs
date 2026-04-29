namespace Surefire;

/// <summary>Represents a single item in a batch job submission.</summary>
/// <param name="JobName">The name of the job to run.</param>
/// <param name="Args">Optional arguments to pass to the job.</param>
/// <param name="Options">Optional run options for this item.</param>
public record BatchItem(string JobName, object? Args = null, BatchRunOptions? Options = null);
