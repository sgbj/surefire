namespace Surefire.Dashboard;

/// <summary>
///     Configuration for the Surefire dashboard endpoints. Pass to
///     <c>MapSurefireDashboard</c> via the <c>configure</c> callback to override defaults.
/// </summary>
public sealed class SurefireDashboardOptions
{
    /// <summary>
    ///     Maximum number of runs returned by the <c>/api/runs/{id}/tree</c> endpoint in a single
    ///     response. Trees larger than this come back with <c>Truncated = true</c> and a
    ///     <c>TotalCount</c> reflecting the full size, so the UI can banner instead of silently
    ///     dropping rows. Defaults to <c>50_000</c>; operators with unusually large run trees
    ///     can raise it (at the cost of payload size and query latency).
    /// </summary>
    public int MaxTreeRuns
    {
        get;
        set
        {
            if (value <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "MaxTreeRuns must be greater than zero.");
            }

            field = value;
        }
    } = 50_000;
}
