namespace Surefire;

/// <summary>
///     Specifies the sort direction for run query results.
///     <para>
///         Null timestamps always sort to the end regardless of direction: an unstarted run
///         (null <c>StartedAt</c>) or non-terminal run (null <c>CompletedAt</c>) never crowds
///         the top of either an ascending or descending list. This is a cross-store contract
///         that overrides each backend's native null-ordering behavior.
///     </para>
/// </summary>
public enum RunOrderDirection
{
    /// <summary>Largest first (default). For timestamps this surfaces the most recent matches.</summary>
    Descending = 0,

    /// <summary>Smallest first. Useful for chronological iteration, backfills, and root-down tree walks.</summary>
    Ascending = 1
}
