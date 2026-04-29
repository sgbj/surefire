namespace Surefire;

/// <summary>
///     A page of query results with the total count of matching items.
/// </summary>
/// <typeparam name="T">The type of items in the page.</typeparam>
public sealed record PagedResult<T>
{
    /// <summary>The items in this page.</summary>
    public required IReadOnlyList<T> Items { get; init; }

    /// <summary>The total number of items matching the query across all pages.</summary>
    public int TotalCount { get; init; }
}
