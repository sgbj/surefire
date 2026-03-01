namespace Surefire;

public sealed class PagedResult<T>
{
    public required IReadOnlyList<T> Items { get; set; }
    public int TotalCount { get; set; }
}
