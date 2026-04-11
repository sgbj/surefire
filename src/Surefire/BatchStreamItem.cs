namespace Surefire;

/// <summary>
///     Represents one output item emitted by a child run in a batch stream.
/// </summary>
/// <typeparam name="T">The output item type.</typeparam>
public sealed record BatchStreamItem<T>
{
    /// <summary>
    ///     The child run ID that emitted <see cref="Item" />.
    /// </summary>
    public required string RunId { get; init; }

    /// <summary>
    ///     The output item value.
    /// </summary>
    public required T Item { get; init; }

    /// <summary>
    ///     The next batch stream resume cursor after this item.
    /// </summary>
    public required BatchEventCursor Cursor { get; init; }
}
