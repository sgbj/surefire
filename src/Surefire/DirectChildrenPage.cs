using System.Globalization;

namespace Surefire;

/// <summary>
///     A page of direct children of a parent run, with a cursor for the next page. The cursor is
///     opaque: callers pass the previous response's <see cref="NextCursor" /> verbatim, and the
///     store encodes it from the last returned row's <c>(CreatedAt, Id)</c> keyset.
/// </summary>
public sealed class DirectChildrenPage
{
    /// <summary>Children in this page, ordered by <c>(CreatedAt ASC, Id ASC)</c>.</summary>
    public required IReadOnlyList<JobRun> Items { get; init; }

    /// <summary>Cursor for the next page, or null if this was the last page.</summary>
    public string? NextCursor { get; init; }

    /// <summary>
    ///     Encodes a <c>(createdAt, id)</c> keyset tuple into the opaque cursor format
    ///     <c>{utcTicks}.{id}</c>. UTC ticks preserve full <see cref="DateTimeOffset" /> precision
    ///     (100ns); Unix ms would lose sub-ms precision and cause boundary items to reappear on
    ///     subsequent pages.
    /// </summary>
    public static string EncodeCursor(DateTimeOffset createdAt, string id) =>
        $"{createdAt.UtcTicks}.{id}";

    /// <summary>
    ///     Decodes a cursor previously produced by <see cref="EncodeCursor" />. Returns
    ///     <c>null</c> if <paramref name="cursor" /> is null/empty. Throws
    ///     <see cref="FormatException" /> on a malformed cursor (including unparseable
    ///     numeric prefix or out-of-range ticks).
    /// </summary>
    public static (DateTimeOffset CreatedAt, string Id)? DecodeCursor(string? cursor)
    {
        if (string.IsNullOrEmpty(cursor))
        {
            return null;
        }

        var dot = cursor.IndexOf('.');
        if (dot <= 0 || dot == cursor.Length - 1)
        {
            throw new FormatException($"Malformed children cursor: '{cursor}'.");
        }

        if (!long.TryParse(cursor.AsSpan(0, dot), CultureInfo.InvariantCulture, out var ticks))
        {
            throw new FormatException($"Malformed children cursor (unparseable ticks): '{cursor}'.");
        }

        try
        {
            return (new(ticks, TimeSpan.Zero), cursor[(dot + 1)..]);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            throw new FormatException($"Malformed children cursor (ticks out of range): '{cursor}'.", ex);
        }
    }
}
