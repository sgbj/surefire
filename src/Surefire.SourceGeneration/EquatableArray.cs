using System.Collections;
using System.Collections.Immutable;

namespace Surefire.SourceGeneration;

/// <summary>
///     Wraps <see cref="ImmutableArray{T}" /> with sequence equality so it can flow through the
///     incremental generator pipeline without invalidating the cache on every keystroke. The
///     default <see cref="ImmutableArray{T}" /> compares by backing-array reference, which makes
///     records containing one re-evaluate every run.
/// </summary>
internal readonly struct EquatableArray<T> : IEquatable<EquatableArray<T>>, IReadOnlyList<T>
{
    public static readonly EquatableArray<T> Empty = new(ImmutableArray<T>.Empty);

    private readonly ImmutableArray<T> _items;

    public EquatableArray(ImmutableArray<T> items) => _items = items;

    public ImmutableArray<T> AsImmutable() => _items.IsDefault ? ImmutableArray<T>.Empty : _items;

    public int Length => _items.IsDefault ? 0 : _items.Length;
    public int Count => Length;
    public bool IsEmpty => Length == 0;
    public T this[int index] => _items[index];

    public bool Equals(EquatableArray<T> other)
    {
        var a = AsImmutable();
        var b = other.AsImmutable();
        if (a.Length != b.Length)
        {
            return false;
        }

        for (var i = 0; i < a.Length; i++)
        {
            if (!EqualityComparer<T>.Default.Equals(a[i], b[i]))
            {
                return false;
            }
        }

        return true;
    }

    public override bool Equals(object? obj) => obj is EquatableArray<T> other && Equals(other);

    public override int GetHashCode()
    {
        unchecked
        {
            var hash = 17;
            foreach (var item in AsImmutable())
            {
                hash = hash * 31 + (item?.GetHashCode() ?? 0);
            }

            return hash;
        }
    }

    public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)AsImmutable()).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public static bool operator ==(EquatableArray<T> left, EquatableArray<T> right) => left.Equals(right);
    public static bool operator !=(EquatableArray<T> left, EquatableArray<T> right) => !left.Equals(right);

    public static implicit operator EquatableArray<T>(ImmutableArray<T> items) => new(items);
}
