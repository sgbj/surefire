namespace Surefire;

/// <summary>
///     Specifies the field used to order run query results.
/// </summary>
public enum RunOrderBy
{
    /// <summary>Order by run creation time.</summary>
    CreatedAt,

    /// <summary>Order by run start time.</summary>
    StartedAt,

    /// <summary>Order by run completion time.</summary>
    CompletedAt
}