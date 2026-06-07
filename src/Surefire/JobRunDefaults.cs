namespace Surefire;

internal static class JobRunDefaults
{
    public static DateTimeOffset? GetDefaultExpiresAt(DateTimeOffset notBefore, SurefireOptions options) =>
        options.EffectiveRunExpirationPeriod is { } expiration
            ? notBefore + expiration
            : null;
}
