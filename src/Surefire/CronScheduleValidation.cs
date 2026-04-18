using Cronos;

namespace Surefire;

internal static class CronScheduleValidation
{
    internal static void Validate(string cronExpression, string? timeZoneId)
    {
        if (!TryParseCron(cronExpression, out _))
        {
            throw new ArgumentException("Cron expression is invalid.", nameof(cronExpression));
        }

        if (!TryResolveTimeZone(timeZoneId, out _))
        {
            throw new ArgumentException("Time zone ID is invalid.", nameof(timeZoneId));
        }
    }

    internal static bool TryParseCron(string cronExpression, out CronExpression cron)
    {
        try
        {
            var fields = cronExpression.Split(' ',
                StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            cron = fields.Length == 6
                ? CronExpression.Parse(cronExpression, CronFormat.IncludeSeconds)
                : CronExpression.Parse(cronExpression, CronFormat.Standard);
            return true;
        }
        catch
        {
            cron = null!;
            return false;
        }
    }

    internal static bool TryResolveTimeZone(string? timeZoneId, out TimeZoneInfo timeZone)
    {
        if (string.IsNullOrWhiteSpace(timeZoneId))
        {
            timeZone = TimeZoneInfo.Utc;
            return true;
        }

        try
        {
            timeZone = TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
            return true;
        }
        catch
        {
            timeZone = null!;
            return false;
        }
    }
}