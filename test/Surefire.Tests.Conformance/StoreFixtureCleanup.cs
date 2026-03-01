using System.Data.Common;

namespace Surefire.Tests.Conformance;

public static class StoreFixtureCleanup
{
    public static string SqlServerDeleteAllScript { get; } = """
                                                             DELETE FROM dbo.surefire_events;
                                                             DELETE FROM dbo.surefire_runs;
                                                             DELETE FROM dbo.surefire_jobs;
                                                             DELETE FROM dbo.surefire_queues;
                                                             DELETE FROM dbo.surefire_rate_limits;
                                                             DELETE FROM dbo.surefire_nodes;
                                                             """;

    public static string DefaultDeleteAllScript { get; } = """
                                                           DELETE FROM surefire_events;
                                                           DELETE FROM surefire_runs;
                                                           DELETE FROM surefire_jobs;
                                                           DELETE FROM surefire_queues;
                                                           DELETE FROM surefire_rate_limits;
                                                           DELETE FROM surefire_nodes;
                                                           """;

    public static async Task ExecuteDeleteAllAsync(DbConnection connection, string commandText,
        CancellationToken cancellationToken = default)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = commandText;
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }
}