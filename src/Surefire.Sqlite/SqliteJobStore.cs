using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Surefire.Sqlite;

internal sealed class SqliteJobStore(
    SqliteOptions sqliteOptions,
    TimeProvider timeProvider) : IJobStore
{
    private readonly string _connectionString = sqliteOptions.ConnectionString;

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, "SELECT 1;");
        _ = await cmd.ExecuteScalarAsync(cancellationToken);
    }

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);

        await using (var walCmd = CreateCommand(conn, "PRAGMA journal_mode=WAL;"))
        {
            await walCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var schemaCmd = CreateCommand(conn,
                         "CREATE TABLE IF NOT EXISTS surefire_schema_migrations (version INTEGER NOT NULL PRIMARY KEY);"))
        {
            await schemaCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        var hasV1 = await HasMigrationVersionAsync(conn, 1, cancellationToken);
        var hasV2 = await HasMigrationVersionAsync(conn, 2, cancellationToken);

        if (hasV1 && hasV2)
        {
            return;
        }

        if (!hasV1)
        {
            await using var ddlCmd = CreateCommand(conn, """
                                                     CREATE TABLE IF NOT EXISTS surefire_jobs (
                                                         name TEXT PRIMARY KEY,
                                                         description TEXT,
                                                         tags TEXT NOT NULL DEFAULT '[]',
                                                         cron_expression TEXT,
                                                         time_zone_id TEXT,
                                                         timeout_ticks INTEGER,
                                                         max_concurrency INTEGER,
                                                         priority INTEGER NOT NULL DEFAULT 0,
                                                         retry_policy TEXT,
                                                         is_continuous INTEGER NOT NULL DEFAULT 0,
                                                         queue TEXT,
                                                         rate_limit_name TEXT,
                                                         is_enabled INTEGER NOT NULL DEFAULT 1,
                                                         misfire_policy INTEGER NOT NULL DEFAULT 0,
                                                         fire_all_limit INTEGER,
                                                         arguments_schema TEXT,
                                                         last_heartbeat_at TEXT,
                                                         last_cron_fire_at TEXT
                                                     );
                                                     CREATE TABLE IF NOT EXISTS surefire_runs (
                                                         id TEXT PRIMARY KEY,
                                                         job_name TEXT NOT NULL,
                                                         status INTEGER NOT NULL DEFAULT 0,
                                                         arguments TEXT,
                                                         result TEXT,
                                                         error TEXT,
                                                         progress REAL NOT NULL DEFAULT 0,
                                                         created_at TEXT NOT NULL,
                                                         started_at TEXT,
                                                         completed_at TEXT,
                                                         cancelled_at TEXT,
                                                         node_name TEXT,
                                                         attempt INTEGER NOT NULL DEFAULT 0,
                                                         trace_id TEXT,
                                                         span_id TEXT,
                                                         parent_run_id TEXT,
                                                         root_run_id TEXT,
                                                         rerun_of_run_id TEXT,
                                                         not_before TEXT NOT NULL,
                                                         not_after TEXT,
                                                         priority INTEGER NOT NULL DEFAULT 0,
                                                         deduplication_id TEXT,
                                                         last_heartbeat_at TEXT,
                                                         batch_total INTEGER,
                                                         batch_completed INTEGER NOT NULL DEFAULT 0,
                                                         batch_failed INTEGER NOT NULL DEFAULT 0,
                                                         queue_priority INTEGER NOT NULL DEFAULT 0
                                                     );
                                                     CREATE INDEX IF NOT EXISTS ix_runs_claim
                                                         ON surefire_runs (queue_priority DESC, priority DESC, not_before, id) WHERE status = 0 AND batch_total IS NULL;
                                                     CREATE INDEX IF NOT EXISTS ix_runs_job_name_status
                                                         ON surefire_runs (job_name, status);
                                                     CREATE INDEX IF NOT EXISTS ix_runs_created_at
                                                         ON surefire_runs (created_at DESC);
                                                     CREATE INDEX IF NOT EXISTS ix_runs_parent_run_id
                                                         ON surefire_runs (parent_run_id);
                                                     CREATE INDEX IF NOT EXISTS ix_runs_root_run_id
                                                         ON surefire_runs (root_run_id);
                                                     CREATE INDEX IF NOT EXISTS ix_runs_node_name_status
                                                         ON surefire_runs (node_name, status);
                                                     CREATE UNIQUE INDEX IF NOT EXISTS ix_runs_dedup
                                                         ON surefire_runs (job_name, deduplication_id)
                                                         WHERE deduplication_id IS NOT NULL AND status NOT IN (2, 4, 5);
                                                     CREATE INDEX IF NOT EXISTS ix_runs_completed
                                                         ON surefire_runs (completed_at, id)
                                                         WHERE completed_at IS NOT NULL;
                                                     CREATE INDEX IF NOT EXISTS ix_runs_not_after
                                                         ON surefire_runs (not_after) WHERE status IN (0, 3) AND batch_total IS NULL AND not_after IS NOT NULL;
                                                     CREATE TABLE IF NOT EXISTS surefire_events (
                                                         id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                         run_id TEXT NOT NULL,
                                                         event_type INTEGER NOT NULL,
                                                         payload TEXT NOT NULL,
                                                         created_at TEXT NOT NULL,
                                                         attempt INTEGER NOT NULL DEFAULT 1,
                                                         FOREIGN KEY (run_id) REFERENCES surefire_runs(id) ON DELETE CASCADE
                                                     );
                                                     CREATE INDEX IF NOT EXISTS ix_events_run_id
                                                         ON surefire_events (run_id, id);
                                                     CREATE TABLE IF NOT EXISTS surefire_nodes (
                                                         name TEXT PRIMARY KEY,
                                                         started_at TEXT NOT NULL,
                                                         last_heartbeat_at TEXT NOT NULL,
                                                         running_count INTEGER NOT NULL DEFAULT 0,
                                                         registered_job_names TEXT NOT NULL DEFAULT '[]',
                                                         registered_queue_names TEXT NOT NULL DEFAULT '[]'
                                                     );
                                                     CREATE TABLE IF NOT EXISTS surefire_queues (
                                                         name TEXT PRIMARY KEY,
                                                         priority INTEGER NOT NULL DEFAULT 0,
                                                         max_concurrency INTEGER,
                                                         is_paused INTEGER NOT NULL DEFAULT 0,
                                                         rate_limit_name TEXT,
                                                         last_heartbeat_at TEXT
                                                     );
                                                     CREATE TABLE IF NOT EXISTS surefire_rate_limits (
                                                         name TEXT PRIMARY KEY,
                                                         type INTEGER NOT NULL DEFAULT 0,
                                                         max_permits INTEGER NOT NULL,
                                                         "window" INTEGER NOT NULL,
                                                         current_count INTEGER NOT NULL DEFAULT 0,
                                                         previous_count INTEGER NOT NULL DEFAULT 0,
                                                         window_start TEXT,
                                                         last_heartbeat_at TEXT
                                                     );
                                                     INSERT OR IGNORE INTO surefire_schema_migrations (version) VALUES (1);
                                                     INSERT OR IGNORE INTO surefire_schema_migrations (version) VALUES (2);
                                                     """);
            await ddlCmd.ExecuteNonQueryAsync(cancellationToken);
            return;
        }

        if (!hasV2)
        {
            var hasTimeoutTicks = await HasColumnAsync(conn, "surefire_jobs", "timeout_ticks", cancellationToken);
            if (!hasTimeoutTicks)
            {
                await using var alterCmd = CreateCommand(conn,
                    "ALTER TABLE surefire_jobs ADD COLUMN timeout_ticks INTEGER;");
                await alterCmd.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var migrateDataCmd = CreateCommand(conn, """
                                                                  UPDATE surefire_jobs
                                                                  SET timeout_ticks = CASE
                                                                      WHEN timeout_ticks IS NOT NULL THEN timeout_ticks
                                                                      WHEN timeout_ms IS NULL THEN NULL
                                                                      ELSE CAST(ROUND(timeout_ms * 10000.0) AS INTEGER)
                                                                  END
                                                                  """))
            {
                await migrateDataCmd.ExecuteNonQueryAsync(cancellationToken);
            }

            await using var versionCmd = CreateCommand(conn,
                "INSERT OR IGNORE INTO surefire_schema_migrations (version) VALUES (2);");
            await versionCmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    public async Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  INSERT INTO surefire_jobs (
                                                      name, description, tags, cron_expression, time_zone_id,
                                                      timeout_ticks, max_concurrency, priority, retry_policy,
                                                      is_continuous, queue, rate_limit_name, is_enabled,
                                                      misfire_policy, fire_all_limit, arguments_schema, last_heartbeat_at
                                                  ) VALUES (
                                                      @name, @description, @tags, @cron_expression, @time_zone_id,
                                                      @timeout_ticks, @max_concurrency, @priority, @retry_policy,
                                                      @is_continuous, @queue, @rate_limit_name, @is_enabled,
                                                      @misfire_policy, @fire_all_limit, @arguments_schema, @last_heartbeat_at
                                                  )
                                                  ON CONFLICT (name) DO UPDATE SET
                                                      description = excluded.description,
                                                      tags = excluded.tags,
                                                      cron_expression = excluded.cron_expression,
                                                      time_zone_id = excluded.time_zone_id,
                                                      timeout_ticks = excluded.timeout_ticks,
                                                      max_concurrency = excluded.max_concurrency,
                                                      priority = excluded.priority,
                                                      retry_policy = excluded.retry_policy,
                                                      is_continuous = excluded.is_continuous,
                                                      queue = excluded.queue,
                                                      rate_limit_name = excluded.rate_limit_name,
                                                      misfire_policy = excluded.misfire_policy,
                                                      fire_all_limit = excluded.fire_all_limit,
                                                      arguments_schema = excluded.arguments_schema,
                                                      last_heartbeat_at = @last_heartbeat_at
                                                  """);
        AddJobParameters(cmd, job);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  SELECT * FROM surefire_jobs WHERE name = @name
                                                  """);
        cmd.Parameters.AddWithValue("@name", name);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadJob(reader) : null;
    }

    public async Task<IReadOnlyList<JobDefinition>> GetJobsAsync(
        JobListFilter? filter = null, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        var sql = "SELECT * FROM surefire_jobs WHERE 1=1";
        await using var cmd = new SqliteCommand { Connection = conn };

        if (filter?.Name is { })
        {
            sql += " AND name LIKE @name ESCAPE '\\'";
            cmd.Parameters.AddWithValue("@name", $"%{EscapeLike(filter.Name)}%");
        }

        if (filter?.Tag is { })
        {
            sql += " AND EXISTS (SELECT 1 FROM json_each(tags) WHERE LOWER(value) = LOWER(@tag))";
            cmd.Parameters.AddWithValue("@tag", filter.Tag);
        }

        if (filter?.IsEnabled is { })
        {
            sql += " AND is_enabled = @ie";
            cmd.Parameters.AddWithValue("@ie", filter.IsEnabled.Value ? 1 : 0);
        }

        if (filter?.HeartbeatAfter is { })
        {
            sql += " AND last_heartbeat_at IS NOT NULL AND last_heartbeat_at > @ha";
            cmd.Parameters.AddWithValue("@ha", FormatTimestamp(filter.HeartbeatAfter.Value));
        }

        cmd.CommandText = sql + " ORDER BY name";
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var jobs = new List<JobDefinition>();
        while (await reader.ReadAsync(cancellationToken))
        {
            jobs.Add(ReadJob(reader));
        }

        return jobs;
    }

    public async Task SetJobEnabledAsync(
        string name, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  UPDATE surefire_jobs SET is_enabled = @e WHERE name = @n
                                                  """);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@e", enabled ? 1 : 0);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateLastCronFireAtAsync(
        string jobName, DateTimeOffset fireAt, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  UPDATE surefire_jobs SET last_cron_fire_at = @f WHERE name = @n
                                                  """);
        cmd.Parameters.AddWithValue("@n", jobName);
        cmd.Parameters.AddWithValue("@f", FormatTimestamp(fireAt));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public Task CreateRunsAsync(
        IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
        => CreateRunsCoreAsync(runs, initialEvents, cancellationToken);

    public Task<bool> TryCreateRunAsync(
        JobRun run, int? maxActiveForJob = null,
        DateTimeOffset? lastCronFireAt = null,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
        => TryCreateRunCoreAsync(run, maxActiveForJob, lastCronFireAt, initialEvents, cancellationToken);

    public async Task<JobRun?> GetRunAsync(string id, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  SELECT * FROM surefire_runs WHERE id = @id
                                                  """);
        cmd.Parameters.AddWithValue("@id", id);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadRun(reader) : null;
    }

    public async Task<PagedResult<JobRun>> GetRunsAsync(
        RunFilter filter, int skip = 0, int take = 50, CancellationToken cancellationToken = default)
    {
        if (skip < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(skip));
        }

        if (take <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(take));
        }

        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(true);

        var where = "WHERE 1=1";
        await using var cmd = new SqliteCommand { Connection = conn, Transaction = tx };

        if (filter.Status is { })
        {
            where += " AND status = @st";
            cmd.Parameters.AddWithValue("@st", (int)filter.Status.Value);
        }

        if (filter.JobName is { })
        {
            if (filter.ExactJobName)
            {
                where += " AND job_name = @jn";
                cmd.Parameters.AddWithValue("@jn", filter.JobName);
            }
            else
            {
                where += " AND job_name LIKE @jn ESCAPE '\\'";
                cmd.Parameters.AddWithValue("@jn", $"%{EscapeLike(filter.JobName)}%");
            }
        }

        if (filter.ParentRunId is { })
        {
            where += " AND parent_run_id = @pr";
            cmd.Parameters.AddWithValue("@pr", filter.ParentRunId);
        }

        if (filter.RootRunId is { })
        {
            where += " AND root_run_id = @rr";
            cmd.Parameters.AddWithValue("@rr", filter.RootRunId);
        }

        if (filter.NodeName is { })
        {
            where += " AND node_name = @nn";
            cmd.Parameters.AddWithValue("@nn", filter.NodeName);
        }

        if (filter.CreatedAfter is { })
        {
            where += " AND created_at > @ca";
            cmd.Parameters.AddWithValue("@ca", FormatTimestamp(filter.CreatedAfter.Value));
        }

        if (filter.CreatedBefore is { })
        {
            where += " AND created_at < @cb";
            cmd.Parameters.AddWithValue("@cb", FormatTimestamp(filter.CreatedBefore.Value));
        }

        if (filter.CompletedAfter is { })
        {
            where += " AND completed_at >= @coa";
            cmd.Parameters.AddWithValue("@coa", FormatTimestamp(filter.CompletedAfter.Value));
        }

        if (filter.LastHeartbeatBefore is { })
        {
            where += " AND last_heartbeat_at < @lhb";
            cmd.Parameters.AddWithValue("@lhb", FormatTimestamp(filter.LastHeartbeatBefore.Value));
        }

        if (filter.IsBatchCoordinator is { })
        {
            where += filter.IsBatchCoordinator.Value
                ? " AND batch_total IS NOT NULL"
                : " AND batch_total IS NULL";
        }

        if (filter.IsTerminal is { })
        {
            where += filter.IsTerminal.Value
                ? " AND status IN (2, 4, 5)"
                : " AND status NOT IN (2, 4, 5)";
        }

        var orderColumn = filter.OrderBy switch
        {
            RunOrderBy.StartedAt => "started_at",
            RunOrderBy.CompletedAt => "completed_at",
            _ => "created_at"
        };

        cmd.CommandText = $"SELECT COUNT(*) FROM surefire_runs {where}";
        var totalCount = Convert.ToInt32(await cmd.ExecuteScalarAsync(cancellationToken));

        cmd.CommandText = $"""
                           SELECT * FROM surefire_runs {where}
                           ORDER BY {orderColumn} DESC, id DESC
                           LIMIT @tk OFFSET @sk
                           """;
        cmd.Parameters.AddWithValue("@sk", skip);
        cmd.Parameters.AddWithValue("@tk", take);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var items = new List<JobRun>();
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(ReadRun(reader));
        }

        await tx.CommitAsync(cancellationToken);
        return new() { Items = items, TotalCount = totalCount };
    }

    public async Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  UPDATE surefire_runs
                                                  SET progress = @p, result = @r, error = @e,
                                                      trace_id = @ti, span_id = @si, last_heartbeat_at = @lh
                                                  WHERE id = @id AND node_name IS @nn AND status NOT IN (2, 4, 5)
                                                  """);
        cmd.Parameters.AddWithValue("@id", run.Id);
        cmd.Parameters.AddWithValue("@p", run.Progress);
        cmd.Parameters.AddWithValue("@r", (object?)run.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@e", (object?)run.Error ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ti", (object?)run.TraceId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@si", (object?)run.SpanId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@lh", FormatNullableTimestamp(run.LastHeartbeatAt));
        cmd.Parameters.AddWithValue("@nn", (object?)run.NodeName ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<bool> TryTransitionRunAsync(
        RunStatusTransition transition, CancellationToken cancellationToken = default)
    {
        if (!RunTransitionRules.IsAllowed(transition.ExpectedStatus, transition.NewStatus)
            || !transition.HasRequiredFields())
        {
            return false;
        }

        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  UPDATE surefire_runs
                                                  SET status = @s, error = @e, result = @r, progress = @p,
                                                      completed_at = COALESCE(@coa, completed_at),
                                                      cancelled_at = COALESCE(@caa, cancelled_at),
                                                      started_at = COALESCE(@sa, started_at),
                                                      node_name = @nn,
                                                      last_heartbeat_at = COALESCE(@lh, last_heartbeat_at),
                                                      not_before = @nb
                                                  WHERE id = @id AND status = @es AND attempt = @ea
                                                      AND status NOT IN (2, 4, 5)
                                                  """);
        cmd.Parameters.AddWithValue("@id", transition.RunId);
        cmd.Parameters.AddWithValue("@s", (int)transition.NewStatus);
        cmd.Parameters.AddWithValue("@e", (object?)transition.Error ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@r", (object?)transition.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@p", transition.Progress);
        cmd.Parameters.AddWithValue("@coa", FormatNullableTimestamp(transition.CompletedAt));
        cmd.Parameters.AddWithValue("@caa", FormatNullableTimestamp(transition.CancelledAt));
        cmd.Parameters.AddWithValue("@sa", FormatNullableTimestamp(transition.StartedAt));
        cmd.Parameters.AddWithValue("@nn", (object?)transition.NodeName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@lh", FormatNullableTimestamp(transition.LastHeartbeatAt));
        cmd.Parameters.AddWithValue("@nb", FormatTimestamp(transition.NotBefore));
        cmd.Parameters.AddWithValue("@es", (int)transition.ExpectedStatus);
        cmd.Parameters.AddWithValue("@ea", transition.ExpectedAttempt);
        return await cmd.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    public async Task<bool> TryCancelRunAsync(
        string runId, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  UPDATE surefire_runs
                                                  SET status = 4, cancelled_at = @c, completed_at = @c
                                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                                  """);
        cmd.Parameters.AddWithValue("@id", runId);
        cmd.Parameters.AddWithValue("@c", FormatTimestamp(timeProvider.GetUtcNow()));
        return await cmd.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    public async Task<JobRun?> ClaimRunAsync(
        string nodeName,
        IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames,
        CancellationToken cancellationToken = default)
    {
        if (jobNames.Count == 0 || queueNames.Count == 0)
        {
            return null;
        }

        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);
        var now = FormatTimestamp(timeProvider.GetUtcNow());
        var jobNameParams = BuildInClause("@jn", jobNames, out var jobNameClause);
        var queueNameParams = BuildInClause("@qn", queueNames, out var queueNameClause);

        var claimSql = $"""
                        SELECT r.id, r.job_name
                        FROM surefire_runs r
                        JOIN surefire_jobs j ON j.name = r.job_name
                        LEFT JOIN surefire_queues q ON q.name = COALESCE(j.queue, 'default')
                        WHERE r.status = 0
                            AND r.batch_total IS NULL
                            AND r.not_before <= @now
                            AND (r.not_after IS NULL OR r.not_after > @now)
                            AND r.job_name IN ({jobNameClause})
                            AND COALESCE(j.queue, 'default') IN ({queueNameClause})
                            AND COALESCE(q.is_paused, 0) = 0
                            AND (j.max_concurrency IS NULL
                                OR (SELECT COUNT(*) FROM surefire_runs r2
                                    WHERE r2.job_name = r.job_name AND r2.status = 1
                                   ) < j.max_concurrency)
                            AND (q.max_concurrency IS NULL
                                OR (SELECT COUNT(*) FROM surefire_runs r2
                                    JOIN surefire_jobs j2 ON j2.name = r2.job_name
                                    WHERE r2.status = 1
                                        AND COALESCE(j2.queue, 'default') = COALESCE(j.queue, 'default')
                                   ) < q.max_concurrency)
                            AND (j.rate_limit_name IS NULL
                                OR NOT EXISTS (
                                    SELECT 1 FROM surefire_rate_limits WHERE name = j.rate_limit_name)
                                OR EXISTS (
                                    SELECT 1 FROM surefire_rate_limits rl
                                    WHERE rl.name = j.rate_limit_name
                                        AND CASE
                                            WHEN rl.window_start IS NULL THEN 1
                                            WHEN (julianday(@now) - julianday(rl.window_start))
                                                * 864000000000.0 >= CAST(rl."window" AS REAL) * 2
                                            THEN 1
                                            WHEN rl.type = 1 AND julianday(rl.window_start)
                                                + CAST(rl."window" AS REAL) / 864000000000.0
                                                <= julianday(@now)
                                            THEN rl.current_count
                                                * MAX(0, 1.0 - ((julianday(@now) - julianday(rl.window_start))
                                                    * 864000000000.0 - CAST(rl."window" AS REAL))
                                                    / CAST(rl."window" AS REAL))
                                                < rl.max_permits
                                            WHEN julianday(rl.window_start)
                                                + CAST(rl."window" AS REAL) / 864000000000.0
                                                <= julianday(@now)
                                            THEN 1
                                            WHEN rl.type = 0
                                            THEN rl.current_count < rl.max_permits
                                            WHEN rl.type = 1
                                            THEN (COALESCE(rl.previous_count, 0)
                                                * MAX(0, 1.0 - (julianday(@now)
                                                    - julianday(rl.window_start))
                                                    * 864000000000.0
                                                    / CAST(rl."window" AS REAL))
                                                + rl.current_count) < rl.max_permits
                                            ELSE rl.current_count < rl.max_permits
                                        END = 1))
                            AND (q.rate_limit_name IS NULL
                                OR NOT EXISTS (
                                    SELECT 1 FROM surefire_rate_limits WHERE name = q.rate_limit_name)
                                OR EXISTS (
                                    SELECT 1 FROM surefire_rate_limits rl
                                    WHERE rl.name = q.rate_limit_name
                                        AND CASE
                                            WHEN rl.window_start IS NULL THEN 1
                                            WHEN (julianday(@now) - julianday(rl.window_start))
                                                * 864000000000.0 >= CAST(rl."window" AS REAL) * 2
                                            THEN 1
                                            WHEN rl.type = 1 AND julianday(rl.window_start)
                                                + CAST(rl."window" AS REAL) / 864000000000.0
                                                <= julianday(@now)
                                            THEN rl.current_count
                                                * MAX(0, 1.0 - ((julianday(@now) - julianday(rl.window_start))
                                                    * 864000000000.0 - CAST(rl."window" AS REAL))
                                                    / CAST(rl."window" AS REAL))
                                                < rl.max_permits
                                            WHEN julianday(rl.window_start)
                                                + CAST(rl."window" AS REAL) / 864000000000.0
                                                <= julianday(@now)
                                            THEN 1
                                            WHEN rl.type = 0
                                            THEN rl.current_count < rl.max_permits
                                            WHEN rl.type = 1
                                            THEN (COALESCE(rl.previous_count, 0)
                                                * MAX(0, 1.0 - (julianday(@now)
                                                    - julianday(rl.window_start))
                                                    * 864000000000.0
                                                    / CAST(rl."window" AS REAL))
                                                + rl.current_count) < rl.max_permits
                                            ELSE rl.current_count < rl.max_permits
                                        END = 1))
                        ORDER BY r.queue_priority DESC, r.priority DESC,
                            r.not_before ASC, r.id ASC
                        LIMIT 1
                        """;

        string? runId = null, jobName = null;
        await using (var selectCmd = CreateCommand(conn, claimSql, tx))
        {
            selectCmd.Parameters.AddWithValue("@now", now);
            foreach (var p in jobNameParams)
            {
                selectCmd.Parameters.Add(p);
            }

            foreach (var p in queueNameParams)
            {
                selectCmd.Parameters.Add(p);
            }

            await using var reader = await selectCmd.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                runId = reader.GetString(0);
                jobName = reader.GetString(1);
            }
        }

        if (runId is null)
        {
            await tx.CommitAsync(cancellationToken);
            return null;
        }

        await using (var updateCmd = CreateCommand(conn, """
                                                         UPDATE surefire_runs
                                                         SET status = 1, node_name = @nn, started_at = @now,
                                                             last_heartbeat_at = @now, attempt = attempt + 1
                                                         WHERE id = @id AND status = 0
                                                         """, tx))
        {
            updateCmd.Parameters.AddWithValue("@id", runId);
            updateCmd.Parameters.AddWithValue("@nn", nodeName);
            updateCmd.Parameters.AddWithValue("@now", now);
            if (await updateCmd.ExecuteNonQueryAsync(cancellationToken) == 0)
            {
                await tx.RollbackAsync(cancellationToken);
                return null;
            }
        }

        await AcquireRateLimitAsync(conn,
            "SELECT rate_limit_name FROM surefire_jobs WHERE name = @name",
            [new("@name", jobName)], now, tx, cancellationToken);

        await AcquireRateLimitAsync(conn,
            """
            SELECT q.rate_limit_name
            FROM surefire_jobs j
            LEFT JOIN surefire_queues q ON q.name = COALESCE(j.queue, 'default')
            WHERE j.name = @name
                AND q.rate_limit_name IS NOT NULL
                AND q.rate_limit_name != COALESCE(j.rate_limit_name, '')
            """,
            [new("@name", jobName)], now, tx, cancellationToken);

        JobRun? result;
        await using (var readCmd = CreateCommand(conn, """
                                                       SELECT * FROM surefire_runs WHERE id = @id
                                                       """, tx))
        {
            readCmd.Parameters.AddWithValue("@id", runId);
            await using var reader = await readCmd.ExecuteReaderAsync(cancellationToken);
            result = await reader.ReadAsync(cancellationToken) ? ReadRun(reader) : null;
        }

        await tx.CommitAsync(cancellationToken);
        return result;
    }

    public async Task<BatchCounters?> TryIncrementBatchCounterAsync(
        string batchRunId, bool isFailed, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  UPDATE surefire_runs
                                                  SET batch_completed = batch_completed + CASE WHEN @f THEN 0 ELSE 1 END,
                                                      batch_failed = batch_failed + CASE WHEN @f THEN 1 ELSE 0 END,
                                                      progress = CASE WHEN batch_total = 0 THEN 1.0 ELSE CAST((
                                                          batch_completed + CASE WHEN @f THEN 0 ELSE 1 END
                                                          + batch_failed + CASE WHEN @f THEN 1 ELSE 0 END
                                                      ) AS REAL) / batch_total END
                                                  WHERE id = @id AND status NOT IN (2, 4, 5) AND batch_total IS NOT NULL
                                                  RETURNING batch_total, batch_completed, batch_failed
                                                  """);
        cmd.Parameters.AddWithValue("@id", batchRunId);
        cmd.Parameters.AddWithValue("@f", isFailed ? 1 : 0);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        if (await reader.ReadAsync(cancellationToken))
        {
            return new(reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2));
        }

        await using var fallbackCmd = CreateCommand(conn, """
                                                          SELECT batch_total, batch_completed, batch_failed
                                                          FROM surefire_runs
                                                          WHERE id = @id
                                                          """);
        fallbackCmd.Parameters.AddWithValue("@id", batchRunId);
        await using var fallbackReader = await fallbackCmd.ExecuteReaderAsync(cancellationToken);

        if (!await fallbackReader.ReadAsync(cancellationToken))
        {
            return null;
        }

        if (fallbackReader.IsDBNull(0))
        {
            return null;
        }

        return new(
            fallbackReader.GetInt32(0),
            fallbackReader.IsDBNull(1) ? 0 : fallbackReader.GetInt32(1),
            fallbackReader.IsDBNull(2) ? 0 : fallbackReader.GetInt32(2));
    }

    public async Task AppendEventsAsync(
        IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default)
    {
        if (events.Count == 0)
        {
            return;
        }

        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);

        await InsertEventsAsync(conn, events, cancellationToken, tx);

        await tx.CommitAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<RunEvent>> GetEventsAsync(
        string runId,
        long sinceId = 0,
        RunEventType[]? types = null,
        int? attempt = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        var sql = """
                  SELECT id, run_id, event_type, payload, created_at, attempt
                  FROM surefire_events
                  WHERE run_id = @ri AND id > @si
                  """;
        await using var cmd = new SqliteCommand { Connection = conn };
        cmd.Parameters.AddWithValue("@ri", runId);
        cmd.Parameters.AddWithValue("@si", sinceId);

        if (types is { Length: > 0 })
        {
            var typeParams = BuildInClause("@et", types.Select(t => (int)t), out var typeClause);
            sql += $" AND event_type IN ({typeClause})";
            foreach (var p in typeParams)
            {
                cmd.Parameters.Add(p);
            }
        }

        if (attempt is { })
        {
            sql += " AND (attempt = @att OR attempt = 0)";
            cmd.Parameters.AddWithValue("@att", attempt.Value);
        }

        cmd.CommandText = sql + " ORDER BY id";
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var result = new List<RunEvent>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new()
            {
                Id = reader.GetInt64(0),
                RunId = reader.GetString(1),
                EventType = (RunEventType)reader.GetInt16(2),
                Payload = reader.GetString(3),
                CreatedAt = ParseTimestamp(reader.GetString(4)),
                Attempt = reader.GetInt32(5)
            });
        }

        return result;
    }

    public async Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchRunId, long sinceEventId = 0,
        int take = 200, CancellationToken cancellationToken = default)
    {
        if (take <= 0)
        {
            return [];
        }

        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  SELECT e.id, e.run_id, e.event_type, e.payload, e.created_at, e.attempt
                                                  FROM surefire_events e
                                                  JOIN surefire_runs r ON r.id = e.run_id
                                                  WHERE r.parent_run_id = @batch_id
                                                      AND e.event_type = @event_type
                                                      AND e.id > @since_id
                                                  ORDER BY e.id
                                                  LIMIT @take
                                                  """);
        cmd.Parameters.AddWithValue("@batch_id", batchRunId);
        cmd.Parameters.AddWithValue("@event_type", (int)RunEventType.Output);
        cmd.Parameters.AddWithValue("@since_id", sinceEventId);
        cmd.Parameters.AddWithValue("@take", take);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var result = new List<RunEvent>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new()
            {
                Id = reader.GetInt64(0),
                RunId = reader.GetString(1),
                EventType = (RunEventType)reader.GetInt16(2),
                Payload = reader.GetString(3),
                CreatedAt = ParseTimestamp(reader.GetString(4)),
                Attempt = reader.GetInt32(5)
            });
        }

        return result;
    }

    public async Task HeartbeatAsync(
        string nodeName,
        IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames,
        IReadOnlyCollection<string> activeRunIds,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);
        var now = FormatTimestamp(timeProvider.GetUtcNow());

        await using (var nodeCmd = CreateCommand(conn, """
                                                       INSERT INTO surefire_nodes (
                                                           name, started_at, last_heartbeat_at, running_count,
                                                           registered_job_names, registered_queue_names
                                                       ) VALUES (@n, @now, @now, @rc, @rjn, @rqn)
                                                       ON CONFLICT (name) DO UPDATE SET
                                                           last_heartbeat_at = @now,
                                                           running_count = @rc,
                                                           registered_job_names = @rjn,
                                                           registered_queue_names = @rqn
                                                       """, tx))
        {
            nodeCmd.Parameters.AddWithValue("@n", nodeName);
            nodeCmd.Parameters.AddWithValue("@now", now);
            nodeCmd.Parameters.AddWithValue("@rc", activeRunIds.Count);
            nodeCmd.Parameters.AddWithValue("@rjn", JsonSerializer.Serialize(jobNames));
            nodeCmd.Parameters.AddWithValue("@rqn", JsonSerializer.Serialize(queueNames));
            await nodeCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        if (activeRunIds.Count > 0)
        {
            const int chunkSize = 900;
            var idList = activeRunIds as List<string> ?? activeRunIds.ToList();

            for (var offset = 0; offset < idList.Count; offset += chunkSize)
            {
                var count = Math.Min(chunkSize, idList.Count - offset);
                var chunk = idList.GetRange(offset, count);
                var idParams = BuildInClause("@rid", chunk, out var idClause);
                await using var runCmd = CreateCommand(conn, $"""
                                                              UPDATE surefire_runs
                                                              SET last_heartbeat_at = @now
                                                              WHERE id IN ({idClause}) AND node_name = @node AND status NOT IN (2, 4, 5)
                                                              """, tx);
                runCmd.Parameters.AddWithValue("@now", now);
                runCmd.Parameters.AddWithValue("@node", nodeName);
                foreach (var p in idParams)
                {
                    runCmd.Parameters.Add(p);
                }

                await runCmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        await tx.CommitAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<NodeInfo>> GetNodesAsync(
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  SELECT * FROM surefire_nodes ORDER BY name
                                                  """);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var nodes = new List<NodeInfo>();
        while (await reader.ReadAsync(cancellationToken))
        {
            nodes.Add(ReadNode(reader));
        }

        return nodes;
    }

    public async Task<NodeInfo?> GetNodeAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  SELECT * FROM surefire_nodes WHERE name = @name LIMIT 1
                                                  """);
        cmd.Parameters.AddWithValue("@name", name);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return ReadNode(reader);
        }

        return null;
    }

    public async Task UpsertQueueAsync(
        QueueDefinition queue, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);

        await using (var cmd = CreateCommand(conn, """
                                                   INSERT INTO surefire_queues (
                                                       name, priority, max_concurrency, is_paused,
                                                       rate_limit_name, last_heartbeat_at
                                                   ) VALUES (@n, @pr, @mc, @ip, @rln, @lh)
                                                   ON CONFLICT (name) DO UPDATE SET
                                                       priority = excluded.priority,
                                                       max_concurrency = excluded.max_concurrency,
                                                       rate_limit_name = excluded.rate_limit_name,
                                                       last_heartbeat_at = @lh
                                                   """, tx))
        {
            cmd.Parameters.AddWithValue("@n", queue.Name);
            cmd.Parameters.AddWithValue("@pr", queue.Priority);
            cmd.Parameters.AddWithValue("@mc",
                queue.MaxConcurrency.HasValue ? queue.MaxConcurrency.Value : DBNull.Value);
            cmd.Parameters.AddWithValue("@ip", queue.IsPaused ? 1 : 0);
            cmd.Parameters.AddWithValue("@rln", (object?)queue.RateLimitName ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@lh", FormatTimestamp(timeProvider.GetUtcNow()));
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var updateCmd = CreateCommand(conn, """
                                                         UPDATE surefire_runs
                                                         SET queue_priority = @pr
                                                         WHERE status = 0 AND batch_total IS NULL
                                                             AND job_name IN (
                                                                 SELECT name FROM surefire_jobs
                                                                 WHERE COALESCE(queue, 'default') = @n
                                                             )
                                                             AND queue_priority != @pr
                                                         """, tx))
        {
            updateCmd.Parameters.AddWithValue("@pr", queue.Priority);
            updateCmd.Parameters.AddWithValue("@n", queue.Name);
            await updateCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await tx.CommitAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  SELECT * FROM surefire_queues ORDER BY name
                                                  """);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var queues = new List<QueueDefinition>();
        while (await reader.ReadAsync(cancellationToken))
        {
            queues.Add(ReadQueue(reader));
        }

        return queues;
    }

    public async Task SetQueuePausedAsync(
        string name, bool isPaused, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  UPDATE surefire_queues SET is_paused = @ip WHERE name = @n
                                                  """);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@ip", isPaused ? 1 : 0);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpsertRateLimitAsync(
        RateLimitDefinition rateLimit, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        var now = FormatTimestamp(timeProvider.GetUtcNow());
        await using var cmd = CreateCommand(conn, """
                                                  INSERT INTO surefire_rate_limits (
                                                      name, type, max_permits, "window", last_heartbeat_at
                                                  ) VALUES (@n, @t, @mp, @w, @lh)
                                                  ON CONFLICT (name) DO UPDATE SET
                                                      type = excluded.type,
                                                      max_permits = excluded.max_permits,
                                                      "window" = excluded."window",
                                                      last_heartbeat_at = @lh
                                                  """);
        cmd.Parameters.AddWithValue("@n", rateLimit.Name);
        cmd.Parameters.AddWithValue("@t", (int)rateLimit.Type);
        cmd.Parameters.AddWithValue("@mp", rateLimit.MaxPermits);
        cmd.Parameters.AddWithValue("@w", rateLimit.Window.Ticks);
        cmd.Parameters.AddWithValue("@lh", now);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> CancelExpiredRunsAsync(CancellationToken cancellationToken = default)
        => (await CancelExpiredRunsWithIdsAsync(cancellationToken)).Count;

    public async Task<IReadOnlyList<string>> CancelExpiredRunsWithIdsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        var now = FormatTimestamp(timeProvider.GetUtcNow());
        await using var tx = conn.BeginTransaction();

        var cancelledIds = new List<string>();

        await using (var selectCmd = CreateCommand(conn, """
                                                         SELECT id
                                                         FROM surefire_runs
                                                         WHERE status IN (0, 3)
                                                             AND batch_total IS NULL
                                                             AND not_after IS NOT NULL
                                                             AND not_after < @now
                                                         """, tx))
        {
            selectCmd.Parameters.AddWithValue("@now", now);
            await using var reader = await selectCmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                cancelledIds.Add(reader.GetString(0));
            }
        }

        if (cancelledIds.Count == 0)
        {
            await tx.CommitAsync(cancellationToken);
            return cancelledIds;
        }

        await using (var updateCmd = CreateCommand(conn, """
                                                         UPDATE surefire_runs
                                                         SET status = 4,
                                                             cancelled_at = @now,
                                                             completed_at = @now
                                                         WHERE status IN (0, 3)
                                                             AND batch_total IS NULL
                                                             AND not_after IS NOT NULL
                                                             AND not_after < @now
                                                         """, tx))
        {
            updateCmd.Parameters.AddWithValue("@now", now);
            await updateCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await tx.CommitAsync(cancellationToken);
        return cancelledIds;
    }

    public async Task PurgeAsync(
        DateTimeOffset threshold, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        var thresholdStr = FormatTimestamp(threshold);

        while (true)
        {
            await using var tx = conn.BeginTransaction(false);
            await using var cmd = CreateCommand(conn, """
                                                      DELETE FROM surefire_runs WHERE rowid IN (
                                                          SELECT rowid FROM surefire_runs
                                                          WHERE (status IN (2, 4, 5)
                                                              AND completed_at < @b
                                                              AND NOT EXISTS (
                                                                  SELECT 1
                                                                  FROM surefire_runs parent
                                                                  WHERE parent.id = surefire_runs.parent_run_id
                                                                      AND parent.batch_total IS NOT NULL
                                                                      AND (
                                                                          parent.status NOT IN (2, 4, 5)
                                                                          OR parent.completed_at IS NULL
                                                                          OR parent.completed_at >= @b
                                                                      )
                                                              ))
                                                              OR (status IN (0, 3) AND not_before < @b)
                                                          LIMIT 1000
                                                      )
                                                      """, tx);
            cmd.Parameters.AddWithValue("@b", thresholdStr);
            var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken);
            await tx.CommitAsync(cancellationToken);
            if (deleted < 1000)
            {
                break;
            }
        }

        await using var metaTx = conn.BeginTransaction(false);

        await ExecuteWithThresholdAsync(conn, """
                                              DELETE FROM surefire_jobs
                                              WHERE last_heartbeat_at IS NOT NULL AND last_heartbeat_at < @b
                                                  AND NOT EXISTS (SELECT 1 FROM surefire_runs r WHERE r.job_name = surefire_jobs.name AND r.status NOT IN (2, 4, 5))
                                              """, thresholdStr, metaTx, cancellationToken);

        await ExecuteWithThresholdAsync(conn, """
                                              DELETE FROM surefire_queues
                                              WHERE last_heartbeat_at IS NOT NULL AND last_heartbeat_at < @b
                                              """, thresholdStr, metaTx, cancellationToken);

        await ExecuteWithThresholdAsync(conn, """
                                              DELETE FROM surefire_rate_limits
                                              WHERE last_heartbeat_at IS NOT NULL AND last_heartbeat_at < @b
                                              """, thresholdStr, metaTx, cancellationToken);

        await ExecuteWithThresholdAsync(conn, """
                                              DELETE FROM surefire_nodes WHERE last_heartbeat_at < @b
                                              """, thresholdStr, metaTx, cancellationToken);

        await metaTx.CommitAsync(cancellationToken);
    }

    public async Task<DashboardStats> GetDashboardStatsAsync(
        DateTimeOffset? since = null,
        int bucketMinutes = 60,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(true);

        if (bucketMinutes <= 0)
        {
            bucketMinutes = 60;
        }

        var now = timeProvider.GetUtcNow();
        var sinceTime = since ?? now.AddHours(-24);

        await using var statsCmd = CreateCommand(conn, """
                                                       SELECT
                                                           (SELECT COUNT(*) FROM surefire_jobs),
                                                           (SELECT COUNT(*) FROM surefire_nodes WHERE last_heartbeat_at >= @node_threshold),
                                                           COUNT(*),
                                                           COALESCE(SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END), 0),
                                                           COALESCE(SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END), 0),
                                                           COALESCE(SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END), 0),
                                                           COALESCE(SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END), 0),
                                                           COALESCE(SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END), 0),
                                                           COALESCE(SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END), 0)
                                                       FROM surefire_runs
                                                       WHERE created_at >= @since AND created_at < @now
                                                       """, tx);
        statsCmd.Parameters.AddWithValue("@node_threshold", FormatTimestamp(now.AddMinutes(-2)));
        statsCmd.Parameters.AddWithValue("@since", FormatTimestamp(sinceTime));
        statsCmd.Parameters.AddWithValue("@now", FormatTimestamp(now));

        var runsByStatus = new Dictionary<string, int>();
        int totalJobs, nodeCount, totalRuns, activeRuns;
        double successRate;

        await using (var reader = await statsCmd.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            totalJobs = Convert.ToInt32(reader.GetValue(0));
            nodeCount = Convert.ToInt32(reader.GetValue(1));
            totalRuns = Convert.ToInt32(reader.GetValue(2));
            var pending = Convert.ToInt32(reader.GetValue(3));
            var running = Convert.ToInt32(reader.GetValue(4));
            var completed = Convert.ToInt32(reader.GetValue(5));
            var retrying = Convert.ToInt32(reader.GetValue(6));
            var cancelled = Convert.ToInt32(reader.GetValue(7));
            var deadLetter = Convert.ToInt32(reader.GetValue(8));

            activeRuns = pending + running + retrying;
            var terminalCount = completed + cancelled + deadLetter;
            successRate = terminalCount > 0 ? completed / (double)terminalCount : 0.0;

            if (pending > 0)
            {
                runsByStatus["Pending"] = pending;
            }

            if (running > 0)
            {
                runsByStatus["Running"] = running;
            }

            if (completed > 0)
            {
                runsByStatus["Completed"] = completed;
            }

            if (retrying > 0)
            {
                runsByStatus["Retrying"] = retrying;
            }

            if (cancelled > 0)
            {
                runsByStatus["Cancelled"] = cancelled;
            }

            if (deadLetter > 0)
            {
                runsByStatus["DeadLetter"] = deadLetter;
            }
        }

        var bucketSeconds = bucketMinutes * 60;
        var bucketSize = TimeSpan.FromMinutes(bucketMinutes);
        var bucketData =
            new Dictionary<int, (int Pending, int Running, int Completed, int Retrying, int Cancelled, int DeadLetter
                )>();

        await using (var timelineCmd = CreateCommand(conn, """
                                                           SELECT
                                                               CAST((julianday(created_at) - julianday(@since)) * 86400.0 / @bucket_seconds AS INTEGER) AS bucket_idx,
                                                               status,
                                                               COUNT(*) AS cnt
                                                           FROM surefire_runs
                                                           WHERE created_at >= @since AND created_at < @now
                                                           GROUP BY bucket_idx, status
                                                           """, tx))
        {
            timelineCmd.Parameters.AddWithValue("@since", FormatTimestamp(sinceTime));
            timelineCmd.Parameters.AddWithValue("@now", FormatTimestamp(now));
            timelineCmd.Parameters.AddWithValue("@bucket_seconds", bucketSeconds);
            await using var reader = await timelineCmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var idx = reader.GetInt32(0);
                var status = (JobStatus)reader.GetInt32(1);
                var cnt = reader.GetInt32(2);

                if (!bucketData.TryGetValue(idx, out var entry))
                {
                    entry = (0, 0, 0, 0, 0, 0);
                }

                bucketData[idx] = status switch
                {
                    JobStatus.Pending => (entry.Pending + cnt, entry.Running, entry.Completed, entry.Retrying,
                        entry.Cancelled, entry.DeadLetter),
                    JobStatus.Running => (entry.Pending, entry.Running + cnt, entry.Completed, entry.Retrying,
                        entry.Cancelled, entry.DeadLetter),
                    JobStatus.Completed => (entry.Pending, entry.Running, entry.Completed + cnt, entry.Retrying,
                        entry.Cancelled, entry.DeadLetter),
                    JobStatus.Retrying => (entry.Pending, entry.Running, entry.Completed, entry.Retrying + cnt,
                        entry.Cancelled, entry.DeadLetter),
                    JobStatus.Cancelled => (entry.Pending, entry.Running, entry.Completed, entry.Retrying,
                        entry.Cancelled + cnt, entry.DeadLetter),
                    JobStatus.DeadLetter => (entry.Pending, entry.Running, entry.Completed, entry.Retrying,
                        entry.Cancelled, entry.DeadLetter + cnt),
                    _ => entry
                };
            }
        }

        await tx.CommitAsync(cancellationToken);

        var buckets = new List<TimelineBucket>();
        var bucketStart = sinceTime;
        var bucketIdx = 0;
        while (bucketStart < now)
        {
            bucketData.TryGetValue(bucketIdx, out var entry);
            buckets.Add(new()
            {
                Start = bucketStart,
                Pending = entry.Pending,
                Running = entry.Running,
                Completed = entry.Completed,
                Retrying = entry.Retrying,
                Cancelled = entry.Cancelled,
                DeadLetter = entry.DeadLetter
            });
            bucketStart += bucketSize;
            bucketIdx++;
        }

        return new()
        {
            TotalJobs = totalJobs,
            TotalRuns = totalRuns,
            ActiveRuns = activeRuns,
            SuccessRate = successRate,
            NodeCount = nodeCount,
            RunsByStatus = runsByStatus,
            Timeline = buckets
        };
    }

    public async Task<JobStats> GetJobStatsAsync(
        string jobName, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  SELECT
                                                      COUNT(*),
                                                      COALESCE(SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END), 0),
                                                      COALESCE(SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END), 0),
                                                      AVG(CASE
                                                          WHEN status = 2 AND started_at IS NOT NULL AND completed_at IS NOT NULL
                                                          THEN (julianday(completed_at) - julianday(started_at)) * 86400.0
                                                          ELSE NULL
                                                      END),
                                                      MAX(CASE WHEN started_at IS NOT NULL THEN started_at ELSE NULL END),
                                                      COALESCE(SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END), 0)
                                                  FROM surefire_runs
                                                  WHERE job_name = @n
                                                  """);
        cmd.Parameters.AddWithValue("@n", jobName);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);

        var total = Convert.ToInt32(reader.GetValue(0));
        var succeeded = Convert.ToInt32(reader.GetValue(1));
        var failed = Convert.ToInt32(reader.GetValue(2));
        var cancelledCount = Convert.ToInt32(reader.GetValue(5));
        var terminalCount = succeeded + failed + cancelledCount;

        return new()
        {
            TotalRuns = total,
            SucceededRuns = succeeded,
            FailedRuns = failed,
            SuccessRate = terminalCount > 0 ? succeeded / (double)terminalCount : 0.0,
            AvgDuration = reader.IsDBNull(3)
                ? null
                : TimeSpan.FromSeconds(reader.GetDouble(3)),
            LastRunAt = reader.IsDBNull(4)
                ? null
                : ParseTimestamp(reader.GetString(4))
        };
    }

    public async Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  WITH queue_names AS (
                                                      SELECT name FROM surefire_queues
                                                      UNION
                                                      SELECT COALESCE(j.queue, 'default') AS name
                                                      FROM surefire_runs r
                                                      JOIN surefire_jobs j ON j.name = r.job_name
                                                      WHERE r.status = 0 AND r.batch_total IS NULL
                                                      UNION
                                                      SELECT COALESCE(j.queue, 'default') AS name
                                                      FROM surefire_runs r
                                                      JOIN surefire_jobs j ON j.name = r.job_name
                                                      WHERE r.status = 1
                                                  ),
                                                  pending AS (
                                                      SELECT COALESCE(j.queue, 'default') AS queue_name, COUNT(*) AS cnt
                                                      FROM surefire_runs r
                                                      JOIN surefire_jobs j ON j.name = r.job_name
                                                      WHERE r.status = 0 AND r.batch_total IS NULL
                                                      GROUP BY queue_name
                                                  ),
                                                  running AS (
                                                      SELECT COALESCE(j.queue, 'default') AS queue_name, COUNT(*) AS cnt
                                                      FROM surefire_runs r
                                                      JOIN surefire_jobs j ON j.name = r.job_name
                                                      WHERE r.status = 1
                                                      GROUP BY queue_name
                                                  )
                                                  SELECT
                                                      qn.name,
                                                      COALESCE(pending.cnt, 0),
                                                      COALESCE(running.cnt, 0)
                                                  FROM queue_names qn
                                                  LEFT JOIN pending ON pending.queue_name = qn.name
                                                  LEFT JOIN running ON running.queue_name = qn.name
                                                  ORDER BY qn.name
                                                  """);

        var stats = new Dictionary<string, QueueStats>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            stats[reader.GetString(0)] = new()
            {
                PendingCount = Convert.ToInt32(reader.GetValue(1)),
                RunningCount = Convert.ToInt32(reader.GetValue(2))
            };
        }

        return stats;
    }

    private async Task CreateRunsCoreAsync(
        IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        if (runs.Count == 0 && (initialEvents is null || initialEvents.Count == 0))
        {
            return;
        }

        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);
        foreach (var run in runs)
        {
            await InsertRunAsync(conn, run, cancellationToken, tx);
        }

        await InsertEventsAsync(conn, initialEvents, cancellationToken, tx);

        await tx.CommitAsync(cancellationToken);
    }

    private async Task<bool> TryCreateRunCoreAsync(
        JobRun run, int? maxActiveForJob, DateTimeOffset? lastCronFireAt,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(deferred: false);

        if (run.DeduplicationId is { })
        {
            await using var dedupCmd = CreateCommand(conn, """
                                                           SELECT COUNT(*)
                                                           FROM surefire_runs
                                                           WHERE job_name = @j AND deduplication_id = @d
                                                               AND status NOT IN (2, 4, 5)
                                                           """, tx);
            dedupCmd.Parameters.AddWithValue("@j", run.JobName);
            dedupCmd.Parameters.AddWithValue("@d", run.DeduplicationId);
            if ((long)(await dedupCmd.ExecuteScalarAsync(cancellationToken))! > 0)
            {
                await tx.CommitAsync(cancellationToken);
                return false;
            }
        }

        if (maxActiveForJob is { })
        {
            await using var enabledCmd = CreateCommand(conn, """
                                                             SELECT is_enabled FROM surefire_jobs WHERE name = @n
                                                             """, tx);
            enabledCmd.Parameters.AddWithValue("@n", run.JobName);
            var enabledValue = await enabledCmd.ExecuteScalarAsync(cancellationToken);
            if (enabledValue is long enabledLong && enabledLong == 0)
            {
                await tx.CommitAsync(cancellationToken);
                return false;
            }

            await using var activeCmd = CreateCommand(conn, """
                                                            SELECT COUNT(*)
                                                            FROM surefire_runs
                                                            WHERE job_name = @j AND status NOT IN (2, 4, 5)
                                                            """, tx);
            activeCmd.Parameters.AddWithValue("@j", run.JobName);
            if ((long)(await activeCmd.ExecuteScalarAsync(cancellationToken))! >= maxActiveForJob.Value)
            {
                await tx.CommitAsync(cancellationToken);
                return false;
            }
        }

        try
        {
            await InsertRunAsync(conn, run, cancellationToken, tx);

            if (lastCronFireAt is { })
            {
                await using var lcfaCmd = CreateCommand(conn, """
                                                              UPDATE surefire_jobs SET last_cron_fire_at = @lcfa WHERE name = @job_name
                                                              """, tx);
                lcfaCmd.Parameters.AddWithValue("@lcfa", FormatTimestamp(lastCronFireAt.Value));
                lcfaCmd.Parameters.AddWithValue("@job_name", run.JobName);
                await lcfaCmd.ExecuteNonQueryAsync(cancellationToken);
            }

            await InsertEventsAsync(conn, initialEvents, cancellationToken, tx);

            await tx.CommitAsync(cancellationToken);
            return true;
        }
        catch (SqliteException ex) when (ex.SqliteErrorCode == 19)
        {
            await tx.RollbackAsync(cancellationToken);
            return false;
        }
    }

    private static async Task InsertEventsAsync(SqliteConnection conn, IReadOnlyList<RunEvent>? events,
        CancellationToken cancellationToken, SqliteTransaction tx)
    {
        if (events is null || events.Count == 0)
        {
            return;
        }

        const int maxEventsPerChunk = 199; // 199 * 5 params = 995, under SQLite's 999-variable limit
        for (var offset = 0; offset < events.Count; offset += maxEventsPerChunk)
        {
            var chunk = Math.Min(maxEventsPerChunk, events.Count - offset);
            await using var cmd = new SqliteCommand { Connection = conn, Transaction = tx };
            var sb = new StringBuilder(
                "INSERT INTO surefire_events (run_id, event_type, payload, created_at, attempt) VALUES ");

            for (var i = 0; i < chunk; i++)
            {
                if (i > 0)
                {
                    sb.Append(',');
                }

                sb.Append($"(@r{i}, @t{i}, @p{i}, @c{i}, @a{i})");
                var evt = events[offset + i];
                cmd.Parameters.AddWithValue($"@r{i}", evt.RunId);
                cmd.Parameters.AddWithValue($"@t{i}", (int)evt.EventType);
                cmd.Parameters.AddWithValue($"@p{i}", evt.Payload);
                cmd.Parameters.AddWithValue($"@c{i}", FormatTimestamp(evt.CreatedAt));
                cmd.Parameters.AddWithValue($"@a{i}", evt.Attempt);
            }

            cmd.CommandText = sb.ToString();
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task<SqliteConnection> CreateConnectionAsync(CancellationToken cancellationToken = default)
    {
        var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var pragmaCmd = conn.CreateCommand();
        pragmaCmd.CommandText = "PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON; PRAGMA synchronous=NORMAL;";
        await pragmaCmd.ExecuteNonQueryAsync(cancellationToken);
        return conn;
    }

    private static SqliteCommand CreateCommand(SqliteConnection conn, string sql, SqliteTransaction? tx = null) =>
        new(sql, conn) { Transaction = tx };

    private static async Task<bool> HasMigrationVersionAsync(SqliteConnection conn, int version,
        CancellationToken cancellationToken)
    {
        await using var cmd = CreateCommand(conn,
            "SELECT COUNT(*) FROM surefire_schema_migrations WHERE version = @version");
        cmd.Parameters.AddWithValue("@version", version);
        return (long)(await cmd.ExecuteScalarAsync(cancellationToken))! > 0;
    }

    private static async Task<bool> HasColumnAsync(SqliteConnection conn, string tableName, string columnName,
        CancellationToken cancellationToken)
    {
        await using var cmd = CreateCommand(conn, $"PRAGMA table_info({tableName});");
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (string.Equals(reader.GetString(reader.GetOrdinal("name")), columnName,
                    StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static async Task ExecuteWithThresholdAsync(
        SqliteConnection conn, string sql, string threshold, SqliteTransaction tx, CancellationToken cancellationToken)
    {
        await using var cmd = CreateCommand(conn, sql, tx);
        cmd.Parameters.AddWithValue("@b", threshold);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private void AddJobParameters(SqliteCommand cmd, JobDefinition job)
    {
        cmd.Parameters.AddWithValue("@name", job.Name);
        cmd.Parameters.AddWithValue("@description", (object?)job.Description ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@tags", JsonSerializer.Serialize(job.Tags));
        cmd.Parameters.AddWithValue("@cron_expression", (object?)job.CronExpression ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@time_zone_id", (object?)job.TimeZoneId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@timeout_ticks",
            job.Timeout.HasValue ? job.Timeout.Value.Ticks : DBNull.Value);
        cmd.Parameters.AddWithValue("@max_concurrency",
            job.MaxConcurrency.HasValue ? job.MaxConcurrency.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("@priority", job.Priority);
        cmd.Parameters.AddWithValue("@retry_policy", JsonSerializer.Serialize(job.RetryPolicy));
        cmd.Parameters.AddWithValue("@is_continuous", job.IsContinuous ? 1 : 0);
        cmd.Parameters.AddWithValue("@queue", (object?)job.Queue ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@rate_limit_name", (object?)job.RateLimitName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@is_enabled", job.IsEnabled ? 1 : 0);
        cmd.Parameters.AddWithValue("@misfire_policy", (int)job.MisfirePolicy);
        cmd.Parameters.AddWithValue("@fire_all_limit",
            job.FireAllLimit.HasValue ? job.FireAllLimit.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("@arguments_schema", (object?)job.ArgumentsSchema ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@last_heartbeat_at", FormatTimestamp(timeProvider.GetUtcNow()));
    }

    private static async Task InsertRunAsync(
        SqliteConnection conn,
        JobRun run,
        CancellationToken cancellationToken,
        SqliteTransaction? tx = null)
    {
        await using var cmd = CreateCommand(conn, """
                                                  INSERT INTO surefire_runs (
                                                      id, job_name, status, arguments, result, error, progress,
                                                      created_at, started_at, completed_at, cancelled_at, node_name,
                                                      attempt, trace_id, span_id, parent_run_id, root_run_id,
                                                      rerun_of_run_id, not_before, not_after, priority,
                                                      deduplication_id, last_heartbeat_at,
                                                      batch_total, batch_completed, batch_failed,
                                                      queue_priority
                                                  ) VALUES (
                                                      @id, @jn, @st, @ar, @re, @er, @pr,
                                                      @ca, @sa, @coa, @caa, @nn,
                                                      @at, @ti, @si, @pri, @rri,
                                                      @ror, @nb, @na, @py,
                                                      @di, @lh,
                                                      @bt, @bc, @bf,
                                                      COALESCE((SELECT q.priority FROM surefire_queues q WHERE q.name = COALESCE((SELECT j.queue FROM surefire_jobs j WHERE j.name = @jn), 'default')), 0)
                                                  )
                                                  """, tx);
        cmd.Parameters.AddWithValue("@id", run.Id);
        cmd.Parameters.AddWithValue("@jn", run.JobName);
        cmd.Parameters.AddWithValue("@st", (int)run.Status);
        cmd.Parameters.AddWithValue("@ar", (object?)run.Arguments ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@re", (object?)run.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@er", (object?)run.Error ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@pr", run.Progress);
        cmd.Parameters.AddWithValue("@ca", FormatTimestamp(run.CreatedAt));
        cmd.Parameters.AddWithValue("@sa", FormatNullableTimestamp(run.StartedAt));
        cmd.Parameters.AddWithValue("@coa", FormatNullableTimestamp(run.CompletedAt));
        cmd.Parameters.AddWithValue("@caa", FormatNullableTimestamp(run.CancelledAt));
        cmd.Parameters.AddWithValue("@nn", (object?)run.NodeName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@at", run.Attempt);
        cmd.Parameters.AddWithValue("@ti", (object?)run.TraceId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@si", (object?)run.SpanId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@pri", (object?)run.ParentRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@rri", (object?)run.RootRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ror", (object?)run.RerunOfRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@nb", FormatTimestamp(run.NotBefore));
        cmd.Parameters.AddWithValue("@na", FormatNullableTimestamp(run.NotAfter));
        cmd.Parameters.AddWithValue("@py", run.Priority);
        cmd.Parameters.AddWithValue("@di", (object?)run.DeduplicationId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@lh", FormatNullableTimestamp(run.LastHeartbeatAt));
        cmd.Parameters.AddWithValue("@bt",
            run.BatchTotal.HasValue ? run.BatchTotal.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("@bc", run.BatchCompleted ?? 0);
        cmd.Parameters.AddWithValue("@bf", run.BatchFailed ?? 0);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private JobDefinition ReadJob(SqliteDataReader reader)
    {
        TimeSpan? timeout = null;
        var timeoutTicksOrdinal = TryGetOrdinal(reader, "timeout_ticks");
        if (timeoutTicksOrdinal >= 0 && !reader.IsDBNull(timeoutTicksOrdinal))
        {
            timeout = TimeSpan.FromTicks(reader.GetInt64(timeoutTicksOrdinal));
        }
        else
        {
            var timeoutMsOrdinal = TryGetOrdinal(reader, "timeout_ms");
            if (timeoutMsOrdinal >= 0 && !reader.IsDBNull(timeoutMsOrdinal))
            {
                timeout = TimeSpan.FromMilliseconds(reader.GetDouble(timeoutMsOrdinal));
            }
        }

        return new()
        {
            Name = reader.GetString(reader.GetOrdinal("name")),
            Description = GetNullableString(reader, "description"),
            Tags = JsonSerializer.Deserialize<string[]>(
                reader.GetString(reader.GetOrdinal("tags"))) ?? [],
            CronExpression = GetNullableString(reader, "cron_expression"),
            TimeZoneId = GetNullableString(reader, "time_zone_id"),
            Timeout = timeout,
            MaxConcurrency = reader.IsDBNull(reader.GetOrdinal("max_concurrency"))
                ? null
                : reader.GetInt32(reader.GetOrdinal("max_concurrency")),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            RetryPolicy = reader.IsDBNull(reader.GetOrdinal("retry_policy"))
                ? new()
                : JsonSerializer.Deserialize<RetryPolicy>(
                    reader.GetString(reader.GetOrdinal("retry_policy"))) ?? new(),
            IsContinuous = reader.GetInt32(reader.GetOrdinal("is_continuous")) != 0,
            Queue = GetNullableString(reader, "queue"),
            RateLimitName = GetNullableString(reader, "rate_limit_name"),
            IsEnabled = reader.GetInt32(reader.GetOrdinal("is_enabled")) != 0,
            MisfirePolicy = (MisfirePolicy)reader.GetInt32(reader.GetOrdinal("misfire_policy")),
            FireAllLimit = reader.IsDBNull(reader.GetOrdinal("fire_all_limit"))
                ? null
                : reader.GetInt32(reader.GetOrdinal("fire_all_limit")),
            ArgumentsSchema = GetNullableString(reader, "arguments_schema"),
            LastHeartbeatAt = GetNullableTimestamp(reader, "last_heartbeat_at"),
            LastCronFireAt = GetNullableTimestamp(reader, "last_cron_fire_at")
        };
    }

    private static QueueDefinition ReadQueue(SqliteDataReader reader) => new()
    {
        Name = reader.GetString(reader.GetOrdinal("name")),
        Priority = reader.GetInt32(reader.GetOrdinal("priority")),
        MaxConcurrency = reader.IsDBNull(reader.GetOrdinal("max_concurrency"))
            ? null
            : reader.GetInt32(reader.GetOrdinal("max_concurrency")),
        IsPaused = reader.GetInt32(reader.GetOrdinal("is_paused")) != 0,
        RateLimitName = GetNullableString(reader, "rate_limit_name"),
        LastHeartbeatAt = GetNullableTimestamp(reader, "last_heartbeat_at")
    };

    private static JobRun ReadRun(SqliteDataReader reader)
    {
        var batchTotal = reader.IsDBNull(reader.GetOrdinal("batch_total"))
            ? (int?)null
            : reader.GetInt32(reader.GetOrdinal("batch_total"));

        return new()
        {
            Id = reader.GetString(reader.GetOrdinal("id")),
            JobName = reader.GetString(reader.GetOrdinal("job_name")),
            Status = (JobStatus)reader.GetInt32(reader.GetOrdinal("status")),
            Arguments = GetNullableString(reader, "arguments"),
            Result = GetNullableString(reader, "result"),
            Error = GetNullableString(reader, "error"),
            Progress = reader.GetDouble(reader.GetOrdinal("progress")),
            CreatedAt = ParseTimestamp(reader.GetString(reader.GetOrdinal("created_at"))),
            StartedAt = GetNullableTimestamp(reader, "started_at"),
            CompletedAt = GetNullableTimestamp(reader, "completed_at"),
            CancelledAt = GetNullableTimestamp(reader, "cancelled_at"),
            NodeName = GetNullableString(reader, "node_name"),
            Attempt = reader.GetInt32(reader.GetOrdinal("attempt")),
            TraceId = GetNullableString(reader, "trace_id"),
            SpanId = GetNullableString(reader, "span_id"),
            ParentRunId = GetNullableString(reader, "parent_run_id"),
            RootRunId = GetNullableString(reader, "root_run_id"),
            RerunOfRunId = GetNullableString(reader, "rerun_of_run_id"),
            NotBefore = ParseTimestamp(reader.GetString(reader.GetOrdinal("not_before"))),
            NotAfter = GetNullableTimestamp(reader, "not_after"),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            QueuePriority = reader.GetInt32(reader.GetOrdinal("queue_priority")),
            DeduplicationId = GetNullableString(reader, "deduplication_id"),
            LastHeartbeatAt = GetNullableTimestamp(reader, "last_heartbeat_at"),
            BatchTotal = batchTotal,
            BatchCompleted = batchTotal.HasValue
                ? reader.IsDBNull(reader.GetOrdinal("batch_completed"))
                    ? null
                    : reader.GetInt32(reader.GetOrdinal("batch_completed"))
                : null,
            BatchFailed = batchTotal.HasValue
                ? reader.IsDBNull(reader.GetOrdinal("batch_failed"))
                    ? null
                    : reader.GetInt32(reader.GetOrdinal("batch_failed"))
                : null
        };
    }

    private static NodeInfo ReadNode(SqliteDataReader reader) => new()
    {
        Name = reader.GetString(reader.GetOrdinal("name")),
        StartedAt = ParseTimestamp(reader.GetString(reader.GetOrdinal("started_at"))),
        LastHeartbeatAt = ParseTimestamp(reader.GetString(reader.GetOrdinal("last_heartbeat_at"))),
        RunningCount = reader.GetInt32(reader.GetOrdinal("running_count")),
        RegisteredJobNames = JsonSerializer.Deserialize<string[]>(
            reader.GetString(reader.GetOrdinal("registered_job_names"))) ?? [],
        RegisteredQueueNames = JsonSerializer.Deserialize<string[]>(
            reader.GetString(reader.GetOrdinal("registered_queue_names"))) ?? []
    };

    private async Task AcquireRateLimitAsync(
        SqliteConnection conn,
        string lookupSql,
        SqliteParameter[] parameters,
        string now,
        SqliteTransaction tx,
        CancellationToken cancellationToken)
    {
        string? rateLimitName;
        await using (var lookupCmd = CreateCommand(conn, lookupSql, tx))
        {
            foreach (var p in parameters)
            {
                lookupCmd.Parameters.Add(p);
            }

            rateLimitName = await lookupCmd.ExecuteScalarAsync(cancellationToken) as string;
        }

        if (rateLimitName is null)
        {
            return;
        }

        await using var acquireCmd = CreateCommand(conn, """
                                                         UPDATE surefire_rate_limits
                                                         SET previous_count = CASE
                                                                 WHEN window_start IS NULL THEN 0
                                                                 WHEN (julianday(@now) - julianday(window_start))
                                                                     * 864000000000.0 >= CAST("window" AS REAL) * 2
                                                                 THEN 0
                                                                 WHEN julianday(window_start)
                                                                     + CAST("window" AS REAL) / 864000000000.0
                                                                     <= julianday(@now)
                                                                 THEN current_count
                                                                 ELSE previous_count
                                                             END,
                                                             current_count = CASE
                                                                 WHEN window_start IS NULL THEN 1
                                                                 WHEN julianday(window_start)
                                                                     + CAST("window" AS REAL) / 864000000000.0
                                                                     <= julianday(@now)
                                                                 THEN 1
                                                                 ELSE current_count + 1
                                                             END,
                                                             window_start = CASE
                                                                 WHEN window_start IS NULL THEN @now
                                                                 WHEN (julianday(@now) - julianday(window_start))
                                                                     * 864000000000.0 >= CAST("window" AS REAL) * 2
                                                                 THEN strftime('%Y-%m-%dT%H:%M:%fZ', julianday(window_start)
                                                                     + CAST("window" AS REAL) * CAST(
                                                                         (julianday(@now) - julianday(window_start)) * 864000000000.0
                                                                         / CAST("window" AS REAL) AS INTEGER)
                                                                     / 864000000000.0)
                                                                 WHEN julianday(window_start)
                                                                     + CAST("window" AS REAL) / 864000000000.0
                                                                     <= julianday(@now)
                                                                 THEN strftime('%Y-%m-%dT%H:%M:%fZ', julianday(window_start)
                                                                     + CAST("window" AS REAL) / 864000000000.0)
                                                                 ELSE window_start
                                                             END
                                                         WHERE name = @name
                                                         """, tx);
        acquireCmd.Parameters.AddWithValue("@name", rateLimitName);
        acquireCmd.Parameters.AddWithValue("@now", now);
        await acquireCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string EscapeLike(string input) =>
        input.Replace(@"\", @"\\").Replace("%", @"\%").Replace("_", @"\_");

    private static string FormatTimestamp(DateTimeOffset dto) =>
        dto.UtcDateTime.ToString("O");

    private static object FormatNullableTimestamp(DateTimeOffset? dto) =>
        dto.HasValue ? FormatTimestamp(dto.Value) : DBNull.Value;

    private static DateTimeOffset ParseTimestamp(string s) =>
        DateTimeOffset.Parse(s, null, DateTimeStyles.RoundtripKind);

    private static int TryGetOrdinal(SqliteDataReader reader, string column)
    {
        for (var i = 0; i < reader.FieldCount; i++)
        {
            if (string.Equals(reader.GetName(i), column, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        return -1;
    }

    private static string? GetNullableString(SqliteDataReader reader, string column)
    {
        var ordinal = reader.GetOrdinal(column);
        return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
    }

    private static DateTimeOffset? GetNullableTimestamp(SqliteDataReader reader, string column)
    {
        var ordinal = reader.GetOrdinal(column);
        return reader.IsDBNull(ordinal) ? null : ParseTimestamp(reader.GetString(ordinal));
    }

    private static SqliteParameter[] BuildInClause<T>(
        string prefix, IEnumerable<T> values, out string clause)
    {
        var list = values.ToList();
        var parameters = new SqliteParameter[list.Count];
        var placeholders = new string[list.Count];
        for (var i = 0; i < list.Count; i++)
        {
            var name = $"{prefix}{i}";
            placeholders[i] = name;
            parameters[i] = new(name, list[i]);
        }

        clause = string.Join(", ", placeholders);
        return parameters;
    }
}