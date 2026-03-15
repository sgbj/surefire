using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Surefire.Sqlite;

internal sealed class SqliteJobStore(
    SqliteOptions sqliteOptions,
    SurefireOptions surefireOptions) : IJobStore
{
    private JsonSerializerOptions JsonOptions => surefireOptions.SerializerOptions;

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);

        // WAL mode persists on the database file — only needs to be set once
        await using (var walCmd = new SqliteCommand("PRAGMA journal_mode=WAL;", conn))
            await walCmd.ExecuteNonQueryAsync(cancellationToken);

        // Bootstrap migrations tracking table, then bail out if v1 already applied.
        await using (var bootstrapCmd = new SqliteCommand(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER NOT NULL PRIMARY KEY);", conn))
        {
            await bootstrapCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var checkCmd = new SqliteCommand(
            "SELECT COUNT(*) FROM schema_migrations WHERE version = 1", conn))
        {
            var count = (long)(await checkCmd.ExecuteScalarAsync(cancellationToken))!;
            if (count > 0) return;
        }

        var sql = """
            CREATE TABLE IF NOT EXISTS jobs (
                name TEXT PRIMARY KEY,
                description TEXT,
                tags TEXT NOT NULL DEFAULT '[]',
                cron_expression TEXT,
                time_zone_id TEXT,
                timeout_ms REAL,
                max_concurrency INTEGER,
                priority INTEGER NOT NULL DEFAULT 0,
                retry_policy TEXT,
                is_continuous INTEGER NOT NULL DEFAULT 0,
                queue TEXT,
                rate_limit_name TEXT,
                is_enabled INTEGER NOT NULL DEFAULT 1,
                is_plan INTEGER NOT NULL DEFAULT 0,
                is_internal INTEGER NOT NULL DEFAULT 0,
                plan_graph TEXT,
                misfire_policy INTEGER NOT NULL DEFAULT 0,
                arguments_schema TEXT,
                last_heartbeat_at TEXT
            );

            CREATE TABLE IF NOT EXISTS runs (
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
                attempt INTEGER NOT NULL DEFAULT 1,
                trace_id TEXT,
                span_id TEXT,
                parent_run_id TEXT,
                retry_of_run_id TEXT,
                rerun_of_run_id TEXT,
                not_before TEXT NOT NULL,
                not_after TEXT,
                priority INTEGER NOT NULL DEFAULT 0,
                deduplication_id TEXT,
                last_heartbeat_at TEXT,
                plan_run_id TEXT,
                plan_step_id TEXT,
                plan_step_name TEXT,
                plan_graph TEXT
            );

            CREATE INDEX IF NOT EXISTS ix_runs_claim ON runs (priority DESC, not_before, created_at) WHERE status = 0;
            CREATE INDEX IF NOT EXISTS ix_runs_job_name_status ON runs (job_name, status);
            CREATE INDEX IF NOT EXISTS ix_runs_created_at ON runs (created_at DESC);
            CREATE INDEX IF NOT EXISTS ix_runs_retry_of_run_id ON runs (retry_of_run_id);
            CREATE INDEX IF NOT EXISTS ix_runs_rerun_of_run_id ON runs (rerun_of_run_id);
            CREATE INDEX IF NOT EXISTS ix_runs_parent_run_id ON runs (parent_run_id);
            CREATE INDEX IF NOT EXISTS ix_runs_node_name_status ON runs (node_name, status);
            CREATE UNIQUE INDEX IF NOT EXISTS ix_runs_deduplication_id ON runs (deduplication_id) WHERE deduplication_id IS NOT NULL;
            CREATE UNIQUE INDEX IF NOT EXISTS ix_runs_plan_step ON runs (plan_run_id, plan_step_id) WHERE plan_run_id IS NOT NULL AND retry_of_run_id IS NULL;
            CREATE INDEX IF NOT EXISTS ix_runs_plan_run_id ON runs (plan_run_id) WHERE plan_run_id IS NOT NULL;

            CREATE TABLE IF NOT EXISTS rate_limits (
                name TEXT PRIMARY KEY,
                type INTEGER NOT NULL DEFAULT 0,
                max_permits INTEGER NOT NULL,
                window_duration_ms REAL NOT NULL,
                current_count INTEGER NOT NULL DEFAULT 0,
                previous_count INTEGER NOT NULL DEFAULT 0,
                window_start TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS queues (
                name TEXT PRIMARY KEY,
                priority INTEGER NOT NULL DEFAULT 0,
                max_concurrency INTEGER,
                is_paused INTEGER NOT NULL DEFAULT 0,
                rate_limit_name TEXT,
                last_heartbeat_at TEXT
            );

            CREATE TABLE IF NOT EXISTS nodes (
                name TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                last_heartbeat_at TEXT NOT NULL,
                running_count INTEGER NOT NULL DEFAULT 0,
                registered_job_names TEXT NOT NULL DEFAULT '[]',
                registered_queue_names TEXT NOT NULL DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS run_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                event_type INTEGER NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS ix_run_events_run_id ON run_events (run_id, id);
            """;

        await using var cmd = new SqliteCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(cancellationToken);

        await using var markCmd = new SqliteCommand(
            "INSERT OR IGNORE INTO schema_migrations (version) VALUES (1)", conn);
        await markCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Jobs --

    public async Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        var sql = """
            INSERT INTO jobs (name, description, tags, cron_expression, time_zone_id, timeout_ms, max_concurrency, priority, retry_policy, is_continuous, queue, rate_limit_name, is_enabled, is_plan, is_internal, plan_graph, misfire_policy, arguments_schema, last_heartbeat_at)
            VALUES (@name, @description, @tags, @cron_expression, @time_zone_id, @timeout_ms, @max_concurrency, @priority, @retry_policy, @is_continuous, @queue, @rate_limit_name, @is_enabled, @is_plan, @is_internal, @plan_graph, @misfire_policy, @arguments_schema, @last_heartbeat_at)
            ON CONFLICT (name) DO UPDATE SET
                description = excluded.description,
                tags = excluded.tags,
                cron_expression = excluded.cron_expression,
                time_zone_id = excluded.time_zone_id,
                timeout_ms = excluded.timeout_ms,
                max_concurrency = excluded.max_concurrency,
                priority = excluded.priority,
                retry_policy = excluded.retry_policy,
                is_continuous = excluded.is_continuous,
                queue = excluded.queue,
                rate_limit_name = excluded.rate_limit_name,
                is_plan = excluded.is_plan,
                is_internal = excluded.is_internal,
                plan_graph = excluded.plan_graph,
                misfire_policy = excluded.misfire_policy,
                arguments_schema = excluded.arguments_schema,
                last_heartbeat_at = @last_heartbeat_at
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(sql, conn);
        cmd.Parameters.AddWithValue("@name", job.Name);
        cmd.Parameters.AddWithValue("@description", (object?)job.Description ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@tags", JsonSerializer.Serialize(job.Tags, JsonOptions));
        cmd.Parameters.AddWithValue("@cron_expression", (object?)job.CronExpression ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@time_zone_id", (object?)job.TimeZoneId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@timeout_ms", job.Timeout.HasValue ? job.Timeout.Value.TotalMilliseconds : DBNull.Value);
        cmd.Parameters.AddWithValue("@max_concurrency", job.MaxConcurrency.HasValue ? job.MaxConcurrency.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("@priority", job.Priority);
        cmd.Parameters.AddWithValue("@retry_policy", JsonSerializer.Serialize(job.RetryPolicy, JsonOptions));
        cmd.Parameters.AddWithValue("@is_continuous", job.IsContinuous ? 1 : 0);
        cmd.Parameters.AddWithValue("@queue", (object?)job.Queue ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@rate_limit_name", (object?)job.RateLimitName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@is_enabled", job.IsEnabled ? 1 : 0);
        cmd.Parameters.AddWithValue("@is_plan", job.IsPlan ? 1 : 0);
        cmd.Parameters.AddWithValue("@is_internal", job.IsInternal ? 1 : 0);
        cmd.Parameters.AddWithValue("@plan_graph", (object?)job.PlanGraph ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@misfire_policy", (int)job.MisfirePolicy);
        cmd.Parameters.AddWithValue("@arguments_schema", (object?)job.ArgumentsSchema ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@last_heartbeat_at", FormatDto(DateTimeOffset.UtcNow));

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("SELECT * FROM jobs WHERE name = @name", conn);
        cmd.Parameters.AddWithValue("@name", name);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadJob(reader) : null;
    }

    public async Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null, CancellationToken cancellationToken = default)
    {
        var sql = "SELECT * FROM jobs WHERE 1=1";
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand { Connection = conn };

        if (filter?.Name is not null)
        {
            sql += " AND name LIKE @name ESCAPE '\\'";
            cmd.Parameters.AddWithValue("@name", $"%{EscapeLike(filter.Name)}%");
        }
        if (filter?.Tag is not null)
        {
            sql += " AND EXISTS (SELECT 1 FROM json_each(tags) WHERE value LIKE @tag ESCAPE '\\')";
            cmd.Parameters.AddWithValue("@tag", $"%{EscapeLike(filter.Tag)}%");
        }
        if (filter?.IsEnabled is not null)
        {
            sql += " AND is_enabled = @is_enabled";
            cmd.Parameters.AddWithValue("@is_enabled", filter.IsEnabled.Value ? 1 : 0);
        }
        if (filter?.HeartbeatAfter is not null)
        {
            sql += " AND last_heartbeat_at IS NOT NULL AND last_heartbeat_at >= @heartbeat_after";
            cmd.Parameters.AddWithValue("@heartbeat_after", FormatDto(filter.HeartbeatAfter.Value));
        }
        if (filter?.IncludeInternal != true)
            sql += " AND is_internal = 0";

        sql += " ORDER BY name";
        cmd.CommandText = sql;

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var jobs = new List<JobDefinition>();
        while (await reader.ReadAsync(cancellationToken))
            jobs.Add(ReadJob(reader));
        return jobs;
    }

    public async Task RemoveJobAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("DELETE FROM jobs WHERE name = @name", conn);
        cmd.Parameters.AddWithValue("@name", name);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task SetJobEnabledAsync(string name, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("UPDATE jobs SET is_enabled = @enabled WHERE name = @name", conn);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@enabled", enabled ? 1 : 0);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Runs --

    public async Task CreateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await InsertRunAsync(conn, run, cancellationToken);
    }

    public async Task CreateRunsAsync(IReadOnlyList<JobRun> runs, CancellationToken cancellationToken = default)
    {
        if (runs.Count == 0) return;

        await using var conn = await OpenAsync(cancellationToken);
        await using var tx = conn.BeginTransaction();

        foreach (var run in runs)
            await InsertRunAsync(conn, run, cancellationToken, tx);

        await tx.CommitAsync(cancellationToken);
    }

    public async Task<bool> TryCreateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        if (run.DeduplicationId is null)
        {
            try
            {
                await CreateRunAsync(run, cancellationToken);
                return true;
            }
            catch (SqliteException ex) when (ex.SqliteErrorCode == 19) // SQLITE_CONSTRAINT
            {
                // Plan step unique constraint violation — step already created by another node
                return false;
            }
        }

        var sql = """
            INSERT OR IGNORE INTO runs (id, job_name, status, arguments, result, error, progress, created_at, started_at, completed_at, cancelled_at, node_name, attempt, trace_id, span_id, parent_run_id, retry_of_run_id, rerun_of_run_id, not_before, not_after, priority, deduplication_id, last_heartbeat_at, plan_run_id, plan_step_id, plan_step_name, plan_graph)
            VALUES (@id, @job_name, @status, @arguments, @result, @error, @progress, @created_at, @started_at, @completed_at, @cancelled_at, @node_name, @attempt, @trace_id, @span_id, @parent_run_id, @retry_of_run_id, @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id, @last_heartbeat_at, @plan_run_id, @plan_step_id, @plan_step_name, @plan_graph)
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(sql, conn);
        AddRunParameters(cmd, run);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0;
    }

    public async Task<bool> TryCreateContinuousRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var tx = conn.BeginTransaction();

        var sql = """
            INSERT OR IGNORE INTO runs (id, job_name, status, arguments, result, error, progress, created_at, started_at, completed_at, cancelled_at, node_name, attempt, trace_id, span_id, parent_run_id, retry_of_run_id, rerun_of_run_id, not_before, not_after, priority, deduplication_id, last_heartbeat_at, plan_run_id, plan_step_id, plan_step_name, plan_graph)
            SELECT @id, @job_name, @status, @arguments, @result, @error, @progress, @created_at, @started_at, @completed_at, @cancelled_at, @node_name, @attempt, @trace_id, @span_id, @parent_run_id, @retry_of_run_id, @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id, @last_heartbeat_at, @plan_run_id, @plan_step_id, @plan_step_name, @plan_graph
            WHERE (SELECT is_enabled FROM jobs WHERE name = @job_name) = 1
              AND (SELECT COUNT(*) FROM runs WHERE job_name = @job_name AND status IN (0, 1))
                < COALESCE((SELECT max_concurrency FROM jobs WHERE name = @job_name), 1)
            """;

        await using var cmd = new SqliteCommand(sql, conn) { Transaction = tx };
        AddRunParameters(cmd, run);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);

        await tx.CommitAsync(cancellationToken);
        return rows > 0;
    }

    public async Task<JobRun?> GetRunAsync(string id, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("SELECT * FROM runs WHERE id = @id", conn);
        cmd.Parameters.AddWithValue("@id", id);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadRun(reader) : null;
    }

    public async Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, CancellationToken cancellationToken = default)
    {
        var where = "WHERE 1=1";
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand { Connection = conn };

        if (filter.JobName is not null)
        {
            if (filter.ExactJobName)
            {
                where += " AND job_name = @job_name";
                cmd.Parameters.AddWithValue("@job_name", filter.JobName);
            }
            else
            {
                where += " AND job_name LIKE @job_name ESCAPE '\\'";
                cmd.Parameters.AddWithValue("@job_name", $"%{EscapeLike(filter.JobName)}%");
            }
        }
        if (filter.Status is not null)
        {
            where += " AND status = @status";
            cmd.Parameters.AddWithValue("@status", (int)filter.Status.Value);
        }
        if (filter.NodeName is not null)
        {
            where += " AND node_name = @node_name";
            cmd.Parameters.AddWithValue("@node_name", filter.NodeName);
        }
        if (filter.ParentRunId is not null)
        {
            where += " AND parent_run_id = @parent_run_id";
            cmd.Parameters.AddWithValue("@parent_run_id", filter.ParentRunId);
        }
        if (filter.RetryOfRunId is not null)
        {
            where += " AND retry_of_run_id = @retry_of_run_id";
            cmd.Parameters.AddWithValue("@retry_of_run_id", filter.RetryOfRunId);
        }
        if (filter.RerunOfRunId is not null)
        {
            where += " AND rerun_of_run_id = @rerun_of_run_id";
            cmd.Parameters.AddWithValue("@rerun_of_run_id", filter.RerunOfRunId);
        }
        if (filter.CreatedAfter is not null)
        {
            where += " AND created_at >= @created_after";
            cmd.Parameters.AddWithValue("@created_after", FormatDto(filter.CreatedAfter.Value));
        }
        if (filter.CreatedBefore is not null)
        {
            where += " AND created_at <= @created_before";
            cmd.Parameters.AddWithValue("@created_before", FormatDto(filter.CreatedBefore.Value));
        }
        if (filter.PlanRunId is not null)
        {
            where += " AND plan_run_id = @plan_run_id";
            cmd.Parameters.AddWithValue("@plan_run_id", filter.PlanRunId);
        }
        if (filter.PlanStepName is not null)
        {
            where += " AND plan_step_name = @plan_step_name";
            cmd.Parameters.AddWithValue("@plan_step_name", filter.PlanStepName);
        }

        var orderCol = filter.OrderBy switch
        {
            RunOrderBy.StartedAt => "started_at",
            RunOrderBy.CompletedAt => "completed_at",
            _ => "created_at"
        };

        // Get total count
        cmd.CommandText = $"SELECT COUNT(*) FROM runs {where}";
        var totalCount = Convert.ToInt32(await cmd.ExecuteScalarAsync(cancellationToken));

        // Get page
        cmd.CommandText = $"SELECT * FROM runs {where} ORDER BY {orderCol} DESC LIMIT @take OFFSET @skip";
        cmd.Parameters.AddWithValue("@skip", filter.Skip);
        cmd.Parameters.AddWithValue("@take", filter.Take);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var items = new List<JobRun>();
        while (await reader.ReadAsync(cancellationToken))
            items.Add(ReadRun(reader));

        return new PagedResult<JobRun> { Items = items, TotalCount = totalCount };
    }

    public async Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        var sql = """
            UPDATE runs SET
                status = @status,
                arguments = @arguments,
                result = @result,
                error = @error,
                progress = @progress,
                started_at = @started_at,
                completed_at = @completed_at,
                cancelled_at = @cancelled_at,
                node_name = @node_name,
                attempt = @attempt,
                trace_id = @trace_id,
                span_id = @span_id,
                parent_run_id = @parent_run_id,
                retry_of_run_id = @retry_of_run_id,
                rerun_of_run_id = @rerun_of_run_id,
                not_before = @not_before,
                not_after = @not_after,
                priority = @priority,
                deduplication_id = @deduplication_id,
                last_heartbeat_at = @last_heartbeat_at,
                plan_run_id = @plan_run_id,
                plan_step_id = @plan_step_id,
                plan_step_name = @plan_step_name,
                plan_graph = @plan_graph
            WHERE id = @id
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(sql, conn);
        AddRunParameters(cmd, run);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<bool> TryUpdateRunStatusAsync(JobRun run, JobStatus expectedStatus, CancellationToken cancellationToken = default)
    {
        var sql = """
            UPDATE runs SET
                status = @status,
                error = @error,
                result = @result,
                progress = @progress,
                completed_at = @completed_at,
                cancelled_at = @cancelled_at
            WHERE id = @id AND status = @expected_status
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", run.Id);
        cmd.Parameters.AddWithValue("@status", (int)run.Status);
        cmd.Parameters.AddWithValue("@error", (object?)run.Error ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@result", (object?)run.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@progress", run.Progress);
        cmd.Parameters.AddWithValue("@completed_at", run.CompletedAt.HasValue ? FormatDto(run.CompletedAt.Value) : DBNull.Value);
        cmd.Parameters.AddWithValue("@cancelled_at", run.CancelledAt.HasValue ? FormatDto(run.CancelledAt.Value) : DBNull.Value);
        cmd.Parameters.AddWithValue("@expected_status", (int)expectedStatus);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0;
    }

    public async Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, IReadOnlyCollection<string> queueNames, CancellationToken cancellationToken = default)
    {
        if (registeredJobNames.Count == 0)
            return null;

        await using var conn = await OpenAsync(cancellationToken);
        await using var tx = conn.BeginTransaction();

        var now = FormatDto(DateTimeOffset.UtcNow);

        // Build IN clauses for job names and queue names
        var jobNameParams = BuildInClause("@jn", registeredJobNames, out var jobNameClause);
        var queueNameParams = BuildInClause("@qn", queueNames, out var queueNameClause);

        var sql = $"""
            SELECT r.id, r.job_name
            FROM runs r
            JOIN jobs j ON j.name = r.job_name
            LEFT JOIN queues q ON q.name = COALESCE(j.queue, 'default')
            WHERE r.status = 0
              AND r.not_before <= @now
              AND (r.not_after IS NULL OR r.not_after > @now)
              AND r.job_name IN ({jobNameClause})
              AND COALESCE(j.queue, 'default') IN ({queueNameClause})
              AND COALESCE(q.is_paused, 0) = 0
              AND (
                  j.max_concurrency IS NULL
                  OR (SELECT COUNT(*) FROM runs r2 WHERE r2.job_name = r.job_name AND r2.status = 1)
                     < j.max_concurrency
              )
              AND (
                  q.max_concurrency IS NULL
                  OR (SELECT COUNT(*) FROM runs r2
                      JOIN jobs j2 ON j2.name = r2.job_name
                      WHERE r2.status = 1 AND COALESCE(j2.queue, 'default') = COALESCE(j.queue, 'default'))
                     < q.max_concurrency
              )
              AND (
                  j.rate_limit_name IS NULL
                  OR EXISTS (SELECT 1 FROM rate_limits rl
                      WHERE rl.name = j.rate_limit_name
                        AND CASE
                            WHEN julianday(rl.window_start, 'utc') + rl.window_duration_ms / 86400000.0 <= julianday(@now, 'utc') THEN 1
                            WHEN rl.type = 0 THEN rl.current_count < rl.max_permits
                            WHEN rl.type = 1 THEN
                                COALESCE(rl.previous_count, 0) * MAX(0, 1.0 - (julianday(@now, 'utc') - julianday(rl.window_start, 'utc')) * 86400000.0 / rl.window_duration_ms)
                                + rl.current_count + 1 <= rl.max_permits
                            ELSE rl.current_count < rl.max_permits
                        END = 1)
              )
              AND (
                  q.rate_limit_name IS NULL
                  OR EXISTS (SELECT 1 FROM rate_limits rl
                      WHERE rl.name = q.rate_limit_name
                        AND CASE
                            WHEN julianday(rl.window_start, 'utc') + rl.window_duration_ms / 86400000.0 <= julianday(@now, 'utc') THEN 1
                            WHEN rl.type = 0 THEN rl.current_count < rl.max_permits
                            WHEN rl.type = 1 THEN
                                COALESCE(rl.previous_count, 0) * MAX(0, 1.0 - (julianday(@now, 'utc') - julianday(rl.window_start, 'utc')) * 86400000.0 / rl.window_duration_ms)
                                + rl.current_count + 1 <= rl.max_permits
                            ELSE rl.current_count < rl.max_permits
                        END = 1)
              )
            ORDER BY COALESCE(q.priority, 0) DESC, r.priority DESC, r.not_before, r.created_at
            LIMIT 1
            """;

        await using var selectCmd = new SqliteCommand(sql, conn, tx);
        selectCmd.Parameters.AddWithValue("@now", now);
        foreach (var p in jobNameParams) selectCmd.Parameters.Add(p);
        foreach (var p in queueNameParams) selectCmd.Parameters.Add(p);

        string? runId;
        string? jobName;
        await using (var reader = await selectCmd.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                await tx.CommitAsync(cancellationToken);
                return null;
            }
            runId = reader.GetString(0);
            jobName = reader.GetString(1);
        }

        // Acquire job-level rate limit
        await AcquireRateLimitIfNeededAsync(conn, tx,
            $"SELECT rate_limit_name FROM jobs WHERE name = @name",
            [new SqliteParameter("@name", jobName)],
            now, cancellationToken);

        // Acquire queue-level rate limit (if different from job-level)
        await AcquireRateLimitIfNeededAsync(conn, tx,
            $"SELECT q.rate_limit_name FROM jobs j LEFT JOIN queues q ON q.name = COALESCE(j.queue, 'default') WHERE j.name = @name AND q.rate_limit_name IS NOT NULL AND q.rate_limit_name != COALESCE(j.rate_limit_name, '')",
            [new SqliteParameter("@name", jobName)],
            now, cancellationToken);

        // Claim the run
        var updateSql = """
            UPDATE runs SET status = 1, node_name = @node_name, started_at = @now, last_heartbeat_at = @now
            WHERE id = @id
            """;
        await using var updateCmd = new SqliteCommand(updateSql, conn, tx);
        updateCmd.Parameters.AddWithValue("@id", runId);
        updateCmd.Parameters.AddWithValue("@node_name", nodeName);
        updateCmd.Parameters.AddWithValue("@now", now);
        await updateCmd.ExecuteNonQueryAsync(cancellationToken);

        // Read the updated run
        await using var readCmd = new SqliteCommand("SELECT * FROM runs WHERE id = @id", conn, tx);
        readCmd.Parameters.AddWithValue("@id", runId);

        JobRun? result;
        await using (var reader = await readCmd.ExecuteReaderAsync(cancellationToken))
        {
            result = await reader.ReadAsync(cancellationToken) ? ReadRun(reader) : null;
        }

        await tx.CommitAsync(cancellationToken);
        return result;
    }

    private static async Task AcquireRateLimitIfNeededAsync(SqliteConnection conn, SqliteTransaction tx, string lookupSql, SqliteParameter[] lookupParams, string now, CancellationToken cancellationToken)
    {
        await using var lookupCmd = new SqliteCommand(lookupSql, conn, tx);
        foreach (var p in lookupParams) lookupCmd.Parameters.Add(p);
        var rlName = await lookupCmd.ExecuteScalarAsync(cancellationToken) as string;

        if (rlName is null) return;

        var acquireSql = """
            UPDATE rate_limits SET
                previous_count = CASE WHEN julianday(window_start, 'utc') + window_duration_ms / 86400000.0 <= julianday(@now, 'utc') THEN current_count ELSE previous_count END,
                current_count = CASE WHEN julianday(window_start, 'utc') + window_duration_ms / 86400000.0 <= julianday(@now, 'utc') THEN 1 ELSE current_count + 1 END,
                window_start = CASE WHEN julianday(window_start, 'utc') + window_duration_ms / 86400000.0 <= julianday(@now, 'utc') THEN @now ELSE window_start END
            WHERE name = @name
            """;

        await using var acquireCmd = new SqliteCommand(acquireSql, conn, tx);
        acquireCmd.Parameters.AddWithValue("@name", rlName);
        acquireCmd.Parameters.AddWithValue("@now", now);
        await acquireCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Rate Limits --

    public async Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken cancellationToken = default)
    {
        var sql = """
            INSERT INTO rate_limits (name, type, max_permits, window_duration_ms, window_start)
            VALUES (@name, @type, @max_permits, @window_duration_ms, @window_start)
            ON CONFLICT (name) DO UPDATE SET
                type = excluded.type,
                max_permits = excluded.max_permits,
                window_duration_ms = excluded.window_duration_ms
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(sql, conn);
        cmd.Parameters.AddWithValue("@name", rateLimit.Name);
        cmd.Parameters.AddWithValue("@type", (int)rateLimit.Type);
        cmd.Parameters.AddWithValue("@max_permits", rateLimit.MaxPermits);
        cmd.Parameters.AddWithValue("@window_duration_ms", rateLimit.Window.TotalMilliseconds);
        cmd.Parameters.AddWithValue("@window_start", FormatDto(DateTimeOffset.UtcNow));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<bool> TryAcquireRateLimitAsync(string name, CancellationToken cancellationToken = default)
    {
        var now = FormatDto(DateTimeOffset.UtcNow);

        var sql = """
            UPDATE rate_limits SET
                previous_count = CASE WHEN julianday(window_start, 'utc') + window_duration_ms / 86400000.0 <= julianday(@now, 'utc') THEN current_count ELSE previous_count END,
                current_count = CASE WHEN julianday(window_start, 'utc') + window_duration_ms / 86400000.0 <= julianday(@now, 'utc') THEN 1 ELSE current_count + 1 END,
                window_start = CASE WHEN julianday(window_start, 'utc') + window_duration_ms / 86400000.0 <= julianday(@now, 'utc') THEN @now ELSE window_start END
            WHERE name = @name
              AND CASE
                  WHEN julianday(window_start, 'utc') + window_duration_ms / 86400000.0 <= julianday(@now, 'utc') THEN 1
                  WHEN type = 0 THEN current_count < max_permits
                  WHEN type = 1 THEN
                      COALESCE(previous_count, 0) * MAX(0, 1.0 - (julianday(@now, 'utc') - julianday(window_start, 'utc')) * 86400000.0 / window_duration_ms)
                      + current_count + 1 <= max_permits
                  ELSE current_count < max_permits
              END
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(sql, conn);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@now", now);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0;
    }

    // -- Queues --

    public async Task UpsertQueueAsync(QueueDefinition queue, CancellationToken cancellationToken = default)
    {
        var sql = """
            INSERT INTO queues (name, priority, max_concurrency, is_paused, rate_limit_name, last_heartbeat_at)
            VALUES (@name, @priority, @max_concurrency, @is_paused, @rate_limit_name, @last_heartbeat_at)
            ON CONFLICT (name) DO UPDATE SET
                priority = excluded.priority,
                max_concurrency = excluded.max_concurrency,
                rate_limit_name = excluded.rate_limit_name,
                last_heartbeat_at = @last_heartbeat_at
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(sql, conn);
        cmd.Parameters.AddWithValue("@name", queue.Name);
        cmd.Parameters.AddWithValue("@priority", queue.Priority);
        cmd.Parameters.AddWithValue("@max_concurrency", queue.MaxConcurrency.HasValue ? queue.MaxConcurrency.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("@is_paused", queue.IsPaused ? 1 : 0);
        cmd.Parameters.AddWithValue("@rate_limit_name", (object?)queue.RateLimitName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@last_heartbeat_at", FormatDto(DateTimeOffset.UtcNow));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<QueueDefinition?> GetQueueAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("SELECT * FROM queues WHERE name = @name", conn);
        cmd.Parameters.AddWithValue("@name", name);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadQueue(reader) : null;
    }

    public async Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("SELECT * FROM queues ORDER BY name", conn);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var queues = new List<QueueDefinition>();
        while (await reader.ReadAsync(cancellationToken))
            queues.Add(ReadQueue(reader));
        return queues;
    }

    public async Task SetQueuePausedAsync(string name, bool isPaused, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("UPDATE queues SET is_paused = @is_paused WHERE name = @name", conn);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@is_paused", isPaused ? 1 : 0);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Nodes --

    public async Task RegisterNodeAsync(NodeInfo node, CancellationToken cancellationToken = default)
    {
        var sql = """
            INSERT INTO nodes (name, started_at, last_heartbeat_at, running_count, registered_job_names, registered_queue_names)
            VALUES (@name, @started_at, @last_heartbeat_at, @running_count, @registered_job_names, @registered_queue_names)
            ON CONFLICT (name) DO UPDATE SET
                started_at = excluded.started_at,
                last_heartbeat_at = excluded.last_heartbeat_at,
                running_count = excluded.running_count,
                registered_job_names = excluded.registered_job_names,
                registered_queue_names = excluded.registered_queue_names
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(sql, conn);
        cmd.Parameters.AddWithValue("@name", node.Name);
        cmd.Parameters.AddWithValue("@started_at", FormatDto(node.StartedAt));
        cmd.Parameters.AddWithValue("@last_heartbeat_at", FormatDto(node.LastHeartbeatAt));
        cmd.Parameters.AddWithValue("@running_count", node.RunningCount);
        cmd.Parameters.AddWithValue("@registered_job_names", JsonSerializer.Serialize(node.RegisteredJobNames, JsonOptions));
        cmd.Parameters.AddWithValue("@registered_queue_names", JsonSerializer.Serialize(node.RegisteredQueueNames, JsonOptions));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, IReadOnlyCollection<string> registeredQueueNames, CancellationToken cancellationToken = default)
    {
        var now = FormatDto(DateTimeOffset.UtcNow);

        var sql = """
            UPDATE nodes SET
                last_heartbeat_at = @now,
                running_count = (SELECT COUNT(*) FROM runs WHERE node_name = @name AND status = 1),
                registered_job_names = @registered_job_names,
                registered_queue_names = @registered_queue_names
            WHERE name = @name
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(sql, conn);
        cmd.Parameters.AddWithValue("@name", nodeName);
        cmd.Parameters.AddWithValue("@now", now);
        cmd.Parameters.AddWithValue("@registered_job_names", JsonSerializer.Serialize(registeredJobNames, JsonOptions));
        cmd.Parameters.AddWithValue("@registered_queue_names", JsonSerializer.Serialize(registeredQueueNames, JsonOptions));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task RemoveNodeAsync(string nodeName, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("DELETE FROM nodes WHERE name = @name", conn);
        cmd.Parameters.AddWithValue("@name", nodeName);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<NodeInfo?> GetNodeAsync(string nodeName, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("SELECT * FROM nodes WHERE name = @name", conn);
        cmd.Parameters.AddWithValue("@name", nodeName);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadNode(reader) : null;
    }

    public async Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("SELECT * FROM nodes ORDER BY name", conn);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var nodes = new List<NodeInfo>();
        while (await reader.ReadAsync(cancellationToken))
            nodes.Add(ReadNode(reader));
        return nodes;
    }

    // -- Run Events --

    public async Task AppendEventAsync(RunEvent evt, CancellationToken cancellationToken = default)
    {
        var sql = """
            INSERT INTO run_events (run_id, event_type, payload, created_at)
            VALUES (@run_id, @event_type, @payload, @created_at)
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(sql, conn);
        cmd.Parameters.AddWithValue("@run_id", evt.RunId);
        cmd.Parameters.AddWithValue("@event_type", (int)evt.EventType);
        cmd.Parameters.AddWithValue("@payload", evt.Payload);
        cmd.Parameters.AddWithValue("@created_at", FormatDto(evt.CreatedAt));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default)
    {
        if (events.Count == 0) return;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand { Connection = conn };

        var sb = new System.Text.StringBuilder();
        sb.Append("INSERT INTO run_events (run_id, event_type, payload, created_at) VALUES ");

        for (var i = 0; i < events.Count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append($"(@r{i},@t{i},@p{i},@c{i})");
            cmd.Parameters.AddWithValue($"@r{i}", events[i].RunId);
            cmd.Parameters.AddWithValue($"@t{i}", (int)events[i].EventType);
            cmd.Parameters.AddWithValue($"@p{i}", events[i].Payload);
            cmd.Parameters.AddWithValue($"@c{i}", FormatDto(events[i].CreatedAt));
        }

        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0, RunEventType[]? types = null, CancellationToken cancellationToken = default)
    {
        var sql = "SELECT id, run_id, event_type, payload, created_at FROM run_events WHERE run_id = @run_id AND id > @since_id";

        if (types is { Length: > 0 })
        {
            var typeParams = BuildInClause("@et", types.Select(t => (int)t), out var typeClause);
            sql += $" AND event_type IN ({typeClause})";
            sql += " ORDER BY id";

            await using var conn = await OpenAsync(cancellationToken);
            await using var cmd = new SqliteCommand(sql, conn);
            cmd.Parameters.AddWithValue("@run_id", runId);
            cmd.Parameters.AddWithValue("@since_id", sinceId);
            foreach (var p in typeParams) cmd.Parameters.Add(p);

            return await ReadEventsAsync(cmd, cancellationToken);
        }
        else
        {
            sql += " ORDER BY id";

            await using var conn = await OpenAsync(cancellationToken);
            await using var cmd = new SqliteCommand(sql, conn);
            cmd.Parameters.AddWithValue("@run_id", runId);
            cmd.Parameters.AddWithValue("@since_id", sinceId);

            return await ReadEventsAsync(cmd, cancellationToken);
        }
    }

    private static async Task<IReadOnlyList<RunEvent>> ReadEventsAsync(SqliteCommand cmd, CancellationToken cancellationToken)
    {
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var result = new List<RunEvent>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new RunEvent
            {
                Id = reader.GetInt64(0),
                RunId = reader.GetString(1),
                EventType = (RunEventType)reader.GetInt16(2),
                Payload = reader.GetString(3),
                CreatedAt = ParseDto(reader.GetString(4))
            });
        }
        return result;
    }

    // -- Heartbeat & Inactive Runs --

    public async Task HeartbeatRunsAsync(IReadOnlyCollection<string> runIds, CancellationToken cancellationToken = default)
    {
        if (runIds.Count == 0) return;

        var now = FormatDto(DateTimeOffset.UtcNow);
        var idParams = BuildInClause("@rid", runIds, out var idClause);

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand($"UPDATE runs SET last_heartbeat_at = @now WHERE id IN ({idClause}) AND status = 1", conn);
        cmd.Parameters.AddWithValue("@now", now);
        foreach (var p in idParams) cmd.Parameters.Add(p);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<JobRun>> GetInactiveRunsAsync(TimeSpan threshold, CancellationToken cancellationToken = default)
    {
        var cutoff = FormatDto(DateTimeOffset.UtcNow - threshold);

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(
            "SELECT * FROM runs WHERE status = 1 AND (last_heartbeat_at IS NULL OR last_heartbeat_at < @cutoff)", conn);
        cmd.Parameters.AddWithValue("@cutoff", cutoff);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var runs = new List<JobRun>();
        while (await reader.ReadAsync(cancellationToken))
            runs.Add(ReadRun(reader));
        return runs;
    }

    // -- Trace --

    public async Task<IReadOnlyList<JobRun>> GetRunTraceAsync(string runId, int limit = 200, CancellationToken cancellationToken = default)
    {
        var sql = """
            WITH RECURSIVE ancestors AS (
                SELECT r.* FROM runs r WHERE r.id = @id
                UNION ALL
                SELECT p.* FROM runs p JOIN ancestors a ON a.parent_run_id = p.id
            ),
            root AS (
                SELECT a.id FROM ancestors a
                WHERE NOT EXISTS (SELECT 1 FROM ancestors b WHERE b.id = a.parent_run_id)
                LIMIT 1
            ),
            descendants AS (
                SELECT r.* FROM runs r JOIN root ON r.id = root.id
                UNION ALL
                SELECT c.* FROM runs c JOIN descendants d ON c.parent_run_id = d.id
            )
            SELECT * FROM descendants ORDER BY created_at LIMIT @limit
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(sql, conn);
        cmd.Parameters.AddWithValue("@id", runId);
        cmd.Parameters.AddWithValue("@limit", limit);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var runs = new List<JobRun>();
        while (await reader.ReadAsync(cancellationToken))
            runs.Add(ReadRun(reader));
        return runs;
    }

    // -- Dashboard Stats --

    public async Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60, CancellationToken cancellationToken = default)
    {
        if (bucketMinutes <= 0) bucketMinutes = 60;

        await using var conn = await OpenAsync(cancellationToken);

        var sinceFilter = since is not null ? " AND created_at >= @since" : "";

        var statsSql = $"""
            SELECT
                (SELECT COUNT(*) FROM jobs) AS total_jobs,
                (SELECT COUNT(*) FROM nodes) AS node_count,
                COUNT(*) AS total_runs,
                SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS running,
                SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END) AS cancelled,
                SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END) AS dead_letter,
                SUM(CASE WHEN status = 6 THEN 1 ELSE 0 END) AS skipped
            FROM runs WHERE 1=1{sinceFilter}
            """;

        await using var statsCmd = new SqliteCommand(statsSql, conn);
        if (since is not null)
            statsCmd.Parameters.AddWithValue("@since", FormatDto(since.Value));

        int totalJobs, nodeCount, totalRuns, activeRuns;
        double successRate;
        var runsByStatus = new Dictionary<string, int>();

        await using (var reader = await statsCmd.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            totalJobs = Convert.ToInt32(reader.GetValue(0));
            nodeCount = Convert.ToInt32(reader.GetValue(1));
            totalRuns = Convert.ToInt32(reader.GetValue(2));
            var pending = Convert.ToInt32(reader.GetValue(3));
            var running = Convert.ToInt32(reader.GetValue(4));
            var completed = Convert.ToInt32(reader.GetValue(5));
            var failed = Convert.ToInt32(reader.GetValue(6));
            var cancelled = Convert.ToInt32(reader.GetValue(7));
            var deadLetter = Convert.ToInt32(reader.GetValue(8));
            var skipped = Convert.ToInt32(reader.GetValue(9));

            activeRuns = running;
            var totalFinished = completed + failed;
            successRate = totalFinished > 0 ? (double)completed / totalFinished * 100 : 0;

            if (pending > 0) runsByStatus[JobStatus.Pending.ToString()] = pending;
            if (running > 0) runsByStatus[JobStatus.Running.ToString()] = running;
            if (completed > 0) runsByStatus[JobStatus.Completed.ToString()] = completed;
            if (failed > 0) runsByStatus[JobStatus.Failed.ToString()] = failed;
            if (cancelled > 0) runsByStatus[JobStatus.Cancelled.ToString()] = cancelled;
            if (deadLetter > 0) runsByStatus[JobStatus.DeadLetter.ToString()] = deadLetter;
            if (skipped > 0) runsByStatus[JobStatus.Skipped.ToString()] = skipped;
        }

        // Recent runs
        await using var recentCmd = new SqliteCommand(
            $"SELECT * FROM runs WHERE 1=1{sinceFilter} ORDER BY created_at DESC LIMIT 10", conn);
        if (since is not null)
            recentCmd.Parameters.AddWithValue("@since", FormatDto(since.Value));

        var recentRuns = new List<JobRun>();
        await using (var reader = await recentCmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
                recentRuns.Add(ReadRun(reader));
        }

        // Timeline
        var timeline = new List<TimelineBucket>();
        if (since is not null)
        {
            var bucketSeconds = bucketMinutes * 60;

            var tlSql = $"""
                SELECT
                    CAST(CAST(strftime('%s', created_at) AS INTEGER) / @bucket_seconds * @bucket_seconds AS INTEGER) AS bucket_epoch,
                    SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS running,
                    SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END) AS cancelled,
                    SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END) AS dead_letter,
                    SUM(CASE WHEN status = 6 THEN 1 ELSE 0 END) AS skipped
                FROM runs
                WHERE created_at >= @since
                GROUP BY bucket_epoch
                ORDER BY bucket_epoch
                """;

            await using var tlCmd = new SqliteCommand(tlSql, conn);
            tlCmd.Parameters.AddWithValue("@bucket_seconds", bucketSeconds);
            tlCmd.Parameters.AddWithValue("@since", FormatDto(since.Value));

            var bucketMap = new Dictionary<long, TimelineBucket>();
            await using (var reader = await tlCmd.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var epoch = reader.GetInt64(0);
                    var dto = DateTimeOffset.FromUnixTimeSeconds(epoch);
                    bucketMap[dto.UtcTicks] = new TimelineBucket
                    {
                        Timestamp = dto,
                        Pending = Convert.ToInt32(reader.GetValue(1)),
                        Running = Convert.ToInt32(reader.GetValue(2)),
                        Completed = Convert.ToInt32(reader.GetValue(3)),
                        Failed = Convert.ToInt32(reader.GetValue(4)),
                        Cancelled = Convert.ToInt32(reader.GetValue(5)),
                        DeadLetter = Convert.ToInt32(reader.GetValue(6)),
                        Skipped = Convert.ToInt32(reader.GetValue(7))
                    };
                }
            }

            // Fill gaps
            var bucketSpanTicks = TimeSpan.FromMinutes(bucketMinutes).Ticks;
            var startTicks = since.Value.UtcTicks;
            startTicks -= startTicks % bucketSpanTicks;
            var endTicks = DateTimeOffset.UtcNow.UtcTicks;
            endTicks -= endTicks % bucketSpanTicks;

            for (var t = startTicks; t <= endTicks; t += bucketSpanTicks)
            {
                if (!bucketMap.TryGetValue(t, out var bucket))
                    bucket = new TimelineBucket { Timestamp = new DateTimeOffset(t, TimeSpan.Zero) };
                timeline.Add(bucket);
            }
        }

        return new DashboardStats
        {
            TotalJobs = totalJobs,
            TotalRuns = totalRuns,
            ActiveRuns = activeRuns,
            SuccessRate = successRate,
            NodeCount = nodeCount,
            RecentRuns = recentRuns,
            RunsByStatus = runsByStatus,
            Timeline = timeline
        };
    }

    // -- Job Stats --

    public async Task<JobStats> GetJobStatsAsync(string jobName, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("""
            SELECT
                COUNT(*) AS total_runs,
                SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS succeeded,
                SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) AS failed,
                AVG(CASE WHEN status IN (2, 3) AND started_at IS NOT NULL AND completed_at IS NOT NULL
                    THEN (julianday(completed_at) - julianday(started_at)) * 86400.0
                    ELSE NULL END) AS avg_duration_secs,
                MAX(created_at) AS last_run_at
            FROM runs WHERE job_name = @name
            """, conn);
        cmd.Parameters.AddWithValue("@name", jobName);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);

        var total = Convert.ToInt32(reader.GetValue(0));
        var succeeded = Convert.ToInt32(reader.GetValue(1));
        var failed = Convert.ToInt32(reader.GetValue(2));
        var totalFinished = succeeded + failed;
        var avgDurationSecs = reader.IsDBNull(3) ? (double?)null : reader.GetDouble(3);
        var lastRunAt = reader.IsDBNull(4) ? (DateTimeOffset?)null : ParseDto(reader.GetString(4));

        return new JobStats
        {
            TotalRuns = total,
            SucceededRuns = succeeded,
            FailedRuns = failed,
            SuccessRate = totalFinished > 0 ? (double)succeeded / totalFinished * 100 : 0,
            AvgDuration = avgDurationSecs is not null ? TimeSpan.FromSeconds(avgDurationSecs.Value) : null,
            LastRunAt = lastRunAt
        };
    }

    // -- Queue Stats --

    public async Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand("""
            SELECT COALESCE(j.queue, 'default') AS queue_name,
                   SUM(CASE WHEN r.status = 0 THEN 1 ELSE 0 END) AS pending,
                   SUM(CASE WHEN r.status = 1 THEN 1 ELSE 0 END) AS running
            FROM runs r
            JOIN jobs j ON j.name = r.job_name
            WHERE r.status IN (0, 1)
            GROUP BY COALESCE(j.queue, 'default')
            """, conn);

        var stats = new Dictionary<string, QueueStats>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            stats[reader.GetString(0)] = new QueueStats
            {
                PendingCount = Convert.ToInt32(reader.GetValue(1)),
                RunningCount = Convert.ToInt32(reader.GetValue(2))
            };
        }
        return stats;
    }

    // -- Expiration --

    public async Task<int> CancelExpiredRunsAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        var now = FormatDto(DateTimeOffset.UtcNow);
        await using var cmd = new SqliteCommand("""
            UPDATE runs
            SET status = 4, cancelled_at = @now
            WHERE status = 0
              AND not_after IS NOT NULL
              AND not_after < @now
            """, conn);
        cmd.Parameters.AddWithValue("@now", now);
        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Purge --

    public async Task<int> PurgeRunsAsync(DateTimeOffset completedBefore, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var tx = conn.BeginTransaction();

        var before = FormatDto(completedBefore);

        // Delete events for runs being purged
        await using (var evtCmd = new SqliteCommand("""
            DELETE FROM run_events WHERE run_id IN (
                SELECT id FROM runs
                WHERE (status IN (2, 3, 4, 5, 6) AND completed_at < @before)
                   OR (status = 0 AND not_before < @before)
            )
            """, conn, tx))
        {
            evtCmd.Parameters.AddWithValue("@before", before);
            await evtCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // Delete runs
        await using var cmd = new SqliteCommand("""
            DELETE FROM runs
            WHERE (status IN (2, 3, 4, 5, 6) AND completed_at < @before)
               OR (status = 0 AND not_before < @before)
            """, conn, tx);
        cmd.Parameters.AddWithValue("@before", before);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);

        await tx.CommitAsync(cancellationToken);
        return rows;
    }

    public async Task<int> PurgeJobsAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(
            "DELETE FROM jobs WHERE last_heartbeat_at IS NOT NULL AND last_heartbeat_at < @before", conn);
        cmd.Parameters.AddWithValue("@before", FormatDto(heartbeatBefore));
        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> PurgeNodesAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(
            "DELETE FROM nodes WHERE last_heartbeat_at < @before", conn);
        cmd.Parameters.AddWithValue("@before", FormatDto(heartbeatBefore));
        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> PurgeQueuesAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqliteCommand(
            "DELETE FROM queues WHERE last_heartbeat_at IS NOT NULL AND last_heartbeat_at < @before", conn);
        cmd.Parameters.AddWithValue("@before", FormatDto(heartbeatBefore));
        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Helpers --

    private async Task<SqliteConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var conn = new SqliteConnection(sqliteOptions.ConnectionString);
        await conn.OpenAsync(cancellationToken);

        await using var pragmaCmd = new SqliteCommand("PRAGMA busy_timeout=5000;", conn);
        await pragmaCmd.ExecuteNonQueryAsync(cancellationToken);

        return conn;
    }

    private async Task InsertRunAsync(SqliteConnection conn, JobRun run, CancellationToken cancellationToken, SqliteTransaction? tx = null)
    {
        var sql = """
            INSERT INTO runs (id, job_name, status, arguments, result, error, progress, created_at, started_at, completed_at, cancelled_at, node_name, attempt, trace_id, span_id, parent_run_id, retry_of_run_id, rerun_of_run_id, not_before, not_after, priority, deduplication_id, last_heartbeat_at, plan_run_id, plan_step_id, plan_step_name, plan_graph)
            VALUES (@id, @job_name, @status, @arguments, @result, @error, @progress, @created_at, @started_at, @completed_at, @cancelled_at, @node_name, @attempt, @trace_id, @span_id, @parent_run_id, @retry_of_run_id, @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id, @last_heartbeat_at, @plan_run_id, @plan_step_id, @plan_step_name, @plan_graph)
            """;

        await using var cmd = new SqliteCommand(sql, conn) { Transaction = tx };
        AddRunParameters(cmd, run);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddRunParameters(SqliteCommand cmd, JobRun run)
    {
        cmd.Parameters.AddWithValue("@id", run.Id);
        cmd.Parameters.AddWithValue("@job_name", run.JobName);
        cmd.Parameters.AddWithValue("@status", (int)run.Status);
        cmd.Parameters.AddWithValue("@arguments", (object?)run.Arguments ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@result", (object?)run.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@error", (object?)run.Error ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@progress", run.Progress);
        cmd.Parameters.AddWithValue("@created_at", FormatDto(run.CreatedAt));
        cmd.Parameters.AddWithValue("@started_at", run.StartedAt.HasValue ? FormatDto(run.StartedAt.Value) : DBNull.Value);
        cmd.Parameters.AddWithValue("@completed_at", run.CompletedAt.HasValue ? FormatDto(run.CompletedAt.Value) : DBNull.Value);
        cmd.Parameters.AddWithValue("@cancelled_at", run.CancelledAt.HasValue ? FormatDto(run.CancelledAt.Value) : DBNull.Value);
        cmd.Parameters.AddWithValue("@node_name", (object?)run.NodeName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@attempt", run.Attempt);
        cmd.Parameters.AddWithValue("@trace_id", (object?)run.TraceId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@span_id", (object?)run.SpanId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@parent_run_id", (object?)run.ParentRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@retry_of_run_id", (object?)run.RetryOfRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@rerun_of_run_id", (object?)run.RerunOfRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@not_before", FormatDto(run.NotBefore));
        cmd.Parameters.AddWithValue("@not_after", run.NotAfter.HasValue ? FormatDto(run.NotAfter.Value) : DBNull.Value);
        cmd.Parameters.AddWithValue("@priority", run.Priority);
        cmd.Parameters.AddWithValue("@deduplication_id", (object?)run.DeduplicationId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@last_heartbeat_at", run.LastHeartbeatAt.HasValue ? FormatDto(run.LastHeartbeatAt.Value) : DBNull.Value);
        cmd.Parameters.AddWithValue("@plan_run_id", (object?)run.PlanRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@plan_step_id", (object?)run.PlanStepId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@plan_step_name", (object?)run.PlanStepName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@plan_graph", (object?)run.PlanGraph ?? DBNull.Value);
    }

    private JobDefinition ReadJob(SqliteDataReader reader)
    {
        var retryPolicyJson = reader.IsDBNull(reader.GetOrdinal("retry_policy"))
            ? null
            : reader.GetString(reader.GetOrdinal("retry_policy"));

        return new JobDefinition
        {
            Name = reader.GetString(reader.GetOrdinal("name")),
            Description = reader.IsDBNull(reader.GetOrdinal("description")) ? null : reader.GetString(reader.GetOrdinal("description")),
            Tags = JsonSerializer.Deserialize<string[]>(reader.GetString(reader.GetOrdinal("tags")), JsonOptions) ?? [],
            CronExpression = reader.IsDBNull(reader.GetOrdinal("cron_expression")) ? null : reader.GetString(reader.GetOrdinal("cron_expression")),
            TimeZoneId = reader.IsDBNull(reader.GetOrdinal("time_zone_id")) ? null : reader.GetString(reader.GetOrdinal("time_zone_id")),
            Timeout = reader.IsDBNull(reader.GetOrdinal("timeout_ms")) ? null : TimeSpan.FromMilliseconds(reader.GetDouble(reader.GetOrdinal("timeout_ms"))),
            MaxConcurrency = reader.IsDBNull(reader.GetOrdinal("max_concurrency")) ? null : reader.GetInt32(reader.GetOrdinal("max_concurrency")),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            RetryPolicy = retryPolicyJson is not null
                ? JsonSerializer.Deserialize<RetryPolicy>(retryPolicyJson, JsonOptions) ?? new RetryPolicy()
                : new RetryPolicy(),
            IsContinuous = reader.GetInt32(reader.GetOrdinal("is_continuous")) != 0,
            Queue = reader.IsDBNull(reader.GetOrdinal("queue")) ? null : reader.GetString(reader.GetOrdinal("queue")),
            RateLimitName = reader.IsDBNull(reader.GetOrdinal("rate_limit_name")) ? null : reader.GetString(reader.GetOrdinal("rate_limit_name")),
            IsEnabled = reader.GetInt32(reader.GetOrdinal("is_enabled")) != 0,
            IsPlan = reader.GetInt32(reader.GetOrdinal("is_plan")) != 0,
            IsInternal = reader.GetInt32(reader.GetOrdinal("is_internal")) != 0,
            PlanGraph = reader.IsDBNull(reader.GetOrdinal("plan_graph")) ? null : reader.GetString(reader.GetOrdinal("plan_graph")),
            MisfirePolicy = (MisfirePolicy)reader.GetInt32(reader.GetOrdinal("misfire_policy")),
            ArgumentsSchema = reader.IsDBNull(reader.GetOrdinal("arguments_schema")) ? null : reader.GetString(reader.GetOrdinal("arguments_schema")),
            LastHeartbeatAt = reader.IsDBNull(reader.GetOrdinal("last_heartbeat_at")) ? null : ParseDto(reader.GetString(reader.GetOrdinal("last_heartbeat_at")))
        };
    }

    private static QueueDefinition ReadQueue(SqliteDataReader reader) => new()
    {
        Name = reader.GetString(reader.GetOrdinal("name")),
        Priority = reader.GetInt32(reader.GetOrdinal("priority")),
        MaxConcurrency = reader.IsDBNull(reader.GetOrdinal("max_concurrency")) ? null : reader.GetInt32(reader.GetOrdinal("max_concurrency")),
        IsPaused = reader.GetInt32(reader.GetOrdinal("is_paused")) != 0,
        RateLimitName = reader.IsDBNull(reader.GetOrdinal("rate_limit_name")) ? null : reader.GetString(reader.GetOrdinal("rate_limit_name")),
        LastHeartbeatAt = reader.IsDBNull(reader.GetOrdinal("last_heartbeat_at")) ? null : ParseDto(reader.GetString(reader.GetOrdinal("last_heartbeat_at")))
    };

    private static JobRun ReadRun(SqliteDataReader reader)
    {
        return new JobRun
        {
            Id = reader.GetString(reader.GetOrdinal("id")),
            JobName = reader.GetString(reader.GetOrdinal("job_name")),
            Status = (JobStatus)reader.GetInt32(reader.GetOrdinal("status")),
            Arguments = reader.IsDBNull(reader.GetOrdinal("arguments")) ? null : reader.GetString(reader.GetOrdinal("arguments")),
            Result = reader.IsDBNull(reader.GetOrdinal("result")) ? null : reader.GetString(reader.GetOrdinal("result")),
            Error = reader.IsDBNull(reader.GetOrdinal("error")) ? null : reader.GetString(reader.GetOrdinal("error")),
            Progress = reader.GetDouble(reader.GetOrdinal("progress")),
            CreatedAt = ParseDto(reader.GetString(reader.GetOrdinal("created_at"))),
            StartedAt = reader.IsDBNull(reader.GetOrdinal("started_at")) ? null : ParseDto(reader.GetString(reader.GetOrdinal("started_at"))),
            CompletedAt = reader.IsDBNull(reader.GetOrdinal("completed_at")) ? null : ParseDto(reader.GetString(reader.GetOrdinal("completed_at"))),
            CancelledAt = reader.IsDBNull(reader.GetOrdinal("cancelled_at")) ? null : ParseDto(reader.GetString(reader.GetOrdinal("cancelled_at"))),
            NodeName = reader.IsDBNull(reader.GetOrdinal("node_name")) ? null : reader.GetString(reader.GetOrdinal("node_name")),
            Attempt = reader.GetInt32(reader.GetOrdinal("attempt")),
            TraceId = reader.IsDBNull(reader.GetOrdinal("trace_id")) ? null : reader.GetString(reader.GetOrdinal("trace_id")),
            SpanId = reader.IsDBNull(reader.GetOrdinal("span_id")) ? null : reader.GetString(reader.GetOrdinal("span_id")),
            ParentRunId = reader.IsDBNull(reader.GetOrdinal("parent_run_id")) ? null : reader.GetString(reader.GetOrdinal("parent_run_id")),
            RetryOfRunId = reader.IsDBNull(reader.GetOrdinal("retry_of_run_id")) ? null : reader.GetString(reader.GetOrdinal("retry_of_run_id")),
            RerunOfRunId = reader.IsDBNull(reader.GetOrdinal("rerun_of_run_id")) ? null : reader.GetString(reader.GetOrdinal("rerun_of_run_id")),
            NotBefore = ParseDto(reader.GetString(reader.GetOrdinal("not_before"))),
            NotAfter = reader.IsDBNull(reader.GetOrdinal("not_after")) ? null : ParseDto(reader.GetString(reader.GetOrdinal("not_after"))),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            DeduplicationId = reader.IsDBNull(reader.GetOrdinal("deduplication_id")) ? null : reader.GetString(reader.GetOrdinal("deduplication_id")),
            LastHeartbeatAt = reader.IsDBNull(reader.GetOrdinal("last_heartbeat_at")) ? null : ParseDto(reader.GetString(reader.GetOrdinal("last_heartbeat_at"))),
            PlanRunId = reader.IsDBNull(reader.GetOrdinal("plan_run_id")) ? null : reader.GetString(reader.GetOrdinal("plan_run_id")),
            PlanStepId = reader.IsDBNull(reader.GetOrdinal("plan_step_id")) ? null : reader.GetString(reader.GetOrdinal("plan_step_id")),
            PlanStepName = reader.IsDBNull(reader.GetOrdinal("plan_step_name")) ? null : reader.GetString(reader.GetOrdinal("plan_step_name")),
            PlanGraph = reader.IsDBNull(reader.GetOrdinal("plan_graph")) ? null : reader.GetString(reader.GetOrdinal("plan_graph"))
        };
    }

    private static NodeInfo ReadNode(SqliteDataReader reader)
    {
        return new NodeInfo
        {
            Name = reader.GetString(reader.GetOrdinal("name")),
            StartedAt = ParseDto(reader.GetString(reader.GetOrdinal("started_at"))),
            LastHeartbeatAt = ParseDto(reader.GetString(reader.GetOrdinal("last_heartbeat_at"))),
            RunningCount = reader.GetInt32(reader.GetOrdinal("running_count")),
            RegisteredJobNames = JsonSerializer.Deserialize<string[]>(reader.GetString(reader.GetOrdinal("registered_job_names"))) ?? [],
            RegisteredQueueNames = JsonSerializer.Deserialize<string[]>(reader.GetString(reader.GetOrdinal("registered_queue_names"))) ?? []
        };
    }

    /// <summary>
    /// Formats a DateTimeOffset as an ISO 8601 UTC string for deterministic ordering in SQLite.
    /// </summary>
    private static string FormatDto(DateTimeOffset dto) => dto.UtcDateTime.ToString("O");

    /// <summary>
    /// Parses an ISO 8601 UTC string back to a DateTimeOffset.
    /// </summary>
    private static DateTimeOffset ParseDto(string s) => DateTimeOffset.Parse(s, null, System.Globalization.DateTimeStyles.RoundtripKind);

    private static string EscapeLike(string input) =>
        input.Replace(@"\", @"\\").Replace("%", @"\%").Replace("_", @"\_");

    private static SqliteParameter[] BuildInClause<T>(string prefix, IEnumerable<T> values, out string clause)
    {
        var list = values.ToList();
        var parameters = new SqliteParameter[list.Count];
        var parts = new string[list.Count];
        for (var i = 0; i < list.Count; i++)
        {
            var name = $"{prefix}{i}";
            parts[i] = name;
            parameters[i] = new SqliteParameter(name, list[i]);
        }
        clause = string.Join(", ", parts);
        return parameters;
    }
}
