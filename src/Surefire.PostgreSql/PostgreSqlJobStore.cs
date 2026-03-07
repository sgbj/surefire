using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace Surefire.PostgreSql;

internal sealed class PostgreSqlJobStore(
    PostgreSqlOptions pgOptions,
    SurefireOptions surefireOptions) : IJobStore
{
    private readonly NpgsqlDataSource _dataSource = pgOptions.DataSource;
    private string Schema => pgOptions.Schema;
    private JsonSerializerOptions JsonOptions => surefireOptions.SerializerOptions;

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);

        var sql = $"""
            CREATE SCHEMA IF NOT EXISTS {Schema};

            CREATE TABLE IF NOT EXISTS {Schema}.jobs (
                name TEXT PRIMARY KEY,
                description TEXT,
                tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
                cron_expression TEXT,
                timeout INTERVAL,
                max_concurrency INT,
                priority INT NOT NULL DEFAULT 0,
                retry_policy JSONB,
                is_continuous BOOLEAN NOT NULL DEFAULT FALSE,
                is_enabled BOOLEAN NOT NULL DEFAULT TRUE
            );

            CREATE TABLE IF NOT EXISTS {Schema}.runs (
                id TEXT PRIMARY KEY,
                job_name TEXT NOT NULL,
                status INT NOT NULL DEFAULT 0,
                arguments TEXT,
                result TEXT,
                error TEXT,
                progress FLOAT8 NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL,
                started_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ,
                cancelled_at TIMESTAMPTZ,
                node_name TEXT,
                attempt INT NOT NULL DEFAULT 1,
                trace_id TEXT,
                span_id TEXT,
                parent_run_id TEXT,
                retry_of_run_id TEXT,
                rerun_of_run_id TEXT,
                not_before TIMESTAMPTZ NOT NULL,
                priority INT NOT NULL DEFAULT 0,
                deduplication_id TEXT,
                last_heartbeat_at TIMESTAMPTZ
            );

            CREATE INDEX IF NOT EXISTS ix_runs_claim ON {Schema}.runs (priority DESC, not_before, created_at) WHERE status = 0;
            CREATE INDEX IF NOT EXISTS ix_runs_job_name_status ON {Schema}.runs (job_name, status);
            CREATE INDEX IF NOT EXISTS ix_runs_created_at ON {Schema}.runs (created_at DESC);
            CREATE INDEX IF NOT EXISTS ix_runs_retry_of_run_id ON {Schema}.runs (retry_of_run_id);
            CREATE INDEX IF NOT EXISTS ix_runs_rerun_of_run_id ON {Schema}.runs (rerun_of_run_id);
            CREATE INDEX IF NOT EXISTS ix_runs_parent_run_id ON {Schema}.runs (parent_run_id);
            CREATE INDEX IF NOT EXISTS ix_runs_node_name_status ON {Schema}.runs (node_name, status);
            CREATE UNIQUE INDEX IF NOT EXISTS ix_runs_deduplication_id ON {Schema}.runs (deduplication_id) WHERE deduplication_id IS NOT NULL;

            CREATE TABLE IF NOT EXISTS {Schema}.nodes (
                name TEXT PRIMARY KEY,
                started_at TIMESTAMPTZ NOT NULL,
                last_heartbeat_at TIMESTAMPTZ NOT NULL,
                running_count INT NOT NULL DEFAULT 0,
                registered_job_names TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[]
            );

            -- Migration: drop unused status column if it exists
            ALTER TABLE {Schema}.nodes DROP COLUMN IF EXISTS status;

            CREATE TABLE IF NOT EXISTS {Schema}.run_events (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                run_id TEXT NOT NULL,
                event_type SMALLINT NOT NULL,
                payload TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );

            CREATE INDEX IF NOT EXISTS ix_run_events_run_id ON {Schema}.run_events (run_id, id);

            -- Migration: drop old run_logs table if it exists
            DROP TABLE IF EXISTS {Schema}.run_logs;
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Jobs --

    public async Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            INSERT INTO {Schema}.jobs (name, description, tags, cron_expression, timeout, max_concurrency, priority, retry_policy, is_continuous, is_enabled)
            VALUES (@name, @description, @tags, @cron_expression, @timeout, @max_concurrency, @priority, @retry_policy, @is_continuous, @is_enabled)
            ON CONFLICT (name) DO UPDATE SET
                description = EXCLUDED.description,
                tags = EXCLUDED.tags,
                cron_expression = EXCLUDED.cron_expression,
                timeout = EXCLUDED.timeout,
                max_concurrency = EXCLUDED.max_concurrency,
                priority = EXCLUDED.priority,
                retry_policy = EXCLUDED.retry_policy,
                is_continuous = EXCLUDED.is_continuous
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter<string>("name", job.Name));
        cmd.Parameters.Add(NullableText("description", job.Description));
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("tags", job.Tags));
        cmd.Parameters.Add(NullableText("cron_expression", job.CronExpression));
        cmd.Parameters.Add(NullableInterval("timeout", job.Timeout));
        cmd.Parameters.Add(NullableInt("max_concurrency", job.MaxConcurrency));
        cmd.Parameters.Add(new NpgsqlParameter<int>("priority", job.Priority));
        cmd.Parameters.Add(new NpgsqlParameter("retry_policy", NpgsqlDbType.Jsonb) { Value = JsonSerializer.Serialize(job.RetryPolicy, JsonOptions) });
        cmd.Parameters.Add(new NpgsqlParameter<bool>("is_continuous", job.IsContinuous));
        cmd.Parameters.Add(new NpgsqlParameter<bool>("is_enabled", job.IsEnabled));

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        var sql = $"SELECT * FROM {Schema}.jobs WHERE name = @name";
        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter<string>("name", name));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadJob(reader) : null;
    }

    public async Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobFilter? filter = null, CancellationToken cancellationToken = default)
    {
        var sql = $"SELECT * FROM {Schema}.jobs WHERE TRUE";
        await using var cmd = _dataSource.CreateCommand();

        if (filter?.Name is not null)
        {
            sql += " AND name ILIKE @name ESCAPE '\\'";
            cmd.Parameters.Add(new NpgsqlParameter<string>("name", $"%{EscapeLike(filter.Name)}%"));
        }
        if (filter?.Tag is not null)
        {
            sql += " AND EXISTS (SELECT 1 FROM unnest(tags) t WHERE t ILIKE @tag ESCAPE '\\')";
            cmd.Parameters.Add(new NpgsqlParameter<string>("tag", $"%{EscapeLike(filter.Tag)}%"));
        }
        if (filter?.IsEnabled is not null)
        {
            sql += " AND is_enabled = @is_enabled";
            cmd.Parameters.Add(new NpgsqlParameter<bool>("is_enabled", filter.IsEnabled.Value));
        }

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
        await using var cmd = _dataSource.CreateCommand($"DELETE FROM {Schema}.jobs WHERE name = @name");
        cmd.Parameters.Add(new NpgsqlParameter<string>("name", name));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task SetJobEnabledAsync(string name, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var cmd = _dataSource.CreateCommand($"UPDATE {Schema}.jobs SET is_enabled = @enabled WHERE name = @name");
        cmd.Parameters.Add(new NpgsqlParameter<string>("name", name));
        cmd.Parameters.Add(new NpgsqlParameter<bool>("enabled", enabled));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Runs --

    public async Task CreateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        await InsertRunAsync(run, cancellationToken);
    }

    public async Task<bool> TryCreateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        if (run.DeduplicationId is null)
        {
            await InsertRunAsync(run, cancellationToken);
            return true;
        }

        var sql = $"""
            INSERT INTO {Schema}.runs (id, job_name, status, arguments, result, error, progress, created_at, started_at, completed_at, cancelled_at, node_name, attempt, trace_id, span_id, parent_run_id, retry_of_run_id, rerun_of_run_id, not_before, priority, deduplication_id, last_heartbeat_at)
            VALUES (@id, @job_name, @status, @arguments, @result, @error, @progress, @created_at, @started_at, @completed_at, @cancelled_at, @node_name, @attempt, @trace_id, @span_id, @parent_run_id, @retry_of_run_id, @rerun_of_run_id, @not_before, @priority, @deduplication_id, @last_heartbeat_at)
            ON CONFLICT (deduplication_id) WHERE deduplication_id IS NOT NULL DO NOTHING
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        AddRunParameters(cmd, run);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0;
    }

    public async Task<JobRun?> GetRunAsync(string id, CancellationToken cancellationToken = default)
    {
        await using var cmd = _dataSource.CreateCommand($"SELECT * FROM {Schema}.runs WHERE id = @id");
        cmd.Parameters.Add(new NpgsqlParameter<string>("id", id));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadRun(reader) : null;
    }

    public async Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, CancellationToken cancellationToken = default)
    {
        var where = "WHERE TRUE";
        await using var cmd = _dataSource.CreateCommand();

        if (filter.JobName is not null)
        {
            if (filter.ExactJobName)
            {
                where += " AND job_name = @job_name";
                cmd.Parameters.Add(new NpgsqlParameter<string>("job_name", filter.JobName));
            }
            else
            {
                where += " AND job_name ILIKE @job_name ESCAPE '\\'";
                cmd.Parameters.Add(new NpgsqlParameter<string>("job_name", $"%{EscapeLike(filter.JobName)}%"));
            }
        }
        if (filter.Status is not null)
        {
            where += " AND status = @status";
            cmd.Parameters.Add(new NpgsqlParameter<int>("status", (int)filter.Status.Value));
        }
        if (filter.NodeName is not null)
        {
            where += " AND node_name = @node_name";
            cmd.Parameters.Add(new NpgsqlParameter<string>("node_name", filter.NodeName));
        }
        if (filter.ParentRunId is not null)
        {
            where += " AND parent_run_id = @parent_run_id";
            cmd.Parameters.Add(new NpgsqlParameter<string>("parent_run_id", filter.ParentRunId));
        }
        if (filter.RetryOfRunId is not null)
        {
            where += " AND retry_of_run_id = @retry_of_run_id";
            cmd.Parameters.Add(new NpgsqlParameter<string>("retry_of_run_id", filter.RetryOfRunId));
        }
        if (filter.RerunOfRunId is not null)
        {
            where += " AND rerun_of_run_id = @rerun_of_run_id";
            cmd.Parameters.Add(new NpgsqlParameter<string>("rerun_of_run_id", filter.RerunOfRunId));
        }
        if (filter.CreatedAfter is not null)
        {
            where += " AND created_at >= @created_after";
            cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>("created_after", filter.CreatedAfter.Value));
        }
        if (filter.CreatedBefore is not null)
        {
            where += " AND created_at <= @created_before";
            cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>("created_before", filter.CreatedBefore.Value));
        }

        var orderCol = filter.OrderBy switch
        {
            RunOrderBy.StartedAt => "started_at",
            RunOrderBy.CompletedAt => "completed_at",
            _ => "created_at"
        };

        // COUNT(*) OVER() gives count + data in one round-trip when rows exist.
        // When OFFSET exceeds results (zero rows returned), fall back to a separate count query.
        cmd.CommandText = $"SELECT *, COUNT(*) OVER() AS total_count FROM {Schema}.runs {where} ORDER BY {orderCol} DESC NULLS LAST OFFSET @skip LIMIT @take";
        cmd.Parameters.Add(new NpgsqlParameter<int>("skip", filter.Skip));
        cmd.Parameters.Add(new NpgsqlParameter<int>("take", filter.Take));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var items = new List<JobRun>();
        var totalCount = 0;
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(ReadRun(reader));
            if (totalCount == 0)
                totalCount = Convert.ToInt32(reader.GetInt64(reader.GetOrdinal("total_count")));
        }

        if (items.Count == 0 && filter.Skip > 0)
        {
            // OFFSET may have exceeded results — get the true count
            await using var countCmd = _dataSource.CreateCommand($"SELECT COUNT(*) FROM {Schema}.runs {where}");
            foreach (NpgsqlParameter p in cmd.Parameters)
            {
                if (p.ParameterName is "skip" or "take") continue;
                countCmd.Parameters.Add(new NpgsqlParameter(p.ParameterName, p.NpgsqlDbType) { Value = p.Value! });
            }
            totalCount = Convert.ToInt32((long)(await countCmd.ExecuteScalarAsync(cancellationToken))!);
        }

        return new PagedResult<JobRun> { Items = items, TotalCount = totalCount };
    }

    public async Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE {Schema}.runs SET
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
                priority = @priority,
                deduplication_id = @deduplication_id,
                last_heartbeat_at = @last_heartbeat_at
            WHERE id = @id
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        AddRunParameters(cmd, run);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<bool> TryUpdateRunStatusAsync(JobRun run, JobStatus expectedStatus, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE {Schema}.runs SET
                status = @status,
                error = @error,
                completed_at = @completed_at,
                cancelled_at = @cancelled_at
            WHERE id = @id AND status = @expected_status
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter<string>("id", run.Id));
        cmd.Parameters.Add(new NpgsqlParameter<int>("status", (int)run.Status));
        cmd.Parameters.Add(NullableText("error", run.Error));
        cmd.Parameters.Add(NullableTimestampTz("completed_at", run.CompletedAt));
        cmd.Parameters.Add(NullableTimestampTz("cancelled_at", run.CancelledAt));
        cmd.Parameters.Add(new NpgsqlParameter<int>("expected_status", (int)expectedStatus));
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0;
    }

    public async Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, CancellationToken cancellationToken = default)
    {
        if (registeredJobNames.Count == 0)
            return null;

        var jobNames = registeredJobNames.ToArray();

        // Use an explicit transaction with two separate statements to serialize
        // max_concurrency checks. Under READ COMMITTED, FOR UPDATE in a CTE doesn't
        // serialize correlated subqueries within the same statement — the COUNT(*)
        // sees a snapshot taken at statement start. By locking jobs rows in statement 1,
        // statement 2 gets a fresh snapshot that sees any concurrent claims.
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        // Statement 1: Lock jobs rows with max_concurrency to serialize concurrent claimers.
        await using (var lockCmd = new NpgsqlCommand(
            $"SELECT name FROM {Schema}.jobs WHERE name = ANY(@job_names) AND max_concurrency IS NOT NULL FOR UPDATE",
            conn, tx))
        {
            lockCmd.Parameters.Add(new NpgsqlParameter<string[]>("job_names", jobNames));
            await lockCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // Statement 2: Claim a run — fresh snapshot now sees any concurrent claims.
        var sql = $"""
            WITH candidate AS (
                SELECT r.id
                FROM {Schema}.runs r
                WHERE r.status = 0
                  AND r.not_before <= NOW()
                  AND r.job_name = ANY(@job_names)
                  AND (
                      NOT EXISTS (SELECT 1 FROM {Schema}.jobs j WHERE j.name = r.job_name AND j.max_concurrency IS NOT NULL)
                      OR (SELECT COUNT(*) FROM {Schema}.runs r2 WHERE r2.job_name = r.job_name AND r2.status = 1)
                         < (SELECT j.max_concurrency FROM {Schema}.jobs j WHERE j.name = r.job_name)
                  )
                ORDER BY r.priority DESC, r.not_before, r.created_at
                LIMIT 1
                FOR UPDATE OF r SKIP LOCKED
            )
            UPDATE {Schema}.runs
            SET status = 1, node_name = @node_name, started_at = NOW(), last_heartbeat_at = NOW()
            FROM candidate
            WHERE {Schema}.runs.id = candidate.id
            RETURNING {Schema}.runs.*
            """;

        await using var cmd = new NpgsqlCommand(sql, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("job_names", jobNames));
        cmd.Parameters.Add(new NpgsqlParameter<string>("node_name", nodeName));

        JobRun? result;
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            result = await reader.ReadAsync(cancellationToken) ? ReadRun(reader) : null;
        }

        await tx.CommitAsync(cancellationToken);
        return result;
    }

    // -- Nodes --

    public async Task RegisterNodeAsync(NodeInfo node, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            INSERT INTO {Schema}.nodes (name, started_at, last_heartbeat_at, running_count, registered_job_names)
            VALUES (@name, @started_at, @last_heartbeat_at, @running_count, @registered_job_names)
            ON CONFLICT (name) DO UPDATE SET
                started_at = EXCLUDED.started_at,
                last_heartbeat_at = EXCLUDED.last_heartbeat_at,
                running_count = EXCLUDED.running_count,
                registered_job_names = EXCLUDED.registered_job_names
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter<string>("name", node.Name));
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>("started_at", node.StartedAt));
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>("last_heartbeat_at", node.LastHeartbeatAt));
        cmd.Parameters.Add(new NpgsqlParameter<int>("running_count", node.RunningCount));
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("registered_job_names", node.RegisteredJobNames.ToArray()));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE {Schema}.nodes SET
                last_heartbeat_at = NOW(),
                running_count = (SELECT COUNT(*) FROM {Schema}.runs WHERE node_name = @name AND status = 1),
                registered_job_names = @registered_job_names
            WHERE name = @name
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter<string>("name", nodeName));
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("registered_job_names", registeredJobNames.ToArray()));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task RemoveNodeAsync(string nodeName, CancellationToken cancellationToken = default)
    {
        await using var cmd = _dataSource.CreateCommand($"DELETE FROM {Schema}.nodes WHERE name = @name");
        cmd.Parameters.Add(new NpgsqlParameter<string>("name", nodeName));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<NodeInfo?> GetNodeAsync(string nodeName, CancellationToken cancellationToken = default)
    {
        await using var cmd = _dataSource.CreateCommand($"SELECT * FROM {Schema}.nodes WHERE name = @name");
        cmd.Parameters.Add(new NpgsqlParameter<string>("name", nodeName));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadNode(reader) : null;
    }

    public async Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default)
    {
        await using var cmd = _dataSource.CreateCommand($"SELECT * FROM {Schema}.nodes ORDER BY name");
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var nodes = new List<NodeInfo>();
        while (await reader.ReadAsync(cancellationToken))
            nodes.Add(ReadNode(reader));
        return nodes;
    }

    // -- Run Events --

    public async Task AppendEventAsync(RunEvent evt, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            INSERT INTO {Schema}.run_events (run_id, event_type, payload, created_at)
            VALUES (@run_id, @event_type, @payload, @created_at)
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter<string>("run_id", evt.RunId));
        cmd.Parameters.Add(new NpgsqlParameter<short>("event_type", (short)evt.EventType));
        cmd.Parameters.Add(new NpgsqlParameter<string>("payload", evt.Payload));
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>("created_at", evt.CreatedAt));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default)
    {
        if (events.Count == 0) return;

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using var writer = await conn.BeginBinaryImportAsync(
            $"COPY {Schema}.run_events (run_id, event_type, payload, created_at) FROM STDIN (FORMAT BINARY)",
            cancellationToken);

        foreach (var evt in events)
        {
            await writer.StartRowAsync(cancellationToken);
            await writer.WriteAsync(evt.RunId, cancellationToken);
            await writer.WriteAsync((short)evt.EventType, NpgsqlDbType.Smallint, cancellationToken);
            await writer.WriteAsync(evt.Payload, cancellationToken);
            await writer.WriteAsync(evt.CreatedAt, NpgsqlDbType.TimestampTz, cancellationToken);
        }

        await writer.CompleteAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0, RunEventType[]? types = null, CancellationToken cancellationToken = default)
    {
        var sql = $"SELECT id, run_id, event_type, payload, created_at FROM {Schema}.run_events WHERE run_id = @run_id AND id > @since_id";

        if (types is { Length: > 0 })
            sql += " AND event_type = ANY(@types)";

        sql += " ORDER BY id";

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter<string>("run_id", runId));
        cmd.Parameters.Add(new NpgsqlParameter<long>("since_id", sinceId));

        if (types is { Length: > 0 })
            cmd.Parameters.Add(new NpgsqlParameter<short[]>("types", types.Select(t => (short)t).ToArray()));

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
                CreatedAt = reader.GetFieldValue<DateTimeOffset>(4)
            });
        }
        return result;
    }

    // -- Heartbeat & Stale Runs --

    public async Task HeartbeatRunsAsync(IReadOnlyCollection<string> runIds, CancellationToken cancellationToken = default)
    {
        if (runIds.Count == 0) return;

        var sql = $"UPDATE {Schema}.runs SET last_heartbeat_at = NOW() WHERE id = ANY(@ids) AND status = 1";
        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("ids", runIds.ToArray()));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<JobRun>> GetStaleRunsAsync(TimeSpan threshold, CancellationToken cancellationToken = default)
    {
        var sql = $"SELECT * FROM {Schema}.runs WHERE status = 1 AND (last_heartbeat_at IS NULL OR last_heartbeat_at < NOW() - @threshold)";
        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter<TimeSpan>("threshold", threshold));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var runs = new List<JobRun>();
        while (await reader.ReadAsync(cancellationToken))
            runs.Add(ReadRun(reader));
        return runs;
    }

    // -- Trace --

    public async Task<IReadOnlyList<JobRun>> GetRunTraceAsync(string runId, int limit = 200, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            WITH RECURSIVE ancestors AS (
                SELECT r.* FROM {Schema}.runs r WHERE r.id = @id
                UNION ALL
                SELECT p.* FROM {Schema}.runs p JOIN ancestors a ON a.parent_run_id = p.id
            ),
            root AS (
                SELECT a.id FROM ancestors a
                WHERE NOT EXISTS (SELECT 1 FROM ancestors b WHERE b.id = a.parent_run_id)
                LIMIT 1
            ),
            descendants AS (
                SELECT r.* FROM {Schema}.runs r JOIN root ON r.id = root.id
                UNION ALL
                SELECT c.* FROM {Schema}.runs c JOIN descendants d ON c.parent_run_id = d.id
            )
            SELECT * FROM descendants ORDER BY created_at LIMIT @limit
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter<string>("id", runId));
        cmd.Parameters.Add(new NpgsqlParameter<int>("limit", limit));

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

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken);

        var sinceFilter = since is not null ? " AND created_at >= @since" : "";

        // Combined stats: totals + counts by status in a single scan
        await using var statsCmd = new NpgsqlCommand($"""
            SELECT
                (SELECT COUNT(*) FROM {Schema}.jobs) AS total_jobs,
                (SELECT COUNT(*) FROM {Schema}.nodes) AS node_count,
                COUNT(*) AS total_runs,
                COUNT(*) FILTER (WHERE status = 0) AS pending,
                COUNT(*) FILTER (WHERE status = 1) AS running,
                COUNT(*) FILTER (WHERE status = 2) AS completed,
                COUNT(*) FILTER (WHERE status = 3) AS failed,
                COUNT(*) FILTER (WHERE status = 4) AS cancelled,
                COUNT(*) FILTER (WHERE status = 5) AS dead_letter
            FROM {Schema}.runs WHERE TRUE{sinceFilter}
            """, conn);
        if (since is not null)
            statsCmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>("since", since.Value));

        int totalJobs, nodeCount, totalRuns, activeRuns;
        double successRate;
        var runsByStatus = new Dictionary<string, int>();

        await using (var reader = await statsCmd.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            totalJobs = Convert.ToInt32(reader.GetInt64(0));
            nodeCount = Convert.ToInt32(reader.GetInt64(1));
            totalRuns = Convert.ToInt32(reader.GetInt64(2));
            var pending = Convert.ToInt32(reader.GetInt64(3));
            var running = Convert.ToInt32(reader.GetInt64(4));
            var completed = Convert.ToInt32(reader.GetInt64(5));
            var failed = Convert.ToInt32(reader.GetInt64(6));
            var cancelled = Convert.ToInt32(reader.GetInt64(7));
            var deadLetter = Convert.ToInt32(reader.GetInt64(8));

            activeRuns = running;
            var totalFinished = completed + failed;
            successRate = totalFinished > 0 ? (double)completed / totalFinished * 100 : 0;

            if (pending > 0) runsByStatus[JobStatus.Pending.ToString()] = pending;
            if (running > 0) runsByStatus[JobStatus.Running.ToString()] = running;
            if (completed > 0) runsByStatus[JobStatus.Completed.ToString()] = completed;
            if (failed > 0) runsByStatus[JobStatus.Failed.ToString()] = failed;
            if (cancelled > 0) runsByStatus[JobStatus.Cancelled.ToString()] = cancelled;
            if (deadLetter > 0) runsByStatus[JobStatus.DeadLetter.ToString()] = deadLetter;
        }

        // Recent runs
        await using var recentCmd = new NpgsqlCommand(
            $"SELECT * FROM {Schema}.runs WHERE TRUE{sinceFilter} ORDER BY created_at DESC LIMIT 10", conn);
        if (since is not null)
            recentCmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>("since", since.Value));

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
            await using var tlCmd = new NpgsqlCommand($"""
                SELECT
                    to_timestamp(floor(extract(epoch FROM created_at) / @bucket_seconds) * @bucket_seconds) AT TIME ZONE 'UTC' AS bucket,
                    COUNT(*) FILTER (WHERE status = 0) AS pending,
                    COUNT(*) FILTER (WHERE status = 1) AS running,
                    COUNT(*) FILTER (WHERE status = 2) AS completed,
                    COUNT(*) FILTER (WHERE status = 3) AS failed,
                    COUNT(*) FILTER (WHERE status = 4) AS cancelled,
                    COUNT(*) FILTER (WHERE status = 5) AS dead_letter
                FROM {Schema}.runs
                WHERE created_at >= @since
                GROUP BY bucket
                ORDER BY bucket
                """, conn);
            tlCmd.Parameters.Add(new NpgsqlParameter<int>("bucket_seconds", bucketMinutes * 60));
            tlCmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>("since", since.Value));

            var bucketMap = new Dictionary<long, TimelineBucket>();
            await using (var reader = await tlCmd.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var ts = reader.GetFieldValue<DateTime>(0);
                    var dto = new DateTimeOffset(ts, TimeSpan.Zero);
                    bucketMap[dto.UtcTicks] = new TimelineBucket
                    {
                        Timestamp = dto,
                        Pending = Convert.ToInt32(reader.GetInt64(1)),
                        Running = Convert.ToInt32(reader.GetInt64(2)),
                        Completed = Convert.ToInt32(reader.GetInt64(3)),
                        Failed = Convert.ToInt32(reader.GetInt64(4)),
                        Cancelled = Convert.ToInt32(reader.GetInt64(5)),
                        DeadLetter = Convert.ToInt32(reader.GetInt64(6))
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

    // -- Purge --

    public async Task<int> PurgeRunsAsync(DateTimeOffset completedBefore, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            WITH purged AS (
                DELETE FROM {Schema}.runs
                WHERE (status IN (2, 3, 4, 5) AND completed_at < @before)
                   OR (status = 0 AND created_at < @before)
                RETURNING id
            ), deleted_events AS (
                DELETE FROM {Schema}.run_events WHERE run_id IN (SELECT id FROM purged)
            )
            SELECT COUNT(*) FROM purged
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>("before", completedBefore));
        var result = await cmd.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt32(result);
    }

    // -- Helpers --

    private async Task InsertRunAsync(JobRun run, CancellationToken cancellationToken)
    {
        var sql = $"""
            INSERT INTO {Schema}.runs (id, job_name, status, arguments, result, error, progress, created_at, started_at, completed_at, cancelled_at, node_name, attempt, trace_id, span_id, parent_run_id, retry_of_run_id, rerun_of_run_id, not_before, priority, deduplication_id, last_heartbeat_at)
            VALUES (@id, @job_name, @status, @arguments, @result, @error, @progress, @created_at, @started_at, @completed_at, @cancelled_at, @node_name, @attempt, @trace_id, @span_id, @parent_run_id, @retry_of_run_id, @rerun_of_run_id, @not_before, @priority, @deduplication_id, @last_heartbeat_at)
            """;

        await using var cmd = _dataSource.CreateCommand(sql);
        AddRunParameters(cmd, run);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddRunParameters(NpgsqlCommand cmd, JobRun run)
    {
        cmd.Parameters.Add(new NpgsqlParameter<string>("id", run.Id));
        cmd.Parameters.Add(new NpgsqlParameter<string>("job_name", run.JobName));
        cmd.Parameters.Add(new NpgsqlParameter<int>("status", (int)run.Status));
        cmd.Parameters.Add(NullableText("arguments", run.Arguments));
        cmd.Parameters.Add(NullableText("result", run.Result));
        cmd.Parameters.Add(NullableText("error", run.Error));
        cmd.Parameters.Add(new NpgsqlParameter<double>("progress", run.Progress));
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>("created_at", run.CreatedAt));
        cmd.Parameters.Add(NullableTimestampTz("started_at", run.StartedAt));
        cmd.Parameters.Add(NullableTimestampTz("completed_at", run.CompletedAt));
        cmd.Parameters.Add(NullableTimestampTz("cancelled_at", run.CancelledAt));
        cmd.Parameters.Add(NullableText("node_name", run.NodeName));
        cmd.Parameters.Add(new NpgsqlParameter<int>("attempt", run.Attempt));
        cmd.Parameters.Add(NullableText("trace_id", run.TraceId));
        cmd.Parameters.Add(NullableText("span_id", run.SpanId));
        cmd.Parameters.Add(NullableText("parent_run_id", run.ParentRunId));
        cmd.Parameters.Add(NullableText("retry_of_run_id", run.RetryOfRunId));
        cmd.Parameters.Add(NullableText("rerun_of_run_id", run.RerunOfRunId));
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset>("not_before", run.NotBefore));
        cmd.Parameters.Add(new NpgsqlParameter<int>("priority", run.Priority));
        cmd.Parameters.Add(NullableText("deduplication_id", run.DeduplicationId));
        cmd.Parameters.Add(NullableTimestampTz("last_heartbeat_at", run.LastHeartbeatAt));
    }

    private static NpgsqlParameter NullableText(string name, string? value) =>
        new(name, NpgsqlDbType.Text) { Value = (object?)value ?? DBNull.Value };

    private static NpgsqlParameter NullableTimestampTz(string name, DateTimeOffset? value) =>
        new(name, NpgsqlDbType.TimestampTz) { Value = value.HasValue ? value.Value : DBNull.Value };

    private static NpgsqlParameter NullableInterval(string name, TimeSpan? value) =>
        new(name, NpgsqlDbType.Interval) { Value = value.HasValue ? value.Value : DBNull.Value };

    private static NpgsqlParameter NullableInt(string name, int? value) =>
        new(name, NpgsqlDbType.Integer) { Value = value.HasValue ? value.Value : DBNull.Value };

    private JobDefinition ReadJob(NpgsqlDataReader reader)
    {
        var retryPolicyJson = reader.IsDBNull(reader.GetOrdinal("retry_policy"))
            ? null
            : reader.GetString(reader.GetOrdinal("retry_policy"));

        return new JobDefinition
        {
            Name = reader.GetString(reader.GetOrdinal("name")),
            Description = reader.IsDBNull(reader.GetOrdinal("description")) ? null : reader.GetString(reader.GetOrdinal("description")),
            Tags = reader.GetFieldValue<string[]>(reader.GetOrdinal("tags")),
            CronExpression = reader.IsDBNull(reader.GetOrdinal("cron_expression")) ? null : reader.GetString(reader.GetOrdinal("cron_expression")),
            Timeout = reader.IsDBNull(reader.GetOrdinal("timeout")) ? null : reader.GetFieldValue<TimeSpan>(reader.GetOrdinal("timeout")),
            MaxConcurrency = reader.IsDBNull(reader.GetOrdinal("max_concurrency")) ? null : reader.GetInt32(reader.GetOrdinal("max_concurrency")),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            RetryPolicy = retryPolicyJson is not null
                ? JsonSerializer.Deserialize<RetryPolicy>(retryPolicyJson, JsonOptions) ?? new RetryPolicy()
                : new RetryPolicy(),
            IsContinuous = reader.GetBoolean(reader.GetOrdinal("is_continuous")),
            IsEnabled = reader.GetBoolean(reader.GetOrdinal("is_enabled"))
        };
    }

    private static JobRun ReadRun(NpgsqlDataReader reader)
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
            CreatedAt = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("created_at")),
            StartedAt = reader.IsDBNull(reader.GetOrdinal("started_at")) ? null : reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("started_at")),
            CompletedAt = reader.IsDBNull(reader.GetOrdinal("completed_at")) ? null : reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("completed_at")),
            CancelledAt = reader.IsDBNull(reader.GetOrdinal("cancelled_at")) ? null : reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("cancelled_at")),
            NodeName = reader.IsDBNull(reader.GetOrdinal("node_name")) ? null : reader.GetString(reader.GetOrdinal("node_name")),
            Attempt = reader.GetInt32(reader.GetOrdinal("attempt")),
            TraceId = reader.IsDBNull(reader.GetOrdinal("trace_id")) ? null : reader.GetString(reader.GetOrdinal("trace_id")),
            SpanId = reader.IsDBNull(reader.GetOrdinal("span_id")) ? null : reader.GetString(reader.GetOrdinal("span_id")),
            ParentRunId = reader.IsDBNull(reader.GetOrdinal("parent_run_id")) ? null : reader.GetString(reader.GetOrdinal("parent_run_id")),
            RetryOfRunId = reader.IsDBNull(reader.GetOrdinal("retry_of_run_id")) ? null : reader.GetString(reader.GetOrdinal("retry_of_run_id")),
            RerunOfRunId = reader.IsDBNull(reader.GetOrdinal("rerun_of_run_id")) ? null : reader.GetString(reader.GetOrdinal("rerun_of_run_id")),
            NotBefore = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("not_before")),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            DeduplicationId = reader.IsDBNull(reader.GetOrdinal("deduplication_id")) ? null : reader.GetString(reader.GetOrdinal("deduplication_id")),
            LastHeartbeatAt = reader.IsDBNull(reader.GetOrdinal("last_heartbeat_at")) ? null : reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("last_heartbeat_at"))
        };
    }

    private static NodeInfo ReadNode(NpgsqlDataReader reader)
    {
        return new NodeInfo
        {
            Name = reader.GetString(reader.GetOrdinal("name")),
            StartedAt = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("started_at")),
            LastHeartbeatAt = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("last_heartbeat_at")),
            RunningCount = reader.GetInt32(reader.GetOrdinal("running_count")),
            RegisteredJobNames = reader.GetFieldValue<string[]>(reader.GetOrdinal("registered_job_names"))
        };
    }

    private static string EscapeLike(string input) =>
        input.Replace(@"\", @"\\").Replace("%", @"\%").Replace("_", @"\_");
}
