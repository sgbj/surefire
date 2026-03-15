using System.Data;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Surefire.SqlServer;

internal sealed class SqlServerJobStore(
    SqlServerOptions sqlServerOptions,
    SurefireOptions surefireOptions) : IJobStore
{
    private string Schema => sqlServerOptions.Schema;
    private JsonSerializerOptions JsonOptions => surefireOptions.SerializerOptions;

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);

        // Create schema if it doesn't exist
        await using (var schemaCmd = new SqlCommand(
            $"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @schema) EXEC('CREATE SCHEMA [{Schema}]')", conn))
        {
            schemaCmd.Parameters.Add(new SqlParameter("@schema", SqlDbType.NVarChar, 128) { Value = Schema });
            await schemaCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // Bootstrap migrations tracking table, then bail out if v1 already applied.
        await using (var bootstrapCmd = new SqlCommand($"""
            IF OBJECT_ID('{Schema}.schema_migrations', 'U') IS NULL
                CREATE TABLE [{Schema}].schema_migrations (version INT NOT NULL PRIMARY KEY);
            """, conn))
        {
            await bootstrapCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var checkCmd = new SqlCommand(
            $"SELECT COUNT(*) FROM [{Schema}].schema_migrations WHERE version = 1", conn))
        {
            var count = (int)(await checkCmd.ExecuteScalarAsync(cancellationToken))!;
            if (count > 0) return;
        }

        var sql = $"""
            IF OBJECT_ID('{Schema}.jobs', 'U') IS NULL
            CREATE TABLE [{Schema}].jobs (
                name NVARCHAR(450) NOT NULL PRIMARY KEY,
                description NVARCHAR(MAX),
                tags NVARCHAR(MAX) NOT NULL DEFAULT '[]',
                cron_expression NVARCHAR(450),
                time_zone_id NVARCHAR(450),
                timeout_ms FLOAT,
                max_concurrency INT,
                priority INT NOT NULL DEFAULT 0,
                retry_policy NVARCHAR(MAX),
                is_continuous BIT NOT NULL DEFAULT 0,
                queue NVARCHAR(450),
                rate_limit_name NVARCHAR(450),
                is_enabled BIT NOT NULL DEFAULT 1,
                is_plan BIT NOT NULL DEFAULT 0,
                is_internal BIT NOT NULL DEFAULT 0,
                plan_graph NVARCHAR(MAX),
                misfire_policy INT NOT NULL DEFAULT 0,
                arguments_schema NVARCHAR(MAX),
                last_heartbeat_at DATETIMEOFFSET
            );

            IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('{Schema}.jobs') AND name = 'is_internal')
                ALTER TABLE [{Schema}].jobs ADD is_internal BIT NOT NULL DEFAULT 0;

            IF OBJECT_ID('{Schema}.runs', 'U') IS NULL
            CREATE TABLE [{Schema}].runs (
                id NVARCHAR(450) NOT NULL PRIMARY KEY,
                job_name NVARCHAR(450) NOT NULL,
                status INT NOT NULL DEFAULT 0,
                arguments NVARCHAR(MAX),
                result NVARCHAR(MAX),
                error NVARCHAR(MAX),
                progress FLOAT NOT NULL DEFAULT 0,
                created_at DATETIMEOFFSET NOT NULL,
                started_at DATETIMEOFFSET,
                completed_at DATETIMEOFFSET,
                cancelled_at DATETIMEOFFSET,
                node_name NVARCHAR(450),
                attempt INT NOT NULL DEFAULT 1,
                trace_id NVARCHAR(450),
                span_id NVARCHAR(450),
                parent_run_id NVARCHAR(450),
                retry_of_run_id NVARCHAR(450),
                rerun_of_run_id NVARCHAR(450),
                not_before DATETIMEOFFSET NOT NULL,
                not_after DATETIMEOFFSET,
                priority INT NOT NULL DEFAULT 0,
                deduplication_id NVARCHAR(450),
                last_heartbeat_at DATETIMEOFFSET,
                plan_run_id NVARCHAR(450),
                plan_step_id NVARCHAR(450),
                plan_step_name NVARCHAR(450),
                plan_graph NVARCHAR(MAX)
            );

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_runs_claim' AND object_id = OBJECT_ID('{Schema}.runs'))
                CREATE INDEX ix_runs_claim ON [{Schema}].runs (priority DESC, not_before, created_at) WHERE status = 0;

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_runs_job_name_status' AND object_id = OBJECT_ID('{Schema}.runs'))
                CREATE INDEX ix_runs_job_name_status ON [{Schema}].runs (job_name, status);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_runs_created_at' AND object_id = OBJECT_ID('{Schema}.runs'))
                CREATE INDEX ix_runs_created_at ON [{Schema}].runs (created_at DESC);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_runs_retry_of_run_id' AND object_id = OBJECT_ID('{Schema}.runs'))
                CREATE INDEX ix_runs_retry_of_run_id ON [{Schema}].runs (retry_of_run_id);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_runs_rerun_of_run_id' AND object_id = OBJECT_ID('{Schema}.runs'))
                CREATE INDEX ix_runs_rerun_of_run_id ON [{Schema}].runs (rerun_of_run_id);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_runs_parent_run_id' AND object_id = OBJECT_ID('{Schema}.runs'))
                CREATE INDEX ix_runs_parent_run_id ON [{Schema}].runs (parent_run_id);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_runs_node_name_status' AND object_id = OBJECT_ID('{Schema}.runs'))
                CREATE INDEX ix_runs_node_name_status ON [{Schema}].runs (node_name, status);

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_runs_deduplication_id' AND object_id = OBJECT_ID('{Schema}.runs'))
                CREATE UNIQUE INDEX ix_runs_deduplication_id ON [{Schema}].runs (deduplication_id) WHERE deduplication_id IS NOT NULL;

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_runs_plan_step' AND object_id = OBJECT_ID('{Schema}.runs'))
                CREATE UNIQUE INDEX ix_runs_plan_step ON [{Schema}].runs (plan_run_id, plan_step_id) WHERE plan_run_id IS NOT NULL AND retry_of_run_id IS NULL;

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_runs_plan_run_id' AND object_id = OBJECT_ID('{Schema}.runs'))
                CREATE INDEX ix_runs_plan_run_id ON [{Schema}].runs (plan_run_id) WHERE plan_run_id IS NOT NULL;

            IF OBJECT_ID('{Schema}.rate_limits', 'U') IS NULL
            CREATE TABLE [{Schema}].rate_limits (
                name NVARCHAR(450) NOT NULL PRIMARY KEY,
                type SMALLINT NOT NULL DEFAULT 0,
                max_permits INT NOT NULL,
                window_duration_ms FLOAT NOT NULL,
                current_count INT NOT NULL DEFAULT 0,
                previous_count INT NOT NULL DEFAULT 0,
                window_start DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET()
            );

            IF OBJECT_ID('{Schema}.queues', 'U') IS NULL
            CREATE TABLE [{Schema}].queues (
                name NVARCHAR(450) NOT NULL PRIMARY KEY,
                priority INT NOT NULL DEFAULT 0,
                max_concurrency INT,
                is_paused BIT NOT NULL DEFAULT 0,
                rate_limit_name NVARCHAR(450),
                last_heartbeat_at DATETIMEOFFSET
            );

            IF OBJECT_ID('{Schema}.nodes', 'U') IS NULL
            CREATE TABLE [{Schema}].nodes (
                name NVARCHAR(450) NOT NULL PRIMARY KEY,
                started_at DATETIMEOFFSET NOT NULL,
                last_heartbeat_at DATETIMEOFFSET NOT NULL,
                running_count INT NOT NULL DEFAULT 0,
                registered_job_names NVARCHAR(MAX) NOT NULL DEFAULT '[]',
                registered_queue_names NVARCHAR(MAX) NOT NULL DEFAULT '[]'
            );

            IF OBJECT_ID('{Schema}.run_events', 'U') IS NULL
            CREATE TABLE [{Schema}].run_events (
                id BIGINT IDENTITY(1,1) PRIMARY KEY,
                run_id NVARCHAR(450) NOT NULL,
                event_type SMALLINT NOT NULL,
                payload NVARCHAR(MAX) NOT NULL,
                created_at DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET()
            );

            IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_run_events_run_id' AND object_id = OBJECT_ID('{Schema}.run_events'))
                CREATE INDEX ix_run_events_run_id ON [{Schema}].run_events (run_id, id);
            """;

        await using var cmd = new SqlCommand(sql, conn);
        await cmd.ExecuteNonQueryAsync(cancellationToken);

        await using var markCmd = new SqlCommand(
            $"IF NOT EXISTS (SELECT 1 FROM [{Schema}].schema_migrations WHERE version = 1) INSERT INTO [{Schema}].schema_migrations (version) VALUES (1)", conn);
        await markCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Jobs --

    public async Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            MERGE [{Schema}].jobs WITH (HOLDLOCK) AS target
            USING (SELECT @name AS name) AS source ON target.name = source.name
            WHEN MATCHED THEN UPDATE SET
                description = @description,
                tags = @tags,
                cron_expression = @cron_expression,
                time_zone_id = @time_zone_id,
                timeout_ms = @timeout_ms,
                max_concurrency = @max_concurrency,
                priority = @priority,
                retry_policy = @retry_policy,
                is_continuous = @is_continuous,
                queue = @queue,
                rate_limit_name = @rate_limit_name,
                is_plan = @is_plan,
                is_internal = @is_internal,
                plan_graph = @plan_graph,
                misfire_policy = @misfire_policy,
                arguments_schema = @arguments_schema,
                last_heartbeat_at = SYSDATETIMEOFFSET()
            WHEN NOT MATCHED THEN INSERT
                (name, description, tags, cron_expression, time_zone_id, timeout_ms, max_concurrency, priority, retry_policy, is_continuous, queue, rate_limit_name, is_enabled, is_plan, is_internal, plan_graph, misfire_policy, arguments_schema, last_heartbeat_at)
            VALUES
                (@name, @description, @tags, @cron_expression, @time_zone_id, @timeout_ms, @max_concurrency, @priority, @retry_policy, @is_continuous, @queue, @rate_limit_name, @is_enabled, @is_plan, @is_internal, @plan_graph, @misfire_policy, @arguments_schema, SYSDATETIMEOFFSET());
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = job.Name });
        cmd.Parameters.Add(NullableNVarChar("@description", job.Description));
        cmd.Parameters.Add(new SqlParameter("@tags", SqlDbType.NVarChar) { Value = JsonSerializer.Serialize(job.Tags, JsonOptions) });
        cmd.Parameters.Add(NullableNVarChar("@cron_expression", job.CronExpression));
        cmd.Parameters.Add(NullableNVarChar("@time_zone_id", job.TimeZoneId));
        cmd.Parameters.Add(new SqlParameter("@timeout_ms", SqlDbType.Float) { Value = job.Timeout.HasValue ? job.Timeout.Value.TotalMilliseconds : DBNull.Value });
        cmd.Parameters.Add(new SqlParameter("@max_concurrency", SqlDbType.Int) { Value = job.MaxConcurrency.HasValue ? job.MaxConcurrency.Value : DBNull.Value });
        cmd.Parameters.Add(new SqlParameter("@priority", SqlDbType.Int) { Value = job.Priority });
        cmd.Parameters.Add(new SqlParameter("@retry_policy", SqlDbType.NVarChar) { Value = JsonSerializer.Serialize(job.RetryPolicy, JsonOptions) });
        cmd.Parameters.Add(new SqlParameter("@is_continuous", SqlDbType.Bit) { Value = job.IsContinuous });
        cmd.Parameters.Add(NullableNVarChar("@queue", job.Queue));
        cmd.Parameters.Add(NullableNVarChar("@rate_limit_name", job.RateLimitName));
        cmd.Parameters.Add(new SqlParameter("@is_enabled", SqlDbType.Bit) { Value = job.IsEnabled });
        cmd.Parameters.Add(new SqlParameter("@is_plan", SqlDbType.Bit) { Value = job.IsPlan });
        cmd.Parameters.Add(new SqlParameter("@is_internal", SqlDbType.Bit) { Value = job.IsInternal });
        cmd.Parameters.Add(NullableNVarChar("@plan_graph", job.PlanGraph));
        cmd.Parameters.Add(new SqlParameter("@misfire_policy", SqlDbType.Int) { Value = (int)job.MisfirePolicy });
        cmd.Parameters.Add(NullableNVarChar("@arguments_schema", job.ArgumentsSchema));

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand($"SELECT * FROM [{Schema}].jobs WHERE name = @name", conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = name });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadJob(reader) : null;
    }

    public async Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null, CancellationToken cancellationToken = default)
    {
        var sql = $"SELECT * FROM [{Schema}].jobs WHERE 1=1";
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand { Connection = conn };

        if (filter?.Name is not null)
        {
            sql += " AND name LIKE @name ESCAPE '\\'";
            cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar) { Value = $"%{EscapeLike(filter.Name)}%" });
        }
        if (filter?.Tag is not null)
        {
            sql += " AND EXISTS (SELECT 1 FROM OPENJSON(tags) WHERE value LIKE @tag ESCAPE '\\')";
            cmd.Parameters.Add(new SqlParameter("@tag", SqlDbType.NVarChar) { Value = $"%{EscapeLike(filter.Tag)}%" });
        }
        if (filter?.IsEnabled is not null)
        {
            sql += " AND is_enabled = @is_enabled";
            cmd.Parameters.Add(new SqlParameter("@is_enabled", SqlDbType.Bit) { Value = filter.IsEnabled.Value });
        }
        if (filter?.HeartbeatAfter is not null)
        {
            sql += " AND last_heartbeat_at IS NOT NULL AND last_heartbeat_at >= @heartbeat_after";
            cmd.Parameters.Add(new SqlParameter("@heartbeat_after", SqlDbType.DateTimeOffset) { Value = filter.HeartbeatAfter.Value });
        }
        if (filter?.IncludeInternal != true)
        {
            sql += " AND is_internal = 0";
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
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand($"DELETE FROM [{Schema}].jobs WHERE name = @name", conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = name });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task SetJobEnabledAsync(string name, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand($"UPDATE [{Schema}].jobs SET is_enabled = @enabled WHERE name = @name", conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = name });
        cmd.Parameters.Add(new SqlParameter("@enabled", SqlDbType.Bit) { Value = enabled });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Runs --

    public async Task CreateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await InsertRunAsync(conn, null, run, cancellationToken);
    }

    public async Task CreateRunsAsync(IReadOnlyList<JobRun> runs, CancellationToken cancellationToken = default)
    {
        if (runs.Count == 0) return;

        await using var conn = await OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);

        foreach (var run in runs)
            await InsertRunAsync(conn, tx, run, cancellationToken);

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
            catch (SqlException ex) when (ex.Number == 2601 || ex.Number == 2627) // unique index/constraint violation
            {
                // Plan step unique constraint violation — step already created by another node
                return false;
            }
        }

        // Use a conditional insert that skips if deduplication_id already exists.
        // Also catch unique constraint violations for the race between check and insert.
        var sql = $"""
            INSERT INTO [{Schema}].runs (id, job_name, status, arguments, result, error, progress, created_at, started_at, completed_at, cancelled_at, node_name, attempt, trace_id, span_id, parent_run_id, retry_of_run_id, rerun_of_run_id, not_before, not_after, priority, deduplication_id, last_heartbeat_at, plan_run_id, plan_step_id, plan_step_name, plan_graph)
            SELECT @id, @job_name, @status, @arguments, @result, @error, @progress, @created_at, @started_at, @completed_at, @cancelled_at, @node_name, @attempt, @trace_id, @span_id, @parent_run_id, @retry_of_run_id, @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id, @last_heartbeat_at, @plan_run_id, @plan_step_id, @plan_step_name, @plan_graph
            WHERE NOT EXISTS (SELECT 1 FROM [{Schema}].runs WHERE deduplication_id = @deduplication_id)
            """;

        try
        {
            await using var conn = await OpenAsync(cancellationToken);
            await using var cmd = new SqlCommand(sql, conn);
            AddRunParameters(cmd, run);
            var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
            return rows > 0;
        }
        catch (SqlException ex) when (ex.Number == 2601 || ex.Number == 2627)
        {
            // Unique constraint violation on deduplication_id — another node inserted first
            return false;
        }
    }

    public async Task<bool> TryCreateContinuousRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);

        // Acquire an application lock to serialize concurrent restart attempts for the same job,
        // ensuring the active count check and insert are atomic across nodes.
        await using (var lockCmd = new SqlCommand(
            "EXEC @result = sp_getapplock @Resource, 'Exclusive', 'Transaction', 5000", conn, tx))
        {
            lockCmd.Parameters.Add(new SqlParameter("@Resource", SqlDbType.NVarChar, 255) { Value = $"surefire_continuous_{run.JobName}" });
            var resultParam = new SqlParameter("@result", SqlDbType.Int) { Direction = ParameterDirection.Output };
            lockCmd.Parameters.Add(resultParam);
            await lockCmd.ExecuteNonQueryAsync(cancellationToken);
            // result >= 0 means lock acquired; < 0 means timeout/error
            if ((int)resultParam.Value < 0)
                return false;
        }

        var sql = $"""
            INSERT INTO [{Schema}].runs (id, job_name, status, arguments, result, error, progress, created_at, started_at, completed_at, cancelled_at, node_name, attempt, trace_id, span_id, parent_run_id, retry_of_run_id, rerun_of_run_id, not_before, not_after, priority, deduplication_id, last_heartbeat_at, plan_run_id, plan_step_id, plan_step_name, plan_graph)
            SELECT @id, @job_name, @status, @arguments, @result, @error, @progress, @created_at, @started_at, @completed_at, @cancelled_at, @node_name, @attempt, @trace_id, @span_id, @parent_run_id, @retry_of_run_id, @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id, @last_heartbeat_at, @plan_run_id, @plan_step_id, @plan_step_name, @plan_graph
            WHERE (SELECT is_enabled FROM [{Schema}].jobs WHERE name = @job_name) = 1
              AND (SELECT COUNT(*) FROM [{Schema}].runs WHERE job_name = @job_name AND status IN (0, 1))
                < COALESCE((SELECT max_concurrency FROM [{Schema}].jobs WHERE name = @job_name), 1)
              AND (@deduplication_id IS NULL OR NOT EXISTS (SELECT 1 FROM [{Schema}].runs WHERE deduplication_id = @deduplication_id))
            """;

        await using var cmd = new SqlCommand(sql, conn) { Transaction = tx };
        AddRunParameters(cmd, run);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);

        await tx.CommitAsync(cancellationToken);
        return rows > 0;
    }

    public async Task<JobRun?> GetRunAsync(string id, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand($"SELECT * FROM [{Schema}].runs WHERE id = @id", conn);
        cmd.Parameters.Add(new SqlParameter("@id", SqlDbType.NVarChar, 450) { Value = id });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadRun(reader) : null;
    }

    public async Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, CancellationToken cancellationToken = default)
    {
        var where = "WHERE 1=1";
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand { Connection = conn };

        if (filter.JobName is not null)
        {
            if (filter.ExactJobName)
            {
                where += " AND job_name = @job_name";
                cmd.Parameters.Add(new SqlParameter("@job_name", SqlDbType.NVarChar, 450) { Value = filter.JobName });
            }
            else
            {
                where += " AND job_name LIKE @job_name ESCAPE '\\'";
                cmd.Parameters.Add(new SqlParameter("@job_name", SqlDbType.NVarChar) { Value = $"%{EscapeLike(filter.JobName)}%" });
            }
        }
        if (filter.Status is not null)
        {
            where += " AND status = @status";
            cmd.Parameters.Add(new SqlParameter("@status", SqlDbType.Int) { Value = (int)filter.Status.Value });
        }
        if (filter.NodeName is not null)
        {
            where += " AND node_name = @node_name";
            cmd.Parameters.Add(new SqlParameter("@node_name", SqlDbType.NVarChar, 450) { Value = filter.NodeName });
        }
        if (filter.ParentRunId is not null)
        {
            where += " AND parent_run_id = @parent_run_id";
            cmd.Parameters.Add(new SqlParameter("@parent_run_id", SqlDbType.NVarChar, 450) { Value = filter.ParentRunId });
        }
        if (filter.RetryOfRunId is not null)
        {
            where += " AND retry_of_run_id = @retry_of_run_id";
            cmd.Parameters.Add(new SqlParameter("@retry_of_run_id", SqlDbType.NVarChar, 450) { Value = filter.RetryOfRunId });
        }
        if (filter.RerunOfRunId is not null)
        {
            where += " AND rerun_of_run_id = @rerun_of_run_id";
            cmd.Parameters.Add(new SqlParameter("@rerun_of_run_id", SqlDbType.NVarChar, 450) { Value = filter.RerunOfRunId });
        }
        if (filter.CreatedAfter is not null)
        {
            where += " AND created_at >= @created_after";
            cmd.Parameters.Add(new SqlParameter("@created_after", SqlDbType.DateTimeOffset) { Value = filter.CreatedAfter.Value });
        }
        if (filter.CreatedBefore is not null)
        {
            where += " AND created_at <= @created_before";
            cmd.Parameters.Add(new SqlParameter("@created_before", SqlDbType.DateTimeOffset) { Value = filter.CreatedBefore.Value });
        }
        if (filter.PlanRunId is not null)
        {
            where += " AND plan_run_id = @plan_run_id";
            cmd.Parameters.Add(new SqlParameter("@plan_run_id", SqlDbType.NVarChar, 450) { Value = filter.PlanRunId });
        }
        if (filter.PlanStepName is not null)
        {
            where += " AND plan_step_name = @plan_step_name";
            cmd.Parameters.Add(new SqlParameter("@plan_step_name", SqlDbType.NVarChar, 450) { Value = filter.PlanStepName });
        }

        var orderCol = filter.OrderBy switch
        {
            RunOrderBy.StartedAt => "started_at",
            RunOrderBy.CompletedAt => "completed_at",
            _ => "created_at"
        };

        // Get total count
        cmd.CommandText = $"SELECT COUNT(*) FROM [{Schema}].runs {where}";
        var totalCount = (int)(await cmd.ExecuteScalarAsync(cancellationToken))!;

        // Get page
        cmd.CommandText = $"SELECT * FROM [{Schema}].runs {where} ORDER BY {orderCol} DESC OFFSET @skip ROWS FETCH NEXT @take ROWS ONLY";
        cmd.Parameters.Add(new SqlParameter("@skip", SqlDbType.Int) { Value = filter.Skip });
        cmd.Parameters.Add(new SqlParameter("@take", SqlDbType.Int) { Value = filter.Take });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var items = new List<JobRun>();
        while (await reader.ReadAsync(cancellationToken))
            items.Add(ReadRun(reader));

        return new PagedResult<JobRun> { Items = items, TotalCount = totalCount };
    }

    public async Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE [{Schema}].runs SET
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
        await using var cmd = new SqlCommand(sql, conn);
        AddRunParameters(cmd, run);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<bool> TryUpdateRunStatusAsync(JobRun run, JobStatus expectedStatus, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE [{Schema}].runs SET
                status = @status,
                error = @error,
                result = @result,
                progress = @progress,
                completed_at = @completed_at,
                cancelled_at = @cancelled_at
            WHERE id = @id AND status = @expected_status
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@id", SqlDbType.NVarChar, 450) { Value = run.Id });
        cmd.Parameters.Add(new SqlParameter("@status", SqlDbType.Int) { Value = (int)run.Status });
        cmd.Parameters.Add(NullableNVarChar("@error", run.Error));
        cmd.Parameters.Add(NullableNVarChar("@result", run.Result));
        cmd.Parameters.Add(new SqlParameter("@progress", SqlDbType.Float) { Value = run.Progress });
        cmd.Parameters.Add(NullableDto("@completed_at", run.CompletedAt));
        cmd.Parameters.Add(NullableDto("@cancelled_at", run.CancelledAt));
        cmd.Parameters.Add(new SqlParameter("@expected_status", SqlDbType.Int) { Value = (int)expectedStatus });
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0;
    }

    public async Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, IReadOnlyCollection<string> queueNames, CancellationToken cancellationToken = default)
    {
        if (registeredJobNames.Count == 0)
            return null;

        await using var conn = await OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);

        // Build IN clauses
        var jobNameParams = BuildInClause("@jn", registeredJobNames, out var jobNameClause);
        var queueNameParams = BuildInClause("@qn", queueNames, out var queueNameClause);

        // Find candidate run, lock it with UPDLOCK/ROWLOCK to prevent concurrent claims
        var selectSql = $"""
            SELECT TOP 1 r.id, r.job_name
            FROM [{Schema}].runs r WITH (UPDLOCK, ROWLOCK)
            JOIN [{Schema}].jobs j WITH (UPDLOCK, HOLDLOCK) ON j.name = r.job_name
            LEFT JOIN [{Schema}].queues q WITH (UPDLOCK, HOLDLOCK) ON q.name = COALESCE(j.queue, 'default')
            WHERE r.status = 0
              AND r.not_before <= SYSDATETIMEOFFSET()
              AND (r.not_after IS NULL OR r.not_after > SYSDATETIMEOFFSET())
              AND r.job_name IN ({jobNameClause})
              AND COALESCE(j.queue, 'default') IN ({queueNameClause})
              AND COALESCE(q.is_paused, 0) = 0
              AND (
                  j.max_concurrency IS NULL
                  OR (SELECT COUNT(*) FROM [{Schema}].runs r2 WHERE r2.job_name = r.job_name AND r2.status = 1)
                     < j.max_concurrency
              )
              AND (
                  q.max_concurrency IS NULL
                  OR (SELECT COUNT(*) FROM [{Schema}].runs r2
                      JOIN [{Schema}].jobs j2 ON j2.name = r2.job_name
                      WHERE r2.status = 1 AND COALESCE(j2.queue, 'default') = COALESCE(j.queue, 'default'))
                     < q.max_concurrency
              )
              AND (
                  j.rate_limit_name IS NULL
                  OR EXISTS (SELECT 1 FROM [{Schema}].rate_limits rl
                      WHERE rl.name = j.rate_limit_name
                        AND CASE
                            WHEN DATEADD(MILLISECOND, rl.window_duration_ms, rl.window_start) <= SYSDATETIMEOFFSET() THEN 1
                            WHEN rl.type = 0 THEN CASE WHEN rl.current_count < rl.max_permits THEN 1 ELSE 0 END
                            WHEN rl.type = 1 THEN CASE WHEN
                                COALESCE(rl.previous_count, 0) * IIF(1.0 - CAST(DATEDIFF_BIG(MILLISECOND, rl.window_start, SYSDATETIMEOFFSET()) AS FLOAT) / rl.window_duration_ms > 0,
                                    1.0 - CAST(DATEDIFF_BIG(MILLISECOND, rl.window_start, SYSDATETIMEOFFSET()) AS FLOAT) / rl.window_duration_ms, 0)
                                + rl.current_count + 1 <= rl.max_permits THEN 1 ELSE 0 END
                            ELSE CASE WHEN rl.current_count < rl.max_permits THEN 1 ELSE 0 END
                        END = 1)
              )
              AND (
                  q.rate_limit_name IS NULL
                  OR EXISTS (SELECT 1 FROM [{Schema}].rate_limits rl
                      WHERE rl.name = q.rate_limit_name
                        AND CASE
                            WHEN DATEADD(MILLISECOND, rl.window_duration_ms, rl.window_start) <= SYSDATETIMEOFFSET() THEN 1
                            WHEN rl.type = 0 THEN CASE WHEN rl.current_count < rl.max_permits THEN 1 ELSE 0 END
                            WHEN rl.type = 1 THEN CASE WHEN
                                COALESCE(rl.previous_count, 0) * IIF(1.0 - CAST(DATEDIFF_BIG(MILLISECOND, rl.window_start, SYSDATETIMEOFFSET()) AS FLOAT) / rl.window_duration_ms > 0,
                                    1.0 - CAST(DATEDIFF_BIG(MILLISECOND, rl.window_start, SYSDATETIMEOFFSET()) AS FLOAT) / rl.window_duration_ms, 0)
                                + rl.current_count + 1 <= rl.max_permits THEN 1 ELSE 0 END
                            ELSE CASE WHEN rl.current_count < rl.max_permits THEN 1 ELSE 0 END
                        END = 1)
              )
            ORDER BY COALESCE(q.priority, 0) DESC, r.priority DESC, r.not_before, r.created_at
            """;

        await using var selectCmd = new SqlCommand(selectSql, conn, tx);
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
            $"SELECT rate_limit_name FROM [{Schema}].jobs WHERE name = @name",
            [new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = jobName }],
            cancellationToken);

        // Acquire queue-level rate limit (if different from job-level)
        await AcquireRateLimitIfNeededAsync(conn, tx,
            $"SELECT q.rate_limit_name FROM [{Schema}].jobs j LEFT JOIN [{Schema}].queues q ON q.name = COALESCE(j.queue, 'default') WHERE j.name = @name AND q.rate_limit_name IS NOT NULL AND q.rate_limit_name != COALESCE(j.rate_limit_name, '')",
            [new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = jobName }],
            cancellationToken);

        // Claim the run
        var updateSql = $"""
            UPDATE [{Schema}].runs SET status = 1, node_name = @node_name, started_at = SYSDATETIMEOFFSET(), last_heartbeat_at = SYSDATETIMEOFFSET()
            WHERE id = @id
            """;
        await using var updateCmd = new SqlCommand(updateSql, conn, tx);
        updateCmd.Parameters.Add(new SqlParameter("@id", SqlDbType.NVarChar, 450) { Value = runId });
        updateCmd.Parameters.Add(new SqlParameter("@node_name", SqlDbType.NVarChar, 450) { Value = nodeName });
        await updateCmd.ExecuteNonQueryAsync(cancellationToken);

        // Read the updated run
        await using var readCmd = new SqlCommand($"SELECT * FROM [{Schema}].runs WHERE id = @id", conn, tx);
        readCmd.Parameters.Add(new SqlParameter("@id", SqlDbType.NVarChar, 450) { Value = runId });

        JobRun? result;
        await using (var reader = await readCmd.ExecuteReaderAsync(cancellationToken))
        {
            result = await reader.ReadAsync(cancellationToken) ? ReadRun(reader) : null;
        }

        await tx.CommitAsync(cancellationToken);
        return result;
    }

    private async Task AcquireRateLimitIfNeededAsync(SqlConnection conn, SqlTransaction tx, string lookupSql, SqlParameter[] lookupParams, CancellationToken cancellationToken)
    {
        await using var lookupCmd = new SqlCommand(lookupSql, conn, tx);
        foreach (var p in lookupParams) lookupCmd.Parameters.Add(p);
        var rlName = await lookupCmd.ExecuteScalarAsync(cancellationToken) as string;

        if (rlName is null) return;

        var acquireSql = $"""
            UPDATE [{Schema}].rate_limits SET
                previous_count = CASE WHEN DATEADD(MILLISECOND, window_duration_ms, window_start) <= SYSDATETIMEOFFSET() THEN current_count ELSE previous_count END,
                current_count = CASE WHEN DATEADD(MILLISECOND, window_duration_ms, window_start) <= SYSDATETIMEOFFSET() THEN 1 ELSE current_count + 1 END,
                window_start = CASE WHEN DATEADD(MILLISECOND, window_duration_ms, window_start) <= SYSDATETIMEOFFSET() THEN SYSDATETIMEOFFSET() ELSE window_start END
            WHERE name = @name
            """;

        await using var acquireCmd = new SqlCommand(acquireSql, conn, tx);
        acquireCmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = rlName });
        await acquireCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Rate Limits --

    public async Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            MERGE [{Schema}].rate_limits WITH (HOLDLOCK) AS target
            USING (SELECT @name AS name) AS source ON target.name = source.name
            WHEN MATCHED THEN UPDATE SET
                type = @type,
                max_permits = @max_permits,
                window_duration_ms = @window_duration_ms
            WHEN NOT MATCHED THEN INSERT
                (name, type, max_permits, window_duration_ms, window_start)
            VALUES
                (@name, @type, @max_permits, @window_duration_ms, SYSDATETIMEOFFSET());
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = rateLimit.Name });
        cmd.Parameters.Add(new SqlParameter("@type", SqlDbType.SmallInt) { Value = (short)rateLimit.Type });
        cmd.Parameters.Add(new SqlParameter("@max_permits", SqlDbType.Int) { Value = rateLimit.MaxPermits });
        cmd.Parameters.Add(new SqlParameter("@window_duration_ms", SqlDbType.Float) { Value = rateLimit.Window.TotalMilliseconds });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<bool> TryAcquireRateLimitAsync(string name, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE [{Schema}].rate_limits SET
                previous_count = CASE WHEN DATEADD(MILLISECOND, window_duration_ms, window_start) <= SYSDATETIMEOFFSET() THEN current_count ELSE previous_count END,
                current_count = CASE WHEN DATEADD(MILLISECOND, window_duration_ms, window_start) <= SYSDATETIMEOFFSET() THEN 1 ELSE current_count + 1 END,
                window_start = CASE WHEN DATEADD(MILLISECOND, window_duration_ms, window_start) <= SYSDATETIMEOFFSET() THEN SYSDATETIMEOFFSET() ELSE window_start END
            WHERE name = @name
              AND CASE
                  WHEN DATEADD(MILLISECOND, window_duration_ms, window_start) <= SYSDATETIMEOFFSET() THEN 1
                  WHEN type = 0 THEN CASE WHEN current_count < max_permits THEN 1 ELSE 0 END
                  WHEN type = 1 THEN CASE WHEN
                      COALESCE(previous_count, 0) * IIF(1.0 - CAST(DATEDIFF_BIG(MILLISECOND, window_start, SYSDATETIMEOFFSET()) AS FLOAT) / window_duration_ms > 0,
                          1.0 - CAST(DATEDIFF_BIG(MILLISECOND, window_start, SYSDATETIMEOFFSET()) AS FLOAT) / window_duration_ms, 0)
                      + current_count + 1 <= max_permits THEN 1 ELSE 0 END
                  ELSE CASE WHEN current_count < max_permits THEN 1 ELSE 0 END
              END = 1
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = name });
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0;
    }

    // -- Queues --

    public async Task UpsertQueueAsync(QueueDefinition queue, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            MERGE [{Schema}].queues WITH (HOLDLOCK) AS target
            USING (SELECT @name AS name) AS source ON target.name = source.name
            WHEN MATCHED THEN UPDATE SET
                priority = @priority,
                max_concurrency = @max_concurrency,
                rate_limit_name = @rate_limit_name,
                last_heartbeat_at = SYSDATETIMEOFFSET()
            WHEN NOT MATCHED THEN INSERT
                (name, priority, max_concurrency, is_paused, rate_limit_name, last_heartbeat_at)
            VALUES
                (@name, @priority, @max_concurrency, @is_paused, @rate_limit_name, SYSDATETIMEOFFSET());
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = queue.Name });
        cmd.Parameters.Add(new SqlParameter("@priority", SqlDbType.Int) { Value = queue.Priority });
        cmd.Parameters.Add(new SqlParameter("@max_concurrency", SqlDbType.Int) { Value = queue.MaxConcurrency.HasValue ? queue.MaxConcurrency.Value : DBNull.Value });
        cmd.Parameters.Add(new SqlParameter("@is_paused", SqlDbType.Bit) { Value = queue.IsPaused });
        cmd.Parameters.Add(NullableNVarChar("@rate_limit_name", queue.RateLimitName));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<QueueDefinition?> GetQueueAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand($"SELECT * FROM [{Schema}].queues WHERE name = @name", conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = name });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadQueue(reader) : null;
    }

    public async Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand($"SELECT * FROM [{Schema}].queues ORDER BY name", conn);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        var queues = new List<QueueDefinition>();
        while (await reader.ReadAsync(cancellationToken))
            queues.Add(ReadQueue(reader));
        return queues;
    }

    public async Task SetQueuePausedAsync(string name, bool isPaused, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand($"UPDATE [{Schema}].queues SET is_paused = @is_paused WHERE name = @name", conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = name });
        cmd.Parameters.Add(new SqlParameter("@is_paused", SqlDbType.Bit) { Value = isPaused });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Nodes --

    public async Task RegisterNodeAsync(NodeInfo node, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            MERGE [{Schema}].nodes WITH (HOLDLOCK) AS target
            USING (SELECT @name AS name) AS source ON target.name = source.name
            WHEN MATCHED THEN UPDATE SET
                started_at = @started_at,
                last_heartbeat_at = @last_heartbeat_at,
                running_count = @running_count,
                registered_job_names = @registered_job_names,
                registered_queue_names = @registered_queue_names
            WHEN NOT MATCHED THEN INSERT
                (name, started_at, last_heartbeat_at, running_count, registered_job_names, registered_queue_names)
            VALUES
                (@name, @started_at, @last_heartbeat_at, @running_count, @registered_job_names, @registered_queue_names);
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = node.Name });
        cmd.Parameters.Add(new SqlParameter("@started_at", SqlDbType.DateTimeOffset) { Value = node.StartedAt });
        cmd.Parameters.Add(new SqlParameter("@last_heartbeat_at", SqlDbType.DateTimeOffset) { Value = node.LastHeartbeatAt });
        cmd.Parameters.Add(new SqlParameter("@running_count", SqlDbType.Int) { Value = node.RunningCount });
        cmd.Parameters.Add(new SqlParameter("@registered_job_names", SqlDbType.NVarChar) { Value = JsonSerializer.Serialize(node.RegisteredJobNames, JsonOptions) });
        cmd.Parameters.Add(new SqlParameter("@registered_queue_names", SqlDbType.NVarChar) { Value = JsonSerializer.Serialize(node.RegisteredQueueNames, JsonOptions) });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, IReadOnlyCollection<string> registeredQueueNames, CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE [{Schema}].nodes SET
                last_heartbeat_at = SYSDATETIMEOFFSET(),
                running_count = (SELECT COUNT(*) FROM [{Schema}].runs WHERE node_name = @name AND status = 1),
                registered_job_names = @registered_job_names,
                registered_queue_names = @registered_queue_names
            WHERE name = @name
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = nodeName });
        cmd.Parameters.Add(new SqlParameter("@registered_job_names", SqlDbType.NVarChar) { Value = JsonSerializer.Serialize(registeredJobNames, JsonOptions) });
        cmd.Parameters.Add(new SqlParameter("@registered_queue_names", SqlDbType.NVarChar) { Value = JsonSerializer.Serialize(registeredQueueNames, JsonOptions) });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task RemoveNodeAsync(string nodeName, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand($"DELETE FROM [{Schema}].nodes WHERE name = @name", conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = nodeName });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<NodeInfo?> GetNodeAsync(string nodeName, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand($"SELECT * FROM [{Schema}].nodes WHERE name = @name", conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = nodeName });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadNode(reader) : null;
    }

    public async Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand($"SELECT * FROM [{Schema}].nodes ORDER BY name", conn);
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
            INSERT INTO [{Schema}].run_events (run_id, event_type, payload, created_at)
            VALUES (@run_id, @event_type, @payload, @created_at)
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@run_id", SqlDbType.NVarChar, 450) { Value = evt.RunId });
        cmd.Parameters.Add(new SqlParameter("@event_type", SqlDbType.SmallInt) { Value = (short)evt.EventType });
        cmd.Parameters.Add(new SqlParameter("@payload", SqlDbType.NVarChar) { Value = evt.Payload });
        cmd.Parameters.Add(new SqlParameter("@created_at", SqlDbType.DateTimeOffset) { Value = evt.CreatedAt });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default)
    {
        if (events.Count == 0) return;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand { Connection = conn };

        var sb = new System.Text.StringBuilder();
        sb.Append($"INSERT INTO [{Schema}].run_events (run_id, event_type, payload, created_at) VALUES ");

        for (var i = 0; i < events.Count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append($"(@r{i},@t{i},@p{i},@c{i})");
            cmd.Parameters.Add(new SqlParameter($"@r{i}", SqlDbType.NVarChar, 450) { Value = events[i].RunId });
            cmd.Parameters.Add(new SqlParameter($"@t{i}", SqlDbType.SmallInt) { Value = (short)events[i].EventType });
            cmd.Parameters.Add(new SqlParameter($"@p{i}", SqlDbType.NVarChar) { Value = events[i].Payload });
            cmd.Parameters.Add(new SqlParameter($"@c{i}", SqlDbType.DateTimeOffset) { Value = events[i].CreatedAt });
        }

        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0, RunEventType[]? types = null, CancellationToken cancellationToken = default)
    {
        var sql = $"SELECT id, run_id, event_type, payload, created_at FROM [{Schema}].run_events WHERE run_id = @run_id AND id > @since_id";

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand { Connection = conn };
        cmd.Parameters.Add(new SqlParameter("@run_id", SqlDbType.NVarChar, 450) { Value = runId });
        cmd.Parameters.Add(new SqlParameter("@since_id", SqlDbType.BigInt) { Value = sinceId });

        if (types is { Length: > 0 })
        {
            var typeParams = BuildInClause("@et", types.Select(t => (int)t), out var typeClause);
            sql += $" AND event_type IN ({typeClause})";
            foreach (var p in typeParams) cmd.Parameters.Add(p);
        }

        sql += " ORDER BY id";
        cmd.CommandText = sql;

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
                CreatedAt = reader.GetDateTimeOffset(4)
            });
        }
        return result;
    }

    // -- Heartbeat & Inactive Runs --

    public async Task HeartbeatRunsAsync(IReadOnlyCollection<string> runIds, CancellationToken cancellationToken = default)
    {
        if (runIds.Count == 0) return;

        var idParams = BuildInClause("@rid", runIds, out var idClause);

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand($"UPDATE [{Schema}].runs SET last_heartbeat_at = SYSDATETIMEOFFSET() WHERE id IN ({idClause}) AND status = 1", conn);
        foreach (var p in idParams) cmd.Parameters.Add(p);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<JobRun>> GetInactiveRunsAsync(TimeSpan threshold, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(
            $"SELECT * FROM [{Schema}].runs WHERE status = 1 AND (last_heartbeat_at IS NULL OR last_heartbeat_at < DATEADD(MILLISECOND, -@threshold_ms, SYSDATETIMEOFFSET()))", conn);
        cmd.Parameters.Add(new SqlParameter("@threshold_ms", SqlDbType.Float) { Value = threshold.TotalMilliseconds });

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
            WITH ancestors AS (
                SELECT r.* FROM [{Schema}].runs r WHERE r.id = @id
                UNION ALL
                SELECT p.* FROM [{Schema}].runs p JOIN ancestors a ON a.parent_run_id = p.id
            ),
            root AS (
                SELECT TOP 1 a.id FROM ancestors a
                WHERE NOT EXISTS (SELECT 1 FROM ancestors b WHERE b.id = a.parent_run_id)
            ),
            descendants AS (
                SELECT r.* FROM [{Schema}].runs r JOIN root ON r.id = root.id
                UNION ALL
                SELECT c.* FROM [{Schema}].runs c JOIN descendants d ON c.parent_run_id = d.id
            )
            SELECT TOP (@limit) * FROM descendants ORDER BY created_at
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@id", SqlDbType.NVarChar, 450) { Value = runId });
        cmd.Parameters.Add(new SqlParameter("@limit", SqlDbType.Int) { Value = limit });

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
                (SELECT COUNT(*) FROM [{Schema}].jobs) AS total_jobs,
                (SELECT COUNT(*) FROM [{Schema}].nodes) AS node_count,
                COUNT(*) AS total_runs,
                SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS running,
                SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) AS failed,
                SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END) AS cancelled,
                SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END) AS dead_letter,
                SUM(CASE WHEN status = 6 THEN 1 ELSE 0 END) AS skipped
            FROM [{Schema}].runs WHERE 1=1{sinceFilter}
            """;

        await using var statsCmd = new SqlCommand(statsSql, conn);
        if (since is not null)
            statsCmd.Parameters.Add(new SqlParameter("@since", SqlDbType.DateTimeOffset) { Value = since.Value });

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
        await using var recentCmd = new SqlCommand(
            $"SELECT TOP 10 * FROM [{Schema}].runs WHERE 1=1{sinceFilter} ORDER BY created_at DESC", conn);
        if (since is not null)
            recentCmd.Parameters.Add(new SqlParameter("@since", SqlDbType.DateTimeOffset) { Value = since.Value });

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
                    CAST(DATEDIFF_BIG(SECOND, '1970-01-01', created_at) / @bucket_seconds * @bucket_seconds AS BIGINT) AS bucket_epoch,
                    SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS running,
                    SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END) AS cancelled,
                    SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END) AS dead_letter,
                    SUM(CASE WHEN status = 6 THEN 1 ELSE 0 END) AS skipped
                FROM [{Schema}].runs
                WHERE created_at >= @since
                GROUP BY DATEDIFF_BIG(SECOND, '1970-01-01', created_at) / @bucket_seconds * @bucket_seconds
                ORDER BY bucket_epoch
                """;

            await using var tlCmd = new SqlCommand(tlSql, conn);
            tlCmd.Parameters.Add(new SqlParameter("@bucket_seconds", SqlDbType.Int) { Value = bucketSeconds });
            tlCmd.Parameters.Add(new SqlParameter("@since", SqlDbType.DateTimeOffset) { Value = since.Value });

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
        await using var cmd = new SqlCommand($"""
            SELECT
                COUNT(*) AS total_runs,
                SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS succeeded,
                SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) AS failed,
                AVG(CASE WHEN status IN (2, 3) AND started_at IS NOT NULL AND completed_at IS NOT NULL
                    THEN CAST(DATEDIFF_BIG(MILLISECOND, started_at, completed_at) AS FLOAT) / 1000.0
                    ELSE NULL END) AS avg_duration_secs,
                MAX(created_at) AS last_run_at
            FROM [{Schema}].runs WHERE job_name = @name
            """, conn);
        cmd.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 450) { Value = jobName });

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);

        var total = Convert.ToInt32(reader.GetValue(0));
        var succeeded = Convert.ToInt32(reader.GetValue(1));
        var failed = Convert.ToInt32(reader.GetValue(2));
        var totalFinished = succeeded + failed;
        var avgDurationSecs = reader.IsDBNull(3) ? (double?)null : reader.GetDouble(3);
        var lastRunAt = reader.IsDBNull(4) ? (DateTimeOffset?)null : reader.GetDateTimeOffset(4);

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
        await using var cmd = new SqlCommand($"""
            SELECT COALESCE(j.queue, 'default') AS queue_name,
                   SUM(CASE WHEN r.status = 0 THEN 1 ELSE 0 END) AS pending,
                   SUM(CASE WHEN r.status = 1 THEN 1 ELSE 0 END) AS running
            FROM [{Schema}].runs r
            JOIN [{Schema}].jobs j ON j.name = r.job_name
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

    // -- Cancel Expired --

    public async Task<int> CancelExpiredRunsAsync(CancellationToken cancellationToken = default)
    {
        var sql = $"""
            UPDATE [{Schema}].runs
            SET status = 4, cancelled_at = SYSDATETIMEOFFSET()
            WHERE status = 0
              AND not_after IS NOT NULL
              AND not_after < SYSDATETIMEOFFSET()
            """;

        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(sql, conn);
        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Purge --

    public async Task<int> PurgeRunsAsync(DateTimeOffset completedBefore, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);

        // Delete events for runs being purged
        await using (var evtCmd = new SqlCommand($"""
            DELETE FROM [{Schema}].run_events WHERE run_id IN (
                SELECT id FROM [{Schema}].runs
                WHERE (status IN (2, 3, 4, 5, 6) AND completed_at < @before)
                   OR (status = 0 AND not_before < @before)
            )
            """, conn, tx))
        {
            evtCmd.Parameters.Add(new SqlParameter("@before", SqlDbType.DateTimeOffset) { Value = completedBefore });
            await evtCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // Delete runs
        await using var cmd = new SqlCommand($"""
            DELETE FROM [{Schema}].runs
            WHERE (status IN (2, 3, 4, 5, 6) AND completed_at < @before)
               OR (status = 0 AND not_before < @before)
            """, conn, tx);
        cmd.Parameters.Add(new SqlParameter("@before", SqlDbType.DateTimeOffset) { Value = completedBefore });
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);

        await tx.CommitAsync(cancellationToken);
        return rows;
    }

    public async Task<int> PurgeJobsAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(
            $"DELETE FROM [{Schema}].jobs WHERE last_heartbeat_at IS NOT NULL AND last_heartbeat_at < @before", conn);
        cmd.Parameters.Add(new SqlParameter("@before", SqlDbType.DateTimeOffset) { Value = heartbeatBefore });
        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> PurgeNodesAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(
            $"DELETE FROM [{Schema}].nodes WHERE last_heartbeat_at < @before", conn);
        cmd.Parameters.Add(new SqlParameter("@before", SqlDbType.DateTimeOffset) { Value = heartbeatBefore });
        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> PurgeQueuesAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default)
    {
        await using var conn = await OpenAsync(cancellationToken);
        await using var cmd = new SqlCommand(
            $"DELETE FROM [{Schema}].queues WHERE last_heartbeat_at IS NOT NULL AND last_heartbeat_at < @before", conn);
        cmd.Parameters.Add(new SqlParameter("@before", SqlDbType.DateTimeOffset) { Value = heartbeatBefore });
        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    // -- Helpers --

    private async Task<SqlConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var conn = new SqlConnection(sqlServerOptions.ConnectionString);
        await conn.OpenAsync(cancellationToken);
        return conn;
    }

    private async Task InsertRunAsync(SqlConnection conn, SqlTransaction? tx, JobRun run, CancellationToken cancellationToken)
    {
        var sql = $"""
            INSERT INTO [{Schema}].runs (id, job_name, status, arguments, result, error, progress, created_at, started_at, completed_at, cancelled_at, node_name, attempt, trace_id, span_id, parent_run_id, retry_of_run_id, rerun_of_run_id, not_before, not_after, priority, deduplication_id, last_heartbeat_at, plan_run_id, plan_step_id, plan_step_name, plan_graph)
            VALUES (@id, @job_name, @status, @arguments, @result, @error, @progress, @created_at, @started_at, @completed_at, @cancelled_at, @node_name, @attempt, @trace_id, @span_id, @parent_run_id, @retry_of_run_id, @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id, @last_heartbeat_at, @plan_run_id, @plan_step_id, @plan_step_name, @plan_graph)
            """;

        await using var cmd = new SqlCommand(sql, conn, tx);
        AddRunParameters(cmd, run);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddRunParameters(SqlCommand cmd, JobRun run)
    {
        cmd.Parameters.Add(new SqlParameter("@id", SqlDbType.NVarChar, 450) { Value = run.Id });
        cmd.Parameters.Add(new SqlParameter("@job_name", SqlDbType.NVarChar, 450) { Value = run.JobName });
        cmd.Parameters.Add(new SqlParameter("@status", SqlDbType.Int) { Value = (int)run.Status });
        cmd.Parameters.Add(NullableNVarChar("@arguments", run.Arguments));
        cmd.Parameters.Add(NullableNVarChar("@result", run.Result));
        cmd.Parameters.Add(NullableNVarChar("@error", run.Error));
        cmd.Parameters.Add(new SqlParameter("@progress", SqlDbType.Float) { Value = run.Progress });
        cmd.Parameters.Add(new SqlParameter("@created_at", SqlDbType.DateTimeOffset) { Value = run.CreatedAt });
        cmd.Parameters.Add(NullableDto("@started_at", run.StartedAt));
        cmd.Parameters.Add(NullableDto("@completed_at", run.CompletedAt));
        cmd.Parameters.Add(NullableDto("@cancelled_at", run.CancelledAt));
        cmd.Parameters.Add(NullableNVarChar("@node_name", run.NodeName));
        cmd.Parameters.Add(new SqlParameter("@attempt", SqlDbType.Int) { Value = run.Attempt });
        cmd.Parameters.Add(NullableNVarChar("@trace_id", run.TraceId));
        cmd.Parameters.Add(NullableNVarChar("@span_id", run.SpanId));
        cmd.Parameters.Add(NullableNVarChar("@parent_run_id", run.ParentRunId));
        cmd.Parameters.Add(NullableNVarChar("@retry_of_run_id", run.RetryOfRunId));
        cmd.Parameters.Add(NullableNVarChar("@rerun_of_run_id", run.RerunOfRunId));
        cmd.Parameters.Add(new SqlParameter("@not_before", SqlDbType.DateTimeOffset) { Value = run.NotBefore });
        cmd.Parameters.Add(NullableDto("@not_after", run.NotAfter));
        cmd.Parameters.Add(new SqlParameter("@priority", SqlDbType.Int) { Value = run.Priority });
        cmd.Parameters.Add(NullableNVarChar("@deduplication_id", run.DeduplicationId));
        cmd.Parameters.Add(NullableDto("@last_heartbeat_at", run.LastHeartbeatAt));
        cmd.Parameters.Add(NullableNVarChar("@plan_run_id", run.PlanRunId));
        cmd.Parameters.Add(NullableNVarChar("@plan_step_id", run.PlanStepId));
        cmd.Parameters.Add(NullableNVarChar("@plan_step_name", run.PlanStepName));
        cmd.Parameters.Add(NullableNVarChar("@plan_graph", run.PlanGraph));
    }

    private static SqlParameter NullableNVarChar(string name, string? value) =>
        new(name, SqlDbType.NVarChar) { Value = (object?)value ?? DBNull.Value };

    private static SqlParameter NullableDto(string name, DateTimeOffset? value) =>
        new(name, SqlDbType.DateTimeOffset) { Value = value.HasValue ? value.Value : DBNull.Value };

    private JobDefinition ReadJob(SqlDataReader reader)
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
            IsContinuous = reader.GetBoolean(reader.GetOrdinal("is_continuous")),
            Queue = reader.IsDBNull(reader.GetOrdinal("queue")) ? null : reader.GetString(reader.GetOrdinal("queue")),
            RateLimitName = reader.IsDBNull(reader.GetOrdinal("rate_limit_name")) ? null : reader.GetString(reader.GetOrdinal("rate_limit_name")),
            IsEnabled = reader.GetBoolean(reader.GetOrdinal("is_enabled")),
            IsPlan = reader.GetBoolean(reader.GetOrdinal("is_plan")),
            IsInternal = reader.GetBoolean(reader.GetOrdinal("is_internal")),
            PlanGraph = reader.IsDBNull(reader.GetOrdinal("plan_graph")) ? null : reader.GetString(reader.GetOrdinal("plan_graph")),
            MisfirePolicy = (MisfirePolicy)reader.GetInt32(reader.GetOrdinal("misfire_policy")),
            ArgumentsSchema = reader.IsDBNull(reader.GetOrdinal("arguments_schema")) ? null : reader.GetString(reader.GetOrdinal("arguments_schema")),
            LastHeartbeatAt = reader.IsDBNull(reader.GetOrdinal("last_heartbeat_at")) ? null : reader.GetDateTimeOffset(reader.GetOrdinal("last_heartbeat_at"))
        };
    }

    private static QueueDefinition ReadQueue(SqlDataReader reader) => new()
    {
        Name = reader.GetString(reader.GetOrdinal("name")),
        Priority = reader.GetInt32(reader.GetOrdinal("priority")),
        MaxConcurrency = reader.IsDBNull(reader.GetOrdinal("max_concurrency")) ? null : reader.GetInt32(reader.GetOrdinal("max_concurrency")),
        IsPaused = reader.GetBoolean(reader.GetOrdinal("is_paused")),
        RateLimitName = reader.IsDBNull(reader.GetOrdinal("rate_limit_name")) ? null : reader.GetString(reader.GetOrdinal("rate_limit_name")),
        LastHeartbeatAt = reader.IsDBNull(reader.GetOrdinal("last_heartbeat_at")) ? null : reader.GetDateTimeOffset(reader.GetOrdinal("last_heartbeat_at"))
    };

    private static JobRun ReadRun(SqlDataReader reader)
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
            CreatedAt = reader.GetDateTimeOffset(reader.GetOrdinal("created_at")),
            StartedAt = reader.IsDBNull(reader.GetOrdinal("started_at")) ? null : reader.GetDateTimeOffset(reader.GetOrdinal("started_at")),
            CompletedAt = reader.IsDBNull(reader.GetOrdinal("completed_at")) ? null : reader.GetDateTimeOffset(reader.GetOrdinal("completed_at")),
            CancelledAt = reader.IsDBNull(reader.GetOrdinal("cancelled_at")) ? null : reader.GetDateTimeOffset(reader.GetOrdinal("cancelled_at")),
            NodeName = reader.IsDBNull(reader.GetOrdinal("node_name")) ? null : reader.GetString(reader.GetOrdinal("node_name")),
            Attempt = reader.GetInt32(reader.GetOrdinal("attempt")),
            TraceId = reader.IsDBNull(reader.GetOrdinal("trace_id")) ? null : reader.GetString(reader.GetOrdinal("trace_id")),
            SpanId = reader.IsDBNull(reader.GetOrdinal("span_id")) ? null : reader.GetString(reader.GetOrdinal("span_id")),
            ParentRunId = reader.IsDBNull(reader.GetOrdinal("parent_run_id")) ? null : reader.GetString(reader.GetOrdinal("parent_run_id")),
            RetryOfRunId = reader.IsDBNull(reader.GetOrdinal("retry_of_run_id")) ? null : reader.GetString(reader.GetOrdinal("retry_of_run_id")),
            RerunOfRunId = reader.IsDBNull(reader.GetOrdinal("rerun_of_run_id")) ? null : reader.GetString(reader.GetOrdinal("rerun_of_run_id")),
            NotBefore = reader.GetDateTimeOffset(reader.GetOrdinal("not_before")),
            NotAfter = reader.IsDBNull(reader.GetOrdinal("not_after")) ? null : reader.GetDateTimeOffset(reader.GetOrdinal("not_after")),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            DeduplicationId = reader.IsDBNull(reader.GetOrdinal("deduplication_id")) ? null : reader.GetString(reader.GetOrdinal("deduplication_id")),
            LastHeartbeatAt = reader.IsDBNull(reader.GetOrdinal("last_heartbeat_at")) ? null : reader.GetDateTimeOffset(reader.GetOrdinal("last_heartbeat_at")),
            PlanRunId = reader.IsDBNull(reader.GetOrdinal("plan_run_id")) ? null : reader.GetString(reader.GetOrdinal("plan_run_id")),
            PlanStepId = reader.IsDBNull(reader.GetOrdinal("plan_step_id")) ? null : reader.GetString(reader.GetOrdinal("plan_step_id")),
            PlanStepName = reader.IsDBNull(reader.GetOrdinal("plan_step_name")) ? null : reader.GetString(reader.GetOrdinal("plan_step_name")),
            PlanGraph = reader.IsDBNull(reader.GetOrdinal("plan_graph")) ? null : reader.GetString(reader.GetOrdinal("plan_graph"))
        };
    }

    private static NodeInfo ReadNode(SqlDataReader reader)
    {
        return new NodeInfo
        {
            Name = reader.GetString(reader.GetOrdinal("name")),
            StartedAt = reader.GetDateTimeOffset(reader.GetOrdinal("started_at")),
            LastHeartbeatAt = reader.GetDateTimeOffset(reader.GetOrdinal("last_heartbeat_at")),
            RunningCount = reader.GetInt32(reader.GetOrdinal("running_count")),
            RegisteredJobNames = JsonSerializer.Deserialize<string[]>(reader.GetString(reader.GetOrdinal("registered_job_names"))) ?? [],
            RegisteredQueueNames = JsonSerializer.Deserialize<string[]>(reader.GetString(reader.GetOrdinal("registered_queue_names"))) ?? []
        };
    }

    private static string EscapeLike(string input) =>
        input.Replace(@"\", @"\\").Replace("%", @"\%").Replace("_", @"\_");

    private static SqlParameter[] BuildInClause<T>(string prefix, IEnumerable<T> values, out string clause)
    {
        var list = values.ToList();
        var parameters = new SqlParameter[list.Count];
        var parts = new string[list.Count];
        for (var i = 0; i < list.Count; i++)
        {
            var name = $"{prefix}{i}";
            parts[i] = name;
            parameters[i] = new SqlParameter(name, list[i]);
        }
        clause = string.Join(", ", parts);
        return parameters;
    }
}
