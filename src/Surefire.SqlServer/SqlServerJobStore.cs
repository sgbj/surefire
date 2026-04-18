using System.Data;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;

namespace Surefire.SqlServer;

/// <summary>
///     SQL Server implementation of <see cref="IJobStore" />.
/// </summary>
internal sealed class SqlServerJobStore(
    string connectionString,
    TimeSpan? commandTimeout,
    TimeProvider timeProvider) : IJobStore
{
    private readonly string _connectionString = connectionString;

    internal int? CommandTimeoutSeconds { get; } =
        CommandTimeouts.ToSeconds(commandTimeout, nameof(commandTimeout));

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT 1";
        _ = await cmd.ExecuteScalarAsync(cancellationToken);
    }

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        await using var lockCmd = CreateCommand(conn);
        lockCmd.CommandText = """
                              DECLARE @result INT;
                              EXEC @result = sp_getapplock
                                  @Resource = 'surefire_migrate',
                                  @LockMode = 'Exclusive',
                                  @LockOwner = 'Session',
                                  @LockTimeout = 30000;
                              IF @result < 0
                                  THROW 50001, 'Failed to acquire migration lock', 1;
                              """;
        await lockCmd.ExecuteNonQueryAsync(cancellationToken);

        try
        {
            await using var migCmd = CreateCommand(conn);
            migCmd.CommandText = """
                                 IF OBJECT_ID('dbo.surefire_schema_migrations', 'U') IS NULL
                                 CREATE TABLE dbo.surefire_schema_migrations (version INT NOT NULL PRIMARY KEY);
                                 """;
            await migCmd.ExecuteNonQueryAsync(cancellationToken);

            await using var checkCmd = CreateCommand(conn);
            checkCmd.CommandText = "SELECT ISNULL(MAX(version), 0) FROM dbo.surefire_schema_migrations";
            var currentVersion = (int)(await checkCmd.ExecuteScalarAsync(cancellationToken))!;

            if (currentVersion < 1)
            {
                await using var cmd = CreateCommand(conn);
                cmd.CommandText = """
                                  IF OBJECT_ID('dbo.surefire_jobs', 'U') IS NULL
                                  CREATE TABLE dbo.surefire_jobs (
                                      name NVARCHAR(450) NOT NULL PRIMARY KEY,
                                      description NVARCHAR(MAX),
                                      tags NVARCHAR(MAX) NOT NULL DEFAULT '[]',
                                      cron_expression NVARCHAR(450),
                                      time_zone_id NVARCHAR(450),
                                      timeout BIGINT,
                                      max_concurrency INT,
                                      priority INT NOT NULL DEFAULT 0,
                                      retry_policy NVARCHAR(MAX),
                                      is_continuous BIT NOT NULL DEFAULT 0,
                                      queue NVARCHAR(450),
                                      rate_limit_name NVARCHAR(450),
                                      is_enabled BIT NOT NULL DEFAULT 1,
                                      misfire_policy INT NOT NULL DEFAULT 0,
                                      fire_all_limit INT,
                                      arguments_schema NVARCHAR(MAX),
                                      last_heartbeat_at DATETIMEOFFSET,
                                      last_cron_fire_at DATETIMEOFFSET
                                  );

                                  IF OBJECT_ID('dbo.surefire_runs', 'U') IS NULL
                                  CREATE TABLE dbo.surefire_runs (
                                      id NVARCHAR(450) NOT NULL PRIMARY KEY,
                                      job_name NVARCHAR(450) NOT NULL,
                                      status INT NOT NULL DEFAULT 0,
                                      arguments NVARCHAR(MAX),
                                      result NVARCHAR(MAX),
                                      reason NVARCHAR(MAX),
                                      progress FLOAT NOT NULL DEFAULT 0,
                                      created_at DATETIMEOFFSET NOT NULL,
                                      started_at DATETIMEOFFSET,
                                      completed_at DATETIMEOFFSET,
                                      cancelled_at DATETIMEOFFSET,
                                      node_name NVARCHAR(450),
                                      attempt INT NOT NULL DEFAULT 0,
                                      trace_id NVARCHAR(450),
                                      span_id NVARCHAR(450),
                                      parent_trace_id NVARCHAR(450),
                                      parent_span_id NVARCHAR(450),
                                      parent_run_id NVARCHAR(450),
                                      root_run_id NVARCHAR(450),
                                      rerun_of_run_id NVARCHAR(450),
                                      not_before DATETIMEOFFSET NOT NULL,
                                      not_after DATETIMEOFFSET,
                                      priority INT NOT NULL DEFAULT 0,
                                      deduplication_id NVARCHAR(450),
                                      last_heartbeat_at DATETIMEOFFSET,
                                      batch_id NVARCHAR(450),
                                      queue_priority INT NOT NULL DEFAULT 0
                                  );

                                  IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'ix_surefire_runs_claim')
                                  CREATE INDEX ix_surefire_runs_claim
                                      ON dbo.surefire_runs (queue_priority DESC, priority DESC, not_before, id)
                                      WHERE status = 0;

                                  IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'ix_surefire_runs_root')
                                  CREATE INDEX ix_surefire_runs_root
                                      ON dbo.surefire_runs (root_run_id)
                                      WHERE root_run_id IS NOT NULL;

                                  IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'ix_surefire_runs_parent')
                                  CREATE INDEX ix_surefire_runs_parent
                                      ON dbo.surefire_runs (parent_run_id, created_at, id)
                                      WHERE parent_run_id IS NOT NULL;

                                  IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'ix_surefire_runs_batch_id')
                                  CREATE INDEX ix_surefire_runs_batch_id
                                      ON dbo.surefire_runs (batch_id)
                                      WHERE batch_id IS NOT NULL;

                                  IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'ix_surefire_runs_dedup')
                                  CREATE UNIQUE INDEX ix_surefire_runs_dedup
                                      ON dbo.surefire_runs (job_name, deduplication_id)
                                      WHERE deduplication_id IS NOT NULL AND status <> 2 AND status <> 4 AND status <> 5;

                                  IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'ix_surefire_runs_completed')
                                  CREATE INDEX ix_surefire_runs_completed
                                      ON dbo.surefire_runs (completed_at, id)
                                      WHERE completed_at IS NOT NULL;

                                  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_surefire_runs_job_running')
                                  CREATE INDEX ix_surefire_runs_job_running
                                      ON dbo.surefire_runs (job_name) WHERE status = 1;

                                  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_surefire_runs_job_nonterminal')
                                  CREATE INDEX ix_surefire_runs_job_nonterminal
                                      ON dbo.surefire_runs (job_name)
                                      WHERE status <> 2 AND status <> 4 AND status <> 5;

                                  IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_surefire_runs_created')
                                  CREATE INDEX ix_surefire_runs_created
                                      ON dbo.surefire_runs (created_at DESC, id DESC);

                                  IF OBJECT_ID('dbo.surefire_batches', 'U') IS NULL
                                  CREATE TABLE dbo.surefire_batches (
                                      id NVARCHAR(450) NOT NULL PRIMARY KEY,
                                      status SMALLINT NOT NULL DEFAULT 0,
                                      total INT NOT NULL DEFAULT 0,
                                      succeeded INT NOT NULL DEFAULT 0,
                                      failed INT NOT NULL DEFAULT 0,
                                      cancelled INT NOT NULL DEFAULT 0,
                                      created_at DATETIMEOFFSET NOT NULL,
                                      completed_at DATETIMEOFFSET
                                  );

                                  IF OBJECT_ID('dbo.surefire_events', 'U') IS NULL
                                  CREATE TABLE dbo.surefire_events (
                                      id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                                      run_id NVARCHAR(450) NOT NULL,
                                      event_type SMALLINT NOT NULL,
                                      payload NVARCHAR(MAX) NOT NULL,
                                      created_at DATETIMEOFFSET NOT NULL,
                                      attempt INT NOT NULL DEFAULT 1,
                                      CONSTRAINT fk_events_run_id FOREIGN KEY (run_id) REFERENCES dbo.surefire_runs(id) ON DELETE CASCADE
                                  );

                                  IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'ix_surefire_events_run')
                                  CREATE INDEX ix_surefire_events_run
                                      ON dbo.surefire_events (run_id, id);

                                  IF OBJECT_ID('dbo.surefire_nodes', 'U') IS NULL
                                  CREATE TABLE dbo.surefire_nodes (
                                      name NVARCHAR(450) NOT NULL PRIMARY KEY,
                                      started_at DATETIMEOFFSET NOT NULL,
                                      last_heartbeat_at DATETIMEOFFSET NOT NULL,
                                      running_count INT NOT NULL DEFAULT 0,
                                      registered_job_names NVARCHAR(MAX) NOT NULL DEFAULT '[]',
                                      registered_queue_names NVARCHAR(MAX) NOT NULL DEFAULT '[]'
                                  );

                                  IF OBJECT_ID('dbo.surefire_queues', 'U') IS NULL
                                  CREATE TABLE dbo.surefire_queues (
                                      name NVARCHAR(450) NOT NULL PRIMARY KEY,
                                      priority INT NOT NULL DEFAULT 0,
                                      max_concurrency INT,
                                      is_paused BIT NOT NULL DEFAULT 0,
                                      rate_limit_name NVARCHAR(450),
                                      last_heartbeat_at DATETIMEOFFSET
                                  );

                                  IF OBJECT_ID('dbo.surefire_rate_limits', 'U') IS NULL
                                  CREATE TABLE dbo.surefire_rate_limits (
                                      name NVARCHAR(450) NOT NULL PRIMARY KEY,
                                      type INT NOT NULL DEFAULT 0,
                                      max_permits INT NOT NULL,
                                      [window] BIGINT NOT NULL,
                                      last_heartbeat_at DATETIMEOFFSET,
                                      current_count INT NOT NULL DEFAULT 0,
                                      previous_count INT NOT NULL DEFAULT 0,
                                      window_start DATETIMEOFFSET
                                  );

                                  INSERT INTO dbo.surefire_schema_migrations (version)
                                  SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM dbo.surefire_schema_migrations WHERE version = 1);
                                  """;
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        catch
        {
            try
            {
                await ReleaseMigrationLockAsync(conn);
            }
            catch
            {
                /* don't mask the primary exception */
            }

            throw;
        }

        await ReleaseMigrationLockAsync(conn);
    }

    public async Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          MERGE dbo.surefire_jobs WITH (HOLDLOCK) AS target
                          USING (SELECT @name AS name) AS source ON target.name = source.name
                          WHEN MATCHED THEN UPDATE SET
                              description = @description,
                              tags = @tags,
                              cron_expression = @cron_expression,
                              time_zone_id = @time_zone_id,
                              timeout = @timeout,
                              max_concurrency = @max_concurrency,
                              priority = @priority,
                              retry_policy = @retry_policy,
                              is_continuous = @is_continuous,
                              queue = @queue,
                              rate_limit_name = @rate_limit_name,
                              misfire_policy = @misfire_policy,
                              fire_all_limit = @fire_all_limit,
                              arguments_schema = @arguments_schema,
                              last_heartbeat_at = SYSUTCDATETIME()
                          WHEN NOT MATCHED THEN INSERT (
                              name, description, tags, cron_expression, time_zone_id, timeout,
                              max_concurrency, priority, retry_policy, is_continuous, queue,
                              rate_limit_name, is_enabled, misfire_policy, fire_all_limit, arguments_schema,
                              last_heartbeat_at
                          ) VALUES (
                              @name, @description, @tags, @cron_expression, @time_zone_id, @timeout,
                              @max_concurrency, @priority, @retry_policy, @is_continuous, @queue,
                              @rate_limit_name, @is_enabled, @misfire_policy, @fire_all_limit, @arguments_schema,
                              SYSUTCDATETIME()
                          );
                          """;

        cmd.Parameters.Add(CreateParameter("@name", job.Name));
        cmd.Parameters.Add(CreateParameter("@description", (object?)job.Description ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@tags", JsonSerializer.Serialize(job.Tags)));
        cmd.Parameters.Add(CreateParameter("@cron_expression", (object?)job.CronExpression ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@time_zone_id", (object?)job.TimeZoneId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@timeout", job.Timeout.HasValue ? job.Timeout.Value.Ticks : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@max_concurrency",
            job.MaxConcurrency.HasValue ? job.MaxConcurrency.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@priority", job.Priority));
        cmd.Parameters.Add(CreateParameter("@retry_policy", SerializeRetryPolicy(job.RetryPolicy)));
        cmd.Parameters.Add(CreateParameter("@is_continuous", job.IsContinuous));
        cmd.Parameters.Add(CreateParameter("@queue", (object?)job.Queue ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@rate_limit_name", (object?)job.RateLimitName ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@is_enabled", job.IsEnabled));
        cmd.Parameters.Add(CreateParameter("@misfire_policy", (int)job.MisfirePolicy));
        cmd.Parameters.Add(CreateParameter("@fire_all_limit",
            job.FireAllLimit.HasValue ? job.FireAllLimit.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@arguments_schema", (object?)job.ArgumentsSchema ?? DBNull.Value));

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM dbo.surefire_jobs WHERE name = @name";
        cmd.Parameters.Add(CreateParameter("@name", name));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return ReadJob(reader);
    }

    public async Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);

        var sb = new StringBuilder("SELECT * FROM dbo.surefire_jobs WHERE 1=1");

        if (filter?.Name is { } nameFilter)
        {
            sb.Append(" AND name LIKE '%' + @name + '%'");
            cmd.Parameters.Add(CreateParameter("@name", EscapeLike(nameFilter)));
        }

        if (filter?.Tag is { } tagFilter)
        {
            sb.Append(" AND LOWER(@tag) IN (SELECT LOWER(value) FROM OPENJSON(tags))");
            cmd.Parameters.Add(CreateParameter("@tag", tagFilter));
        }

        if (filter?.IsEnabled is { } enabledFilter)
        {
            sb.Append(" AND is_enabled = @is_enabled");
            cmd.Parameters.Add(CreateParameter("@is_enabled", enabledFilter));
        }

        if (filter?.HeartbeatAfter is { } heartbeatFilter)
        {
            sb.Append(" AND last_heartbeat_at > @heartbeat_after");
            cmd.Parameters.Add(CreateParameter("@heartbeat_after", heartbeatFilter));
        }

        sb.Append(" ORDER BY name");
        cmd.CommandText = sb.ToString();

        var results = new List<JobDefinition>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadJob(reader));
        }

        return results;
    }

    public async Task SetJobEnabledAsync(string name, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "UPDATE dbo.surefire_jobs SET is_enabled = @enabled WHERE name = @name";
        cmd.Parameters.Add(CreateParameter("@name", name));
        cmd.Parameters.Add(CreateParameter("@enabled", enabled));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateLastCronFireAtAsync(string jobName, DateTimeOffset fireAt,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "UPDATE dbo.surefire_jobs SET last_cron_fire_at = @fire_at WHERE name = @name";
        cmd.Parameters.Add(CreateParameter("@name", jobName));
        cmd.Parameters.Add(CreateParameter("@fire_at", fireAt));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public Task CreateRunsAsync(IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
        => CreateRunsCoreAsync(runs, initialEvents, cancellationToken);

    public Task<bool> TryCreateRunAsync(JobRun run, int? maxActiveForJob = null,
        DateTimeOffset? lastCronFireAt = null,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
        => TryCreateRunCoreAsync(run, maxActiveForJob, lastCronFireAt, initialEvents, cancellationToken);

    public async Task<JobRun?> GetRunAsync(string id, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM dbo.surefire_runs WHERE id = @id";
        cmd.Parameters.Add(CreateParameter("@id", id));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return ReadRun(reader);
    }

    public async Task<IReadOnlyList<JobRun>> GetRunsByIdsAsync(IReadOnlyList<string> ids,
        CancellationToken cancellationToken = default)
    {
        if (ids.Count == 0)
        {
            return [];
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        var placeholders = new string[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            var p = "@id" + i;
            placeholders[i] = p;
            cmd.Parameters.Add(CreateParameter(p, ids[i]));
        }

        cmd.CommandText = $"SELECT * FROM dbo.surefire_runs WHERE id IN ({string.Join(",", placeholders)})";

        var byId = new Dictionary<string, JobRun>(ids.Count, StringComparer.Ordinal);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var run = ReadRun(reader);
            byId[run.Id] = run;
        }

        var result = new List<JobRun>(byId.Count);
        foreach (var id in ids)
        {
            if (byId.TryGetValue(id, out var run))
            {
                result.Add(run);
            }
        }

        return result;
    }

    public async Task<DirectChildrenPage> GetDirectChildrenAsync(string parentRunId,
        string? afterCursor = null,
        string? beforeCursor = null,
        int take = 50,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(take, 0);
        if (!string.IsNullOrEmpty(afterCursor) && !string.IsNullOrEmpty(beforeCursor))
        {
            throw new ArgumentException(
                "afterCursor and beforeCursor are mutually exclusive.", nameof(afterCursor));
        }

        var after = DirectChildrenPage.DecodeCursor(afterCursor);
        var before = DirectChildrenPage.DecodeCursor(beforeCursor);

        string sql;
        if (before is { })
        {
            sql = """
                  SELECT TOP (@take) * FROM dbo.surefire_runs
                  WHERE parent_run_id = @parent
                    AND (created_at < @cts OR (created_at = @cts AND id < @cid))
                  ORDER BY created_at DESC, id DESC
                  """;
        }
        else if (after is { })
        {
            sql = """
                  SELECT TOP (@take) * FROM dbo.surefire_runs
                  WHERE parent_run_id = @parent
                    AND (created_at > @cts OR (created_at = @cts AND id > @cid))
                  ORDER BY created_at, id
                  """;
        }
        else
        {
            sql = """
                  SELECT TOP (@take) * FROM dbo.surefire_runs
                  WHERE parent_run_id = @parent
                  ORDER BY created_at, id
                  """;
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = sql;
        cmd.Parameters.Add(CreateParameter("@parent", parentRunId));
        // Keyset pagination with take+1 lookahead: NextCursor is non-null iff
        // a strictly additional row exists beyond the page boundary, per the
        // DirectChildrenPage contract.
        cmd.Parameters.Add(CreateParameter("@take", take + 1));
        if ((after ?? before) is { } c)
        {
            cmd.Parameters.Add(CreateParameter("@cts", c.CreatedAt));
            cmd.Parameters.Add(CreateParameter("@cid", c.Id));
        }

        var items = new List<JobRun>(take + 1);
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                items.Add(ReadRun(reader));
            }
        }

        var hasMore = items.Count > take;
        if (hasMore)
        {
            items.RemoveAt(items.Count - 1);
        }

        var nextCursor = hasMore
            ? DirectChildrenPage.EncodeCursor(items[^1].CreatedAt, items[^1].Id)
            : null;

        return new() { Items = items, NextCursor = nextCursor };
    }

    public async Task<IReadOnlyList<JobRun>> GetAncestorChainAsync(string runId,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        // Parent IDs are immutable; recursion terminates when parent_run_id is null.
        // OPTION (MAXRECURSION 0) disables SQL Server's default 100-row recursion guard.
        cmd.CommandText = """
                          WITH ancestors(depth, id) AS (
                              SELECT 0, parent_run_id FROM dbo.surefire_runs WHERE id = @id
                              UNION ALL
                              SELECT a.depth + 1, r.parent_run_id
                              FROM ancestors a
                              JOIN dbo.surefire_runs r ON r.id = a.id
                              WHERE a.id IS NOT NULL
                          )
                          SELECT r.* FROM ancestors a
                          JOIN dbo.surefire_runs r ON r.id = a.id
                          WHERE a.id IS NOT NULL
                          ORDER BY a.depth DESC
                          OPTION (MAXRECURSION 0)
                          """;
        cmd.Parameters.Add(CreateParameter("@id", runId));
        var chain = new List<JobRun>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            chain.Add(ReadRun(reader));
        }

        return chain;
    }

    public async Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, int skip = 0, int take = 50,
        CancellationToken cancellationToken = default)
    {
        if (skip < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(skip));
        }

        if (take <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(take));
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        var whereParts = new List<string>();
        await using var countCmd = CreateCommand(conn);
        BuildRunFilterWhere(filter, whereParts, countCmd);
        var whereClause = whereParts.Count > 0 ? "WHERE " + string.Join(" AND ", whereParts) : "";

        countCmd.CommandText = $"SELECT COUNT(*) FROM dbo.surefire_runs {whereClause}";
        var totalCount = (int)(await countCmd.ExecuteScalarAsync(cancellationToken))!;

        if (totalCount == 0 || skip >= totalCount)
        {
            return new() { Items = [], TotalCount = totalCount };
        }

        var orderBy = filter.OrderBy switch
        {
            RunOrderBy.StartedAt => "started_at DESC, id DESC",
            RunOrderBy.CompletedAt => "completed_at DESC, id DESC",
            _ => "created_at DESC, id DESC"
        };

        await using var cmd = CreateCommand(conn);
        BuildRunFilterWhere(filter, [], cmd);
        cmd.CommandText =
            $"SELECT * FROM dbo.surefire_runs {whereClause} ORDER BY {orderBy} OFFSET @skip ROWS FETCH NEXT @take ROWS ONLY";
        cmd.Parameters.Add(CreateParameter("@take", take));
        cmd.Parameters.Add(CreateParameter("@skip", skip));

        var items = new List<JobRun>();
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                items.Add(ReadRun(reader));
            }
        }

        return new() { Items = items, TotalCount = totalCount };
    }

    public async Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          UPDATE dbo.surefire_runs SET
                              progress = @progress,
                              result = @result,
                              reason = @reason,
                              trace_id = @trace_id,
                              span_id = @span_id,
                              last_heartbeat_at = @last_heartbeat_at
                          WHERE id = @id AND (node_name = @node_name OR (node_name IS NULL AND @node_name IS NULL)) AND status NOT IN (2, 4, 5)
                          """;

        cmd.Parameters.Add(CreateParameter("@id", run.Id));
        cmd.Parameters.Add(CreateParameter("@progress", run.Progress));
        cmd.Parameters.Add(CreateParameter("@result", (object?)run.Result ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@reason", (object?)run.Reason ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@trace_id", (object?)run.TraceId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@span_id", (object?)run.SpanId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@last_heartbeat_at",
            run.LastHeartbeatAt.HasValue ? run.LastHeartbeatAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@node_name", (object?)run.NodeName ?? DBNull.Value));

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<RunTransitionResult> TryTransitionRunAsync(RunStatusTransition transition,
        CancellationToken cancellationToken = default)
    {
        if (!RunTransitionRules.IsAllowed(transition.ExpectedStatus, transition.NewStatus)
            || !transition.HasRequiredFields())
        {
            return RunTransitionResult.NotApplied;
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          UPDATE dbo.surefire_runs SET
                              status = @new_status,
                              node_name = @node_name,
                              started_at = COALESCE(@started_at, started_at),
                              completed_at = COALESCE(@completed_at, completed_at),
                              cancelled_at = COALESCE(@cancelled_at, cancelled_at),
                              reason = @reason,
                              result = @result,
                              progress = @progress,
                              not_before = @not_before,
                              last_heartbeat_at = COALESCE(@last_heartbeat_at, last_heartbeat_at)
                          WHERE id = @id
                              AND status = @expected_status
                              AND attempt = @expected_attempt
                              AND status NOT IN (2, 4, 5)
                          """;

        cmd.Parameters.Add(CreateParameter("@id", transition.RunId));
        cmd.Parameters.Add(CreateParameter("@new_status", (int)transition.NewStatus));
        cmd.Parameters.Add(CreateParameter("@node_name", (object?)transition.NodeName ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@started_at",
            transition.StartedAt.HasValue ? transition.StartedAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@completed_at",
            transition.CompletedAt.HasValue ? transition.CompletedAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@cancelled_at",
            transition.CancelledAt.HasValue ? transition.CancelledAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@reason", (object?)transition.Reason ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@result", (object?)transition.Result ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@progress", transition.Progress));
        cmd.Parameters.Add(CreateParameter("@not_before", transition.NotBefore));
        cmd.Parameters.Add(CreateParameter("@last_heartbeat_at",
            transition.LastHeartbeatAt.HasValue ? transition.LastHeartbeatAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@expected_status", (int)transition.ExpectedStatus));
        cmd.Parameters.Add(CreateParameter("@expected_attempt", transition.ExpectedAttempt));

        var updated = await cmd.ExecuteNonQueryAsync(cancellationToken) > 0;
        if (updated)
        {
            var transitionEvents = new List<RunEvent>();
            transitionEvents.Add(RunStatusEvents.Create(transition.RunId, transition.ExpectedAttempt,
                transition.NewStatus, timeProvider.GetUtcNow()));
            if (transition.Events is { Count: > 0 })
            {
                transitionEvents.AddRange(transition.Events);
            }

            await InsertEventsAsync(conn, tx, transitionEvents, cancellationToken);
        }

        BatchCompletionInfo? batchCompletion = null;
        var newStatus = transition.NewStatus;
        if (updated && (newStatus == JobStatus.Succeeded || newStatus == JobStatus.Cancelled ||
                        newStatus == JobStatus.Failed))
        {
            await using var batchIdCmd = CreateCommand(conn);
            batchIdCmd.Transaction = tx;
            batchIdCmd.CommandText = "SELECT batch_id FROM dbo.surefire_runs WHERE id = @id";
            batchIdCmd.Parameters.Add(CreateParameter("@id", transition.RunId));
            var batchIdObj = await batchIdCmd.ExecuteScalarAsync(cancellationToken);
            var batchId = batchIdObj is string s ? s : null;

            if (batchId is { })
            {
                await using var incrCmd = CreateCommand(conn);
                incrCmd.Transaction = tx;
                incrCmd.CommandText = """
                                      UPDATE dbo.surefire_batches
                                      SET succeeded = succeeded + CASE WHEN @status = 2 THEN 1 ELSE 0 END,
                                          failed    = failed    + CASE WHEN @status = 5 THEN 1 ELSE 0 END,
                                          cancelled = cancelled + CASE WHEN @status = 4 THEN 1 ELSE 0 END
                                      OUTPUT INSERTED.total, INSERTED.succeeded, INSERTED.failed, INSERTED.cancelled
                                      WHERE id = @id AND status NOT IN (2, 4, 5)
                                      """;
                incrCmd.Parameters.Add(CreateParameter("@id", batchId));
                incrCmd.Parameters.Add(CreateParameter("@status", (int)newStatus));
                await using var reader = await incrCmd.ExecuteReaderAsync(cancellationToken);

                if (await reader.ReadAsync(cancellationToken))
                {
                    var total = reader.GetInt32(0);
                    var succeeded = reader.GetInt32(1);
                    var failed = reader.GetInt32(2);
                    var cancelled = reader.GetInt32(3);

                    if (succeeded + failed + cancelled >= total)
                    {
                        var batchStatus = failed > 0 ? JobStatus.Failed
                            : cancelled > 0 ? JobStatus.Cancelled
                            : JobStatus.Succeeded;
                        var completedAt = timeProvider.GetUtcNow();

                        await reader.CloseAsync();

                        await using var completeCmd = CreateCommand(conn);
                        completeCmd.Transaction = tx;
                        completeCmd.CommandText = """
                                                  UPDATE dbo.surefire_batches
                                                  SET status = @status, completed_at = @completed_at
                                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                                  """;
                        completeCmd.Parameters.Add(CreateParameter("@id", batchId));
                        completeCmd.Parameters.Add(CreateParameter("@status", (short)batchStatus));
                        completeCmd.Parameters.Add(CreateParameter("@completed_at", completedAt));
                        await completeCmd.ExecuteNonQueryAsync(cancellationToken);

                        batchCompletion = new(batchId, batchStatus, completedAt);
                    }
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return new(updated, batchCompletion);
    }

    public async Task<RunTransitionResult> TryCancelRunAsync(string runId,
        int? expectedAttempt = null,
        string? reason = null,
        IReadOnlyList<RunEvent>? events = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          DECLARE @now DATETIMEOFFSET(7) = TODATETIMEOFFSET(SYSUTCDATETIME(), 0);

                          DECLARE @updated TABLE (
                              id NVARCHAR(450) NOT NULL,
                              attempt INT NOT NULL,
                              batch_id NVARCHAR(450)
                          );

                          UPDATE dbo.surefire_runs SET
                              status = 4,
                              cancelled_at = @now,
                              completed_at = @now,
                              reason = COALESCE(@reason, reason)
                          OUTPUT INSERTED.id, INSERTED.attempt, INSERTED.batch_id INTO @updated(id, attempt, batch_id)
                          WHERE id = @id AND status NOT IN (2, 4, 5)
                              AND (@expected_attempt IS NULL OR attempt = @expected_attempt);

                          SELECT id, attempt, batch_id FROM @updated;
                          """;

        cmd.Parameters.Add(CreateParameter("@id", runId));
        cmd.Parameters.Add(CreateParameter("@reason", (object?)reason ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@expected_attempt",
            expectedAttempt.HasValue ? expectedAttempt.Value : DBNull.Value));

        int? attempt = null;
        string? batchId = null;
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                attempt = reader.GetInt32(1);
                batchId = reader.IsDBNull(2) ? null : reader.GetString(2);
            }
        }

        if (attempt is null)
        {
            await tx.CommitAsync(cancellationToken);
            return RunTransitionResult.NotApplied;
        }

        var allEvents = new List<RunEvent>();
        allEvents.Add(RunStatusEvents.Create(runId, attempt.Value, JobStatus.Cancelled, timeProvider.GetUtcNow()));
        if (events is { Count: > 0 })
        {
            allEvents.AddRange(events);
        }

        await InsertEventsAsync(conn, tx, allEvents, cancellationToken);

        BatchCompletionInfo? batchCompletion = null;
        if (batchId is { })
        {
            await using var incrCmd = CreateCommand(conn);
            incrCmd.Transaction = tx;
            incrCmd.CommandText = """
                                  UPDATE dbo.surefire_batches
                                  SET cancelled = cancelled + 1
                                  OUTPUT INSERTED.total, INSERTED.succeeded, INSERTED.failed, INSERTED.cancelled
                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                  """;
            incrCmd.Parameters.Add(CreateParameter("@id", batchId));
            await using var batchReader = await incrCmd.ExecuteReaderAsync(cancellationToken);

            if (await batchReader.ReadAsync(cancellationToken))
            {
                var total = batchReader.GetInt32(0);
                var succeeded = batchReader.GetInt32(1);
                var failed = batchReader.GetInt32(2);
                var cancelled = batchReader.GetInt32(3);

                if (succeeded + failed + cancelled >= total)
                {
                    var batchStatus = failed > 0 ? JobStatus.Failed
                        : cancelled > 0 ? JobStatus.Cancelled
                        : JobStatus.Succeeded;
                    var completedAt = timeProvider.GetUtcNow();

                    await batchReader.CloseAsync();

                    await using var completeCmd = CreateCommand(conn);
                    completeCmd.Transaction = tx;
                    completeCmd.CommandText = """
                                              UPDATE dbo.surefire_batches
                                              SET status = @status, completed_at = @completed_at
                                              WHERE id = @id AND status NOT IN (2, 4, 5)
                                              """;
                    completeCmd.Parameters.Add(CreateParameter("@id", batchId));
                    completeCmd.Parameters.Add(CreateParameter("@status", (short)batchStatus));
                    completeCmd.Parameters.Add(CreateParameter("@completed_at", completedAt));
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);

                    batchCompletion = new(batchId, batchStatus, completedAt);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return new(true, batchCompletion);
    }

    public Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, CancellationToken cancellationToken = default)
    {
        if (jobNames.Count == 0 || queueNames.Count == 0)
        {
            return Task.FromResult<JobRun?>(null);
        }

        return ClaimRunCoreAsync(nodeName, jobNames, queueNames, cancellationToken);
    }


    public async Task CreateBatchAsync(JobBatch batch, IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);

        await using var batchCmd = CreateCommand(conn);
        batchCmd.Transaction = tx;
        batchCmd.CommandText = """
                               INSERT INTO dbo.surefire_batches (id, status, total, succeeded, failed, cancelled, created_at, completed_at)
                               VALUES (@id, @status, @total, @succeeded, @failed, @cancelled, @created_at, @completed_at)
                               """;
        batchCmd.Parameters.Add(CreateParameter("@id", batch.Id));
        batchCmd.Parameters.Add(CreateParameter("@status", (short)batch.Status));
        batchCmd.Parameters.Add(CreateParameter("@total", batch.Total));
        batchCmd.Parameters.Add(CreateParameter("@succeeded", batch.Succeeded));
        batchCmd.Parameters.Add(CreateParameter("@failed", batch.Failed));
        batchCmd.Parameters.Add(CreateParameter("@cancelled", batch.Cancelled));
        batchCmd.Parameters.Add(CreateParameter("@created_at", batch.CreatedAt));
        batchCmd.Parameters.Add(CreateParameter("@completed_at",
            batch.CompletedAt.HasValue ? batch.CompletedAt.Value : DBNull.Value));
        await batchCmd.ExecuteNonQueryAsync(cancellationToken);

        const int paramsPerRun = 26;
        const int maxParams = 2100;
        var chunkSize = maxParams / paramsPerRun;

        for (var offset = 0; offset < runs.Count; offset += chunkSize)
        {
            var chunk = runs.Skip(offset).Take(chunkSize).ToList();
            var sb = new StringBuilder();
            sb.Append("""
                      INSERT INTO dbo.surefire_runs (
                          id, job_name, status, arguments, result, reason, progress,
                          created_at, started_at, completed_at, cancelled_at, node_name,
                          attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                          rerun_of_run_id, not_before, not_after, priority, deduplication_id,
                          last_heartbeat_at, batch_id, queue_priority
                      ) VALUES
                      """);

            await using var runCmd = CreateCommand(conn);
            runCmd.Transaction = tx;

            for (var i = 0; i < chunk.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(',');
                }

                var p = $"@p{i}_";
                sb.Append($"""
                           (
                               {p}id, {p}job_name, {p}status, {p}arguments, {p}result, {p}reason, {p}progress,
                               {p}created_at, {p}started_at, {p}completed_at, {p}cancelled_at, {p}node_name,
                               {p}attempt, {p}trace_id, {p}span_id, {p}parent_trace_id, {p}parent_span_id, {p}parent_run_id, {p}root_run_id,
                               {p}rerun_of_run_id, {p}not_before, {p}not_after, {p}priority, {p}deduplication_id,
                               {p}last_heartbeat_at, {p}batch_id,
                               ISNULL((SELECT q.priority FROM dbo.surefire_queues q WHERE q.name = ISNULL((SELECT j.queue FROM dbo.surefire_jobs j WHERE j.name = {p}job_name), 'default')), 0)
                           )
                           """);
                AddRunParams(runCmd, $"@p{i}_", chunk[i]);
            }

            runCmd.CommandText = sb.ToString();
            await runCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await InsertEventsAsync(conn, tx, initialEvents, cancellationToken);
        await tx.CommitAsync(cancellationToken);
    }

    public async Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM dbo.surefire_batches WHERE id = @id";
        cmd.Parameters.Add(CreateParameter("@id", batchId));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return ReadBatch(reader);
    }

    public async Task<bool> TryCompleteBatchAsync(string batchId, JobStatus status, DateTimeOffset completedAt,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          UPDATE dbo.surefire_batches
                          SET status = @status, completed_at = @completed_at
                          OUTPUT INSERTED.id
                          WHERE id = @id AND status NOT IN (2, 4, 5)
                          """;
        cmd.Parameters.Add(CreateParameter("@id", batchId));
        cmd.Parameters.Add(CreateParameter("@status", (short)status));
        cmd.Parameters.Add(CreateParameter("@completed_at", completedAt));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> CancelBatchRunsAsync(string batchId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          DECLARE @now DATETIMEOFFSET(7) = TODATETIMEOFFSET(SYSUTCDATETIME(), 0);
                          DECLARE @cancelled TABLE (id NVARCHAR(450) NOT NULL, attempt INT NOT NULL);

                          UPDATE dbo.surefire_runs SET
                              status = 4,
                              cancelled_at = @now,
                              completed_at = @now,
                              reason = COALESCE(@reason, reason)
                          OUTPUT INSERTED.id, INSERTED.attempt INTO @cancelled(id, attempt)
                          WHERE batch_id = @batch_id AND status IN (0, 1);

                          SELECT id, attempt FROM @cancelled;
                          """;
        cmd.Parameters.Add(CreateParameter("@batch_id", batchId));
        cmd.Parameters.Add(CreateParameter("@reason", (object?)reason ?? DBNull.Value));

        var cancelledIds = new List<string>();
        var statusEvents = new List<RunEvent>();
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var runId = reader.GetString(0);
                cancelledIds.Add(runId);
                statusEvents.Add(RunStatusEvents.Create(
                    runId,
                    reader.GetInt32(1),
                    JobStatus.Cancelled,
                    timeProvider.GetUtcNow()));
            }
        }

        await InsertEventsAsync(conn, tx, statusEvents, cancellationToken);

        if (cancelledIds.Count > 0)
        {
            await using var incrCmd = CreateCommand(conn);
            incrCmd.Transaction = tx;
            incrCmd.CommandText = """
                                  UPDATE dbo.surefire_batches
                                  SET cancelled = cancelled + @cnt
                                  OUTPUT INSERTED.total, INSERTED.succeeded, INSERTED.failed, INSERTED.cancelled
                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                  """;
            incrCmd.Parameters.Add(CreateParameter("@id", batchId));
            incrCmd.Parameters.Add(CreateParameter("@cnt", cancelledIds.Count));
            await using var batchReader = await incrCmd.ExecuteReaderAsync(cancellationToken);

            if (await batchReader.ReadAsync(cancellationToken))
            {
                var total = batchReader.GetInt32(0);
                var succeeded = batchReader.GetInt32(1);
                var failed = batchReader.GetInt32(2);
                var cancelled = batchReader.GetInt32(3);

                if (succeeded + failed + cancelled >= total)
                {
                    var batchStatus = failed > 0 ? JobStatus.Failed
                        : cancelled > 0 ? JobStatus.Cancelled
                        : JobStatus.Succeeded;
                    var completedAt = timeProvider.GetUtcNow();

                    await batchReader.CloseAsync();

                    await using var completeCmd = CreateCommand(conn);
                    completeCmd.Transaction = tx;
                    completeCmd.CommandText = """
                                              UPDATE dbo.surefire_batches
                                              SET status = @status, completed_at = @completed_at
                                              WHERE id = @id AND status NOT IN (2, 4, 5)
                                              """;
                    completeCmd.Parameters.Add(CreateParameter("@id", batchId));
                    completeCmd.Parameters.Add(CreateParameter("@status", (short)batchStatus));
                    completeCmd.Parameters.Add(CreateParameter("@completed_at", completedAt));
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return cancelledIds;
    }

    public async Task<IReadOnlyList<string>> CancelChildRunsAsync(string parentRunId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          DECLARE @now DATETIMEOFFSET(7) = TODATETIMEOFFSET(SYSUTCDATETIME(), 0);
                          DECLARE @cancelled TABLE (id NVARCHAR(450) NOT NULL, attempt INT NOT NULL, batch_id NVARCHAR(450));

                          UPDATE dbo.surefire_runs SET
                              status = 4,
                              cancelled_at = @now,
                              completed_at = @now,
                              reason = COALESCE(@reason, reason)
                          OUTPUT INSERTED.id, INSERTED.attempt, INSERTED.batch_id INTO @cancelled(id, attempt, batch_id)
                          WHERE parent_run_id = @parent_run_id AND status IN (0, 1);

                          SELECT id, attempt, batch_id FROM @cancelled;
                          """;
        cmd.Parameters.Add(CreateParameter("@parent_run_id", parentRunId));
        cmd.Parameters.Add(CreateParameter("@reason", (object?)reason ?? DBNull.Value));

        var cancelledIds = new List<string>();
        var cancelledBatchIds = new Dictionary<string, int>();
        var statusEvents = new List<RunEvent>();
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var runId = reader.GetString(0);
                cancelledIds.Add(runId);
                statusEvents.Add(RunStatusEvents.Create(
                    runId,
                    reader.GetInt32(1),
                    JobStatus.Cancelled,
                    timeProvider.GetUtcNow()));
                var bId = reader.IsDBNull(2) ? null : reader.GetString(2);
                if (bId is { })
                {
                    cancelledBatchIds[bId] = cancelledBatchIds.GetValueOrDefault(bId) + 1;
                }
            }
        }

        await InsertEventsAsync(conn, tx, statusEvents, cancellationToken);

        foreach (var (batchId, cnt) in cancelledBatchIds)
        {
            await using var incrCmd = CreateCommand(conn);
            incrCmd.Transaction = tx;
            incrCmd.CommandText = """
                                  UPDATE dbo.surefire_batches
                                  SET cancelled = cancelled + @cnt
                                  OUTPUT INSERTED.total, INSERTED.succeeded, INSERTED.failed, INSERTED.cancelled
                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                  """;
            incrCmd.Parameters.Add(CreateParameter("@id", batchId));
            incrCmd.Parameters.Add(CreateParameter("@cnt", cnt));
            await using var batchReader = await incrCmd.ExecuteReaderAsync(cancellationToken);

            if (await batchReader.ReadAsync(cancellationToken))
            {
                var total = batchReader.GetInt32(0);
                var succeeded = batchReader.GetInt32(1);
                var failed = batchReader.GetInt32(2);
                var cancelled = batchReader.GetInt32(3);

                if (succeeded + failed + cancelled >= total)
                {
                    var batchStatus = failed > 0 ? JobStatus.Failed
                        : cancelled > 0 ? JobStatus.Cancelled
                        : JobStatus.Succeeded;
                    var completedAt = timeProvider.GetUtcNow();

                    await batchReader.CloseAsync();

                    await using var completeCmd = CreateCommand(conn);
                    completeCmd.Transaction = tx;
                    completeCmd.CommandText = """
                                              UPDATE dbo.surefire_batches
                                              SET status = @status, completed_at = @completed_at
                                              WHERE id = @id AND status NOT IN (2, 4, 5)
                                              """;
                    completeCmd.Parameters.Add(CreateParameter("@id", batchId));
                    completeCmd.Parameters.Add(CreateParameter("@status", (short)batchStatus));
                    completeCmd.Parameters.Add(CreateParameter("@completed_at", completedAt));
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return cancelledIds;
    }

    public async Task<IReadOnlyList<string>> GetCompletableBatchIdsAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SELECT b.id FROM dbo.surefire_batches b
                          WHERE b.status NOT IN (2, 4, 5)
                          AND NOT EXISTS (
                              SELECT 1 FROM dbo.surefire_runs r
                              WHERE r.batch_id = b.id AND r.status NOT IN (2, 4, 5)
                          )
                          """;
        var result = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(reader.GetString(0));
        }

        return result;
    }

    public async Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default)
    {
        if (events.Count == 0)
        {
            return;
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);

        await InsertEventsAsync(conn, tx, events, cancellationToken);

        await tx.CommitAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0,
        RunEventType[]? types = null, int? attempt = null, int? take = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);

        var sb = new StringBuilder("SELECT * FROM dbo.surefire_events WHERE run_id = @run_id AND id > @since_id");
        cmd.Parameters.Add(CreateParameter("@run_id", runId));
        cmd.Parameters.Add(CreateParameter("@since_id", sinceId));

        if (types is { Length: > 0 })
        {
            var typeParams = types.Select((t, i) =>
            {
                var name = $"@et{i}";
                cmd.Parameters.Add(CreateParameter(name, (short)t));
                return name;
            }).ToList();
            sb.Append($" AND event_type IN ({string.Join(",", typeParams)})");
        }

        if (attempt is { })
        {
            sb.Append(" AND (attempt = @attempt OR attempt = 0)");
            cmd.Parameters.Add(CreateParameter("@attempt", attempt.Value));
        }

        sb.Append(" ORDER BY id");

        if (take is { })
        {
            sb.Append(" OFFSET 0 ROWS FETCH NEXT @take ROWS ONLY");
            cmd.Parameters.Add(CreateParameter("@take", take.Value));
        }

        cmd.CommandText = sb.ToString();

        var results = new List<RunEvent>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadEvent(reader));
        }

        return results;
    }

    public async Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchId, long sinceEventId = 0,
        int take = 200, CancellationToken cancellationToken = default)
    {
        if (take <= 0)
        {
            return [];
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SELECT TOP (@take) e.*
                          FROM dbo.surefire_events e
                          INNER JOIN dbo.surefire_runs r ON r.id = e.run_id
                          WHERE r.batch_id = @batch_id
                              AND e.event_type = @event_type
                              AND e.id > @since_event_id
                          ORDER BY e.id ASC
                          """;
        cmd.Parameters.Add(CreateParameter("@batch_id", batchId));
        cmd.Parameters.Add(CreateParameter("@event_type", (short)RunEventType.Output));
        cmd.Parameters.Add(CreateParameter("@since_event_id", sinceEventId));
        cmd.Parameters.Add(CreateParameter("@take", take));

        var results = new List<RunEvent>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadEvent(reader));
        }

        return results;
    }

    public async Task<IReadOnlyList<RunEvent>> GetBatchEventsAsync(string batchId, long sinceEventId = 0,
        int take = 200, CancellationToken cancellationToken = default)
    {
        if (take <= 0)
        {
            return [];
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SELECT TOP (@take) e.*
                          FROM dbo.surefire_events e
                          INNER JOIN dbo.surefire_runs r ON r.id = e.run_id
                          WHERE r.batch_id = @batch_id
                              AND e.id > @since_event_id
                          ORDER BY e.id ASC
                          """;
        cmd.Parameters.Add(CreateParameter("@batch_id", batchId));
        cmd.Parameters.Add(CreateParameter("@since_event_id", sinceEventId));
        cmd.Parameters.Add(CreateParameter("@take", take));

        var results = new List<RunEvent>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadEvent(reader));
        }

        return results;
    }

    public async Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, IReadOnlyCollection<string> activeRunIds,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);

        await using var nodeCmd = CreateCommand(conn);
        nodeCmd.Transaction = tx;
        nodeCmd.CommandText = """
                              MERGE dbo.surefire_nodes WITH (HOLDLOCK) AS target
                              USING (SELECT @name AS name) AS source ON target.name = source.name
                              WHEN MATCHED THEN UPDATE SET
                                  last_heartbeat_at = SYSUTCDATETIME(),
                                  running_count = @running_count,
                                  registered_job_names = @job_names,
                                  registered_queue_names = @queue_names
                              WHEN NOT MATCHED THEN INSERT (name, started_at, last_heartbeat_at, running_count, registered_job_names, registered_queue_names)
                                  VALUES (@name, SYSUTCDATETIME(), SYSUTCDATETIME(), @running_count, @job_names, @queue_names);
                              """;
        nodeCmd.Parameters.Add(CreateParameter("@name", nodeName));
        nodeCmd.Parameters.Add(CreateParameter("@running_count", activeRunIds.Count));
        nodeCmd.Parameters.Add(CreateParameter("@job_names", JsonSerializer.Serialize(jobNames)));
        nodeCmd.Parameters.Add(CreateParameter("@queue_names", JsonSerializer.Serialize(queueNames)));
        await nodeCmd.ExecuteNonQueryAsync(cancellationToken);

        if (activeRunIds.Count > 0)
        {
            const int maxRunIdsPerBatch = 2000;
            var activeRunIdsList = activeRunIds as IList<string> ?? activeRunIds.ToList();

            foreach (var chunk in activeRunIdsList.Chunk(maxRunIdsPerBatch))
            {
                await using var runCmd = CreateCommand(conn);
                runCmd.Transaction = tx;
                var idList = string.Join(", ", chunk.Select((_, i) => $"@rid_{i}"));
                runCmd.CommandText = $"""
                                      UPDATE dbo.surefire_runs SET last_heartbeat_at = SYSUTCDATETIME()
                                      WHERE id IN ({idList}) AND node_name = @node AND status NOT IN (2, 4, 5)
                                      """;
                runCmd.Parameters.Add(CreateParameter("@node", nodeName));
                for (var i = 0; i < chunk.Length; i++)
                {
                    runCmd.Parameters.Add(CreateParameter($"@rid_{i}", chunk[i]));
                }

                await runCmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        await tx.CommitAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> GetExternallyStoppedRunIdsAsync(IReadOnlyCollection<string> runIds,
        CancellationToken cancellationToken = default)
    {
        if (runIds.Count == 0)
        {
            return [];
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);

        var idList = new List<string>();
        var idsArr = runIds as IList<string> ?? runIds.ToList();
        for (var i = 0; i < idsArr.Count; i++)
        {
            var p = $"@rid_{i}";
            idList.Add(p);
            cmd.Parameters.Add(CreateParameter(p, idsArr[i]));
        }

        var inClause = string.Join(", ", idList);
        // Returns input IDs that no longer correspond to a Running row, including IDs that were
        // deleted entirely.
        cmd.CommandText = $"""
                           SELECT input_id
                           FROM (VALUES {string.Join(", ", idList.Select(p => $"({p})"))}) AS i(input_id)
                           LEFT JOIN dbo.surefire_runs r ON r.id = i.input_id
                           WHERE r.id IS NULL OR r.status <> 1
                           """;

        var results = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(reader.GetString(0));
        }

        return results;
    }

    public async Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM dbo.surefire_nodes";

        var results = new List<NodeInfo>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadNode(reader));
        }

        return results;
    }

    public async Task<NodeInfo?> GetNodeAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT TOP (1) * FROM dbo.surefire_nodes WHERE name = @name";
        cmd.Parameters.Add(CreateParameter("@name", name));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return ReadNode(reader);
        }

        return null;
    }

    public async Task UpsertQueueAsync(QueueDefinition queue, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          MERGE dbo.surefire_queues WITH (HOLDLOCK) AS target
                          USING (SELECT @name AS name) AS source ON target.name = source.name
                          WHEN MATCHED THEN UPDATE SET
                              priority = @priority,
                              max_concurrency = @max_concurrency,
                              rate_limit_name = @rate_limit_name,
                              last_heartbeat_at = SYSUTCDATETIME()
                          WHEN NOT MATCHED THEN INSERT (name, priority, max_concurrency, rate_limit_name, last_heartbeat_at)
                              VALUES (@name, @priority, @max_concurrency, @rate_limit_name, SYSUTCDATETIME());

                          UPDATE r SET r.queue_priority = @priority
                          FROM dbo.surefire_runs r
                          JOIN dbo.surefire_jobs j ON j.name = r.job_name
                          WHERE ISNULL(j.queue, 'default') = @name AND r.status = 0
                              AND r.queue_priority != @priority;
                          """;

        cmd.Parameters.Add(CreateParameter("@name", queue.Name));
        cmd.Parameters.Add(CreateParameter("@priority", queue.Priority));
        cmd.Parameters.Add(CreateParameter("@max_concurrency",
            queue.MaxConcurrency.HasValue ? queue.MaxConcurrency.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@rate_limit_name", (object?)queue.RateLimitName ?? DBNull.Value));

        await cmd.ExecuteNonQueryAsync(cancellationToken);
        await tx.CommitAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM dbo.surefire_queues";

        var results = new List<QueueDefinition>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadQueue(reader));
        }

        return results;
    }

    public async Task SetQueuePausedAsync(string name, bool isPaused, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          MERGE dbo.surefire_queues WITH (HOLDLOCK) AS target
                          USING (SELECT @name AS name) AS source ON target.name = source.name
                          WHEN MATCHED THEN UPDATE SET is_paused = @is_paused
                          WHEN NOT MATCHED THEN INSERT (name, priority, max_concurrency, is_paused, rate_limit_name, last_heartbeat_at)
                              VALUES (@name, 0, NULL, @is_paused, NULL, NULL);
                          """;
        cmd.Parameters.Add(CreateParameter("@name", name));
        cmd.Parameters.Add(CreateParameter("@is_paused", isPaused));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          MERGE dbo.surefire_rate_limits WITH (HOLDLOCK) AS target
                          USING (SELECT @name AS name) AS source ON target.name = source.name
                          WHEN MATCHED THEN UPDATE SET
                              type = @type,
                              max_permits = @max_permits,
                              [window] = @window,
                              last_heartbeat_at = SYSUTCDATETIME()
                          WHEN NOT MATCHED THEN INSERT (name, type, max_permits, [window], last_heartbeat_at)
                              VALUES (@name, @type, @max_permits, @window, SYSUTCDATETIME());
                          """;

        cmd.Parameters.Add(CreateParameter("@name", rateLimit.Name));
        cmd.Parameters.Add(CreateParameter("@type", (int)rateLimit.Type));
        cmd.Parameters.Add(CreateParameter("@max_permits", rateLimit.MaxPermits));
        cmd.Parameters.Add(CreateParameter("@window", rateLimit.Window.Ticks));

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> CancelExpiredRunsWithIdsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          DECLARE @now DATETIMEOFFSET(7) = TODATETIMEOFFSET(SYSUTCDATETIME(), 0);
                          DECLARE @cancelled TABLE (id NVARCHAR(450) NOT NULL, attempt INT NOT NULL, batch_id NVARCHAR(450));

                          UPDATE dbo.surefire_runs SET
                              status = 4,
                              cancelled_at = @now,
                              completed_at = @now,
                              reason = @reason
                          OUTPUT inserted.id, inserted.attempt, inserted.batch_id INTO @cancelled(id, attempt, batch_id)
                          WHERE status = 0
                              AND not_after IS NOT NULL
                              AND not_after < @now;

                          SELECT id, attempt, batch_id FROM @cancelled;
                          """;
        cmd.Parameters.Add(CreateParameter("@reason", "Run expired past NotAfter deadline."));

        var cancelledIds = new List<string>();
        var cancelledBatchIds = new Dictionary<string, int>();
        var statusEvents = new List<RunEvent>();
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var runId = reader.GetString(0);
                cancelledIds.Add(runId);
                statusEvents.Add(RunStatusEvents.Create(runId, reader.GetInt32(1), JobStatus.Cancelled,
                    timeProvider.GetUtcNow()));
                var bId = reader.IsDBNull(2) ? null : reader.GetString(2);
                if (bId is { })
                {
                    cancelledBatchIds[bId] = cancelledBatchIds.GetValueOrDefault(bId) + 1;
                }
            }
        }

        await InsertEventsAsync(conn, tx, statusEvents, cancellationToken);

        foreach (var (batchId, cnt) in cancelledBatchIds)
        {
            await using var incrCmd = CreateCommand(conn);
            incrCmd.Transaction = tx;
            incrCmd.CommandText = """
                                  UPDATE dbo.surefire_batches
                                  SET cancelled = cancelled + @cnt
                                  OUTPUT INSERTED.total, INSERTED.succeeded, INSERTED.failed, INSERTED.cancelled
                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                  """;
            incrCmd.Parameters.Add(CreateParameter("@id", batchId));
            incrCmd.Parameters.Add(CreateParameter("@cnt", cnt));
            await using var batchReader = await incrCmd.ExecuteReaderAsync(cancellationToken);

            if (await batchReader.ReadAsync(cancellationToken))
            {
                var total = batchReader.GetInt32(0);
                var succeeded = batchReader.GetInt32(1);
                var failed = batchReader.GetInt32(2);
                var cancelled = batchReader.GetInt32(3);

                if (succeeded + failed + cancelled >= total)
                {
                    var batchStatus = failed > 0 ? JobStatus.Failed
                        : cancelled > 0 ? JobStatus.Cancelled
                        : JobStatus.Succeeded;
                    var completedAt = timeProvider.GetUtcNow();

                    await batchReader.CloseAsync();

                    await using var completeCmd = CreateCommand(conn);
                    completeCmd.Transaction = tx;
                    completeCmd.CommandText = """
                                              UPDATE dbo.surefire_batches
                                              SET status = @status, completed_at = @completed_at
                                              WHERE id = @id AND status NOT IN (2, 4, 5)
                                              """;
                    completeCmd.Parameters.Add(CreateParameter("@id", batchId));
                    completeCmd.Parameters.Add(CreateParameter("@status", (short)batchStatus));
                    completeCmd.Parameters.Add(CreateParameter("@completed_at", completedAt));
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return cancelledIds;
    }

    public async Task PurgeAsync(DateTimeOffset threshold, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        while (true)
        {
            await using var runCmd = CreateCommand(conn);
            runCmd.CommandText = """
                                 DELETE TOP (1000) FROM dbo.surefire_runs
                                 WHERE (status IN (2, 4, 5)
                                     AND completed_at < @threshold
                                     AND (batch_id IS NULL OR EXISTS (
                                         SELECT 1 FROM dbo.surefire_batches b
                                         WHERE b.id = dbo.surefire_runs.batch_id
                                             AND b.status IN (2, 4, 5)
                                             AND b.completed_at IS NOT NULL
                                             AND b.completed_at < @threshold
                                     )))
                                     OR (status = 0 AND not_before < @threshold)
                                 """;
            runCmd.Parameters.Add(CreateParameter("@threshold", threshold));
            if (await runCmd.ExecuteNonQueryAsync(cancellationToken) == 0)
            {
                break;
            }
        }

        await using var jobCmd = CreateCommand(conn);
        jobCmd.CommandText = """
                             DELETE FROM dbo.surefire_jobs
                             WHERE last_heartbeat_at < @threshold
                                 AND NOT EXISTS (SELECT 1 FROM dbo.surefire_runs r WHERE r.job_name = dbo.surefire_jobs.name AND r.status NOT IN (2, 4, 5))
                             """;
        jobCmd.Parameters.Add(CreateParameter("@threshold", threshold));
        await jobCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var queueCmd = CreateCommand(conn);
        queueCmd.CommandText = "DELETE FROM dbo.surefire_queues WHERE last_heartbeat_at < @threshold";
        queueCmd.Parameters.Add(CreateParameter("@threshold", threshold));
        await queueCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var rlCmd = CreateCommand(conn);
        rlCmd.CommandText = "DELETE FROM dbo.surefire_rate_limits WHERE last_heartbeat_at < @threshold";
        rlCmd.Parameters.Add(CreateParameter("@threshold", threshold));
        await rlCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var batchCmd = CreateCommand(conn);
        batchCmd.CommandText =
            "DELETE FROM dbo.surefire_batches WHERE status IN (2, 4, 5) AND completed_at IS NOT NULL AND completed_at < @threshold";
        batchCmd.Parameters.Add(CreateParameter("@threshold", threshold));
        await batchCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var nodeCmd = CreateCommand(conn);
        nodeCmd.CommandText = "DELETE FROM dbo.surefire_nodes WHERE last_heartbeat_at < @threshold";
        nodeCmd.Parameters.Add(CreateParameter("@threshold", threshold));
        await nodeCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);

        var now = timeProvider.GetUtcNow();
        if (bucketMinutes <= 0)
        {
            bucketMinutes = 60;
        }

        var rawSince = since ?? now.AddHours(-24);
        var sinceTime = new DateTimeOffset(rawSince.Ticks / TimeSpan.TicksPerMinute * TimeSpan.TicksPerMinute,
            rawSince.Offset);

        await using var statsCmd = CreateCommand(conn);
        statsCmd.CommandText = """
                               SELECT
                                   (SELECT COUNT(*) FROM dbo.surefire_jobs) AS total_jobs,
                                   (SELECT COUNT(*) FROM dbo.surefire_nodes WHERE last_heartbeat_at >= DATEADD(MINUTE, -2, @now)) AS node_count,
                                   COUNT(*) AS total_runs,
                                   ISNULL(SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END), 0) AS pending,
                                   ISNULL(SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END), 0) AS running,
                                   ISNULL(SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END), 0) AS succeeded,
                                   ISNULL(SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END), 0) AS cancelled,
                                   ISNULL(SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END), 0) AS failed
                               FROM dbo.surefire_runs
                               WHERE created_at >= @since AND created_at <= @now
                               """;
        statsCmd.Parameters.Add(CreateParameter("@now", now));
        statsCmd.Parameters.Add(CreateParameter("@since", sinceTime));

        int totalJobs = 0, totalRuns = 0, activeRuns = 0, nodeCount = 0;
        var runsByStatus = new Dictionary<string, int>();
        double successRate = 0;
        await using (var reader = await statsCmd.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                totalJobs = reader.GetInt32(reader.GetOrdinal("total_jobs"));
                nodeCount = reader.GetInt32(reader.GetOrdinal("node_count"));
                totalRuns = reader.GetInt32(reader.GetOrdinal("total_runs"));

                var pending = reader.GetInt32(reader.GetOrdinal("pending"));
                var running = reader.GetInt32(reader.GetOrdinal("running"));
                var succeeded = reader.GetInt32(reader.GetOrdinal("succeeded"));
                var cancelled = reader.GetInt32(reader.GetOrdinal("cancelled"));
                var failed = reader.GetInt32(reader.GetOrdinal("failed"));

                activeRuns = pending + running;

                if (pending > 0)
                {
                    runsByStatus["Pending"] = pending;
                }

                if (running > 0)
                {
                    runsByStatus["Running"] = running;
                }

                if (succeeded > 0)
                {
                    runsByStatus["Succeeded"] = succeeded;
                }

                if (cancelled > 0)
                {
                    runsByStatus["Cancelled"] = cancelled;
                }

                if (failed > 0)
                {
                    runsByStatus["Failed"] = failed;
                }

                var terminalCount = succeeded + cancelled + failed;
                successRate = terminalCount > 0 ? succeeded / (double)terminalCount : 0.0;
            }
        }

        await using var bucketCmd = CreateCommand(conn);
        bucketCmd.CommandText = """
                                 SELECT
                                     DATEADD(MINUTE, (DATEDIFF(MINUTE, @since, created_at) / @bucket_minutes) * @bucket_minutes, @since) AS bucket_start,
                                     SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS pending,
                                     SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS running,
                                     SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS succeeded,
                                     SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END) AS cancelled,
                                     SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END) AS failed
                                FROM dbo.surefire_runs
                                WHERE created_at >= @since AND created_at <= @now
                                GROUP BY DATEADD(MINUTE, (DATEDIFF(MINUTE, @since, created_at) / @bucket_minutes) * @bucket_minutes, @since)
                                ORDER BY bucket_start
                                """;
        bucketCmd.Parameters.Add(CreateParameter("@since", sinceTime));
        bucketCmd.Parameters.Add(CreateParameter("@bucket_minutes", bucketMinutes));
        bucketCmd.Parameters.Add(CreateParameter("@now", now));

        var bucketMap = new Dictionary<DateTimeOffset, TimelineBucket>();
        await using (var reader = await bucketCmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var start = reader.GetDateTimeOffset(0);
                bucketMap[start] = new()
                {
                    Start = start,
                    Pending = reader.GetInt32(1),
                    Running = reader.GetInt32(2),
                    Succeeded = reader.GetInt32(3),
                    Cancelled = reader.GetInt32(4),
                    Failed = reader.GetInt32(5)
                };
            }
        }

        var buckets = new List<TimelineBucket>();
        var bucketStart = sinceTime;
        var bucketSpan = TimeSpan.FromMinutes(bucketMinutes);
        while (bucketStart <= now)
        {
            if (bucketMap.TryGetValue(bucketStart, out var bucket))
            {
                buckets.Add(bucket);
            }
            else
            {
                buckets.Add(new() { Start = bucketStart });
            }

            bucketStart += bucketSpan;
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

    public async Task<JobStats> GetJobStatsAsync(string jobName, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SELECT
                              COUNT(*) AS total_runs,
                              SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS succeeded,
                              SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END) AS failed,
                              CASE
                                  WHEN SUM(CASE WHEN status IN (2, 4, 5) THEN 1 ELSE 0 END) > 0
                                  THEN CAST(SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS FLOAT) / SUM(CASE WHEN status IN (2, 4, 5) THEN 1 ELSE 0 END)
                                  ELSE 0
                              END AS success_rate,
                              AVG(CASE WHEN status = 2 AND started_at IS NOT NULL AND completed_at IS NOT NULL
                                  THEN CAST(DATEDIFF_BIG(MICROSECOND, started_at, completed_at) AS FLOAT) / 1000000.0
                                  ELSE NULL END) AS avg_duration_secs,
                              MAX(started_at) AS last_run_at
                          FROM dbo.surefire_runs WHERE job_name = @job_name
                          """;
        cmd.Parameters.Add(CreateParameter("@job_name", jobName));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);

        return new()
        {
            TotalRuns = reader.GetInt32(0),
            SucceededRuns = reader.GetInt32(1),
            FailedRuns = reader.GetInt32(2),
            SuccessRate = reader.GetDouble(3),
            AvgDuration = !reader.IsDBNull(4)
                ? TimeSpan.FromSeconds(reader.GetDouble(4))
                : null,
            LastRunAt = !reader.IsDBNull(5)
                ? reader.GetDateTimeOffset(5)
                : null
        };
    }

    public async Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          WITH queue_names AS (
                              SELECT name FROM dbo.surefire_queues
                              UNION
                              SELECT ISNULL(j.queue, 'default') AS name
                              FROM dbo.surefire_runs r
                              JOIN dbo.surefire_jobs j ON j.name = r.job_name
                              WHERE r.status = 0
                              UNION
                              SELECT ISNULL(j.queue, 'default') AS name
                              FROM dbo.surefire_runs r
                              JOIN dbo.surefire_jobs j ON j.name = r.job_name
                              WHERE r.status = 1
                          ),
                          pending AS (
                              SELECT ISNULL(j.queue, 'default') AS queue_name, COUNT(*) AS cnt
                              FROM dbo.surefire_runs r
                              JOIN dbo.surefire_jobs j ON j.name = r.job_name
                              WHERE r.status = 0
                              GROUP BY ISNULL(j.queue, 'default')
                          ),
                          running AS (
                              SELECT ISNULL(j.queue, 'default') AS queue_name, COUNT(*) AS cnt
                              FROM dbo.surefire_runs r
                              JOIN dbo.surefire_jobs j ON j.name = r.job_name
                              WHERE r.status = 1
                              GROUP BY ISNULL(j.queue, 'default')
                          )
                          SELECT
                              qn.name,
                              ISNULL(pending.cnt, 0) AS pending_count,
                              ISNULL(running.cnt, 0) AS running_count
                          FROM queue_names qn
                          LEFT JOIN pending ON pending.queue_name = qn.name
                          LEFT JOIN running ON running.queue_name = qn.name
                          ORDER BY qn.name
                          """;

        var results = new Dictionary<string, QueueStats>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results[reader.GetString(0)] = new()
            {
                PendingCount = reader.GetInt32(1),
                RunningCount = reader.GetInt32(2)
            };
        }

        return results;
    }

    public bool IsTransientException(Exception ex) =>
        ex is SqlException sql && sql.Number is
            -2 // command timeout
            or 1205 // deadlock victim
            or 1222 // lock request timeout exceeded
            or 8645 // memory grant timeout
            or 8651 // low memory
            or 40197 // Azure SQL: service-induced error
            or 40501 // Azure SQL: service busy
            or 40613 // Azure SQL: database unavailable
            or 49918 // Azure SQL: not enough resources
            or 49919 // Azure SQL: too many CREATE/UPDATE operations
            or 49920; // Azure SQL: too many operations in progress

    private async Task<JobRun?> ClaimRunCoreAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);

        var jobNamesArr = jobNames as IList<string> ?? jobNames.ToList();
        var queueNamesArr = queueNames as IList<string> ?? queueNames.ToList();

        var jobParams = new List<string>();
        var queueParams = new List<string>();

        // Lock rate limit rows to serialize concurrent claims
        await using var lockRlCmd = CreateCommand(conn);
        lockRlCmd.Transaction = tx;
        for (var i = 0; i < jobNamesArr.Count; i++)
        {
            jobParams.Add($"@jn_{i}");
            lockRlCmd.Parameters.Add(CreateParameter($"@jn_{i}", jobNamesArr[i]));
        }

        var jobNamesIn = string.Join(", ", jobParams);
        lockRlCmd.CommandText = $"""
                                 SELECT name FROM dbo.surefire_jobs WITH (UPDLOCK, ROWLOCK)
                                 WHERE name IN ({jobNamesIn}) AND (max_concurrency IS NOT NULL OR rate_limit_name IS NOT NULL);

                                 SELECT name FROM dbo.surefire_queues WITH (UPDLOCK, ROWLOCK)
                                 WHERE name IN (
                                     SELECT ISNULL(j.queue, 'default') FROM dbo.surefire_jobs j WHERE j.name IN ({jobNamesIn})
                                 )
                                 AND max_concurrency IS NOT NULL;

                                 SELECT * FROM dbo.surefire_rate_limits WITH (UPDLOCK)
                                 WHERE name IN (
                                     SELECT j.rate_limit_name FROM dbo.surefire_jobs j
                                     WHERE j.name IN ({jobNamesIn}) AND j.rate_limit_name IS NOT NULL
                                     UNION
                                     SELECT q.rate_limit_name FROM dbo.surefire_jobs j
                                     JOIN dbo.surefire_queues q ON q.name = ISNULL(j.queue, 'default')
                                     WHERE j.name IN ({jobNamesIn}) AND q.rate_limit_name IS NOT NULL
                                 )
                                 """;
        await lockRlCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;

        for (var i = 0; i < jobNamesArr.Count; i++)
        {
            cmd.Parameters.Add(CreateParameter($"@jn_{i}", jobNamesArr[i]));
        }

        for (var i = 0; i < queueNamesArr.Count; i++)
        {
            queueParams.Add($"@qn_{i}");
            cmd.Parameters.Add(CreateParameter($"@qn_{i}", queueNamesArr[i]));
        }

        var queueNamesIn = string.Join(", ", queueParams);
        cmd.CommandText = $"""
                           DECLARE @now DATETIMEOFFSET(7) = TODATETIMEOFFSET(SYSUTCDATETIME(), 0);

                           ;WITH jobs_at_capacity AS (
                               SELECT cr.job_name
                               FROM dbo.surefire_runs cr
                               JOIN dbo.surefire_jobs cj ON cj.name = cr.job_name
                               WHERE cr.status = 1 AND cj.max_concurrency IS NOT NULL
                               GROUP BY cr.job_name, cj.max_concurrency
                               HAVING COUNT(*) >= cj.max_concurrency
                           ),
                           queues_at_capacity AS (
                               SELECT ISNULL(cj.queue, 'default') AS queue_name
                               FROM dbo.surefire_runs cr
                               JOIN dbo.surefire_jobs cj ON cj.name = cr.job_name
                               JOIN dbo.surefire_queues cq ON cq.name = ISNULL(cj.queue, 'default')
                               WHERE cr.status = 1 AND cq.max_concurrency IS NOT NULL
                               GROUP BY ISNULL(cj.queue, 'default'), cq.max_concurrency
                               HAVING COUNT(*) >= cq.max_concurrency
                           ),
                           candidate AS (
                               SELECT TOP 1 r.id, r.job_name
                               FROM dbo.surefire_runs r WITH (UPDLOCK, READPAST)
                               JOIN dbo.surefire_jobs j ON j.name = r.job_name
                               LEFT JOIN dbo.surefire_queues q ON q.name = ISNULL(j.queue, 'default')
                               WHERE r.status = 0
                                   AND r.not_before <= @now
                                   AND (r.not_after IS NULL OR r.not_after > @now)
                                   AND r.job_name IN ({jobNamesIn})
                                   AND ISNULL(j.queue, 'default') IN ({queueNamesIn})
                                   AND ISNULL(q.is_paused, 0) = 0
                                   AND (j.max_concurrency IS NULL OR r.job_name NOT IN (SELECT job_name FROM jobs_at_capacity))
                                   AND (q.max_concurrency IS NULL OR ISNULL(j.queue, 'default') NOT IN (SELECT queue_name FROM queues_at_capacity))
                                   AND (j.rate_limit_name IS NULL OR NOT EXISTS (
                                       SELECT 1 FROM dbo.surefire_rate_limits rl
                                       WHERE rl.name = j.rate_limit_name
                                       AND (CASE
                                           WHEN rl.type = 1 THEN
                                               CASE
                                                   WHEN rl.window_start IS NULL THEN 0
                                                   WHEN DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 >= rl.[window] * 2 THEN 0
                                                   WHEN DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 >= rl.[window] THEN
                                                       rl.current_count * IIF(
                                                           1.0 - (CAST(DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 AS FLOAT) - rl.[window]) / rl.[window] > 0,
                                                           1.0 - (CAST(DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 AS FLOAT) - rl.[window]) / rl.[window],
                                                           0)
                                                   ELSE
                                                       rl.current_count + rl.previous_count * IIF(1.0 - CAST(DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 AS FLOAT) / rl.[window] > 0,
                                                           1.0 - CAST(DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 AS FLOAT) / rl.[window], 0)
                                               END
                                           ELSE
                                               CASE
                                                   WHEN rl.window_start IS NULL THEN 0
                                                   WHEN DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 >= rl.[window] THEN 0
                                                   ELSE rl.current_count
                                               END
                                       END) >= rl.max_permits
                                   ))
                                   AND (q.rate_limit_name IS NULL OR q.rate_limit_name = j.rate_limit_name OR NOT EXISTS (
                                       SELECT 1 FROM dbo.surefire_rate_limits rl
                                       WHERE rl.name = q.rate_limit_name
                                       AND (CASE
                                           WHEN rl.type = 1 THEN
                                               CASE
                                                   WHEN rl.window_start IS NULL THEN 0
                                                   WHEN DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 >= rl.[window] * 2 THEN 0
                                                   WHEN DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 >= rl.[window] THEN
                                                       rl.current_count * IIF(
                                                           1.0 - (CAST(DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 AS FLOAT) - rl.[window]) / rl.[window] > 0,
                                                           1.0 - (CAST(DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 AS FLOAT) - rl.[window]) / rl.[window],
                                                           0)
                                                   ELSE
                                                       rl.current_count + rl.previous_count * IIF(1.0 - CAST(DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 AS FLOAT) / rl.[window] > 0,
                                                           1.0 - CAST(DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 AS FLOAT) / rl.[window], 0)
                                               END
                                           ELSE
                                               CASE
                                                   WHEN rl.window_start IS NULL THEN 0
                                                   WHEN DATEDIFF_BIG(MICROSECOND, rl.window_start, @now) * 10 >= rl.[window] THEN 0
                                                   ELSE rl.current_count
                                               END
                                       END) >= rl.max_permits
                                   ))
                               ORDER BY r.queue_priority DESC, r.priority DESC, r.not_before ASC, r.id ASC
                           )
                           UPDATE dbo.surefire_runs SET
                               status = 1,
                               node_name = @node_name,
                               started_at = @now,
                               last_heartbeat_at = @now,
                               attempt = dbo.surefire_runs.attempt + 1
                           OUTPUT INSERTED.*
                           FROM dbo.surefire_runs INNER JOIN candidate ON dbo.surefire_runs.id = candidate.id;
                           """;

        cmd.Parameters.Add(CreateParameter("@node_name", nodeName));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        JobRun? claimed = null;
        if (await reader.ReadAsync(cancellationToken))
        {
            claimed = ReadRun(reader);
        }

        await reader.CloseAsync();

        if (claimed is { })
        {
            await InsertEventsAsync(conn, tx,
                [RunStatusEvents.Create(claimed.Id, claimed.Attempt, claimed.Status, timeProvider.GetUtcNow())],
                cancellationToken);

            var job = await GetJobInternalAsync(conn, tx, claimed.JobName, cancellationToken);
            if (job is { })
            {
                var jobRateLimit = job.RateLimitName;
                var queueName = job.Queue ?? "default";
                var queueDef = await GetQueueInternalAsync(conn, tx, queueName, cancellationToken);
                var queueRateLimit = queueDef?.RateLimitName;

                if (jobRateLimit is { })
                {
                    await AcquireRateLimitAsync(conn, tx, jobRateLimit, cancellationToken);
                }

                if (queueRateLimit is { } && queueRateLimit != jobRateLimit)
                {
                    await AcquireRateLimitAsync(conn, tx, queueRateLimit, cancellationToken);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return claimed;
    }

    private async Task CreateRunsCoreAsync(IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        if (runs.Count == 0 && (initialEvents is null || initialEvents.Count == 0))
        {
            return;
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);

        const int paramsPerRun = 26;
        const int maxParams = 2100;
        var chunkSize = maxParams / paramsPerRun;

        for (var offset = 0; offset < runs.Count; offset += chunkSize)
        {
            var chunk = runs.Skip(offset).Take(chunkSize).ToList();
            var sb = new StringBuilder();
            sb.Append("""
                      INSERT INTO dbo.surefire_runs (
                          id, job_name, status, arguments, result, reason, progress,
                          created_at, started_at, completed_at, cancelled_at, node_name,
                          attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                          rerun_of_run_id, not_before, not_after, priority, deduplication_id,
                          last_heartbeat_at, batch_id, queue_priority
                      ) VALUES
                      """);

            await using var cmd = CreateCommand(conn);
            cmd.Transaction = tx;

            for (var i = 0; i < chunk.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(',');
                }

                var p = $"@p{i}_";
                sb.Append($"""
                           (
                               {p}id, {p}job_name, {p}status, {p}arguments, {p}result, {p}reason, {p}progress,
                               {p}created_at, {p}started_at, {p}completed_at, {p}cancelled_at, {p}node_name,
                               {p}attempt, {p}trace_id, {p}span_id, {p}parent_trace_id, {p}parent_span_id, {p}parent_run_id, {p}root_run_id,
                               {p}rerun_of_run_id, {p}not_before, {p}not_after, {p}priority, {p}deduplication_id,
                               {p}last_heartbeat_at, {p}batch_id,
                               ISNULL((SELECT q.priority FROM dbo.surefire_queues q WHERE q.name = ISNULL((SELECT j.queue FROM dbo.surefire_jobs j WHERE j.name = {p}job_name), 'default')), 0)
                           )
                           """);

                var run = chunk[i];
                AddRunParams(cmd, $"@p{i}_", run);
            }

            cmd.CommandText = sb.ToString();
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await InsertEventsAsync(conn, tx, initialEvents, cancellationToken);

        await tx.CommitAsync(cancellationToken);
    }

    private async Task<bool> TryCreateRunCoreAsync(JobRun run, int? maxActiveForJob,
        DateTimeOffset? lastCronFireAt,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;

        if (maxActiveForJob is { })
        {
            await using var lockCmd = CreateCommand(conn);
            lockCmd.Transaction = tx;
            lockCmd.CommandText = "SELECT 1 FROM dbo.surefire_jobs WITH (UPDLOCK, ROWLOCK) WHERE name = @name";
            lockCmd.Parameters.Add(CreateParameter("@name", run.JobName));
            await lockCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        var conditions = new List<string>();

        if (run.DeduplicationId is { })
        {
            conditions.Add("""
                           NOT EXISTS (
                               SELECT 1 FROM dbo.surefire_runs
                               WHERE job_name = @job_name AND deduplication_id = @dedup_id
                                   AND status NOT IN (2, 4, 5)
                           )
                           """);
            cmd.Parameters.Add(CreateParameter("@dedup_id", run.DeduplicationId));
        }

        if (maxActiveForJob is { })
        {
            conditions.Add("""
                           (SELECT COUNT(*) FROM dbo.surefire_runs
                            WHERE job_name = @job_name AND status NOT IN (2, 4, 5)) < @max_active
                           """);
            cmd.Parameters.Add(CreateParameter("@max_active", maxActiveForJob.Value));
            conditions.Add("""
                           ISNULL((SELECT is_enabled FROM dbo.surefire_jobs WHERE name = @job_name), 1) = 1
                           """);
        }

        if (conditions.Count == 0)
        {
            cmd.CommandText = """
                              INSERT INTO dbo.surefire_runs (
                                  id, job_name, status, arguments, result, reason, progress,
                                  created_at, started_at, completed_at, cancelled_at, node_name,
                                  attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                                  rerun_of_run_id, not_before, not_after, priority, deduplication_id,
                                  last_heartbeat_at, batch_id, queue_priority
                              )
                              SELECT
                                  @id, @job_name, @status, @arguments, @result, @reason, @progress,
                                  @created_at, @started_at, @completed_at, @cancelled_at, @node_name,
                                  @attempt, @trace_id, @span_id, @parent_trace_id, @parent_span_id, @parent_run_id, @root_run_id,
                                  @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id,
                                  @last_heartbeat_at, @batch_id,
                                  ISNULL((SELECT q.priority FROM dbo.surefire_queues q WHERE q.name = ISNULL((SELECT j.queue FROM dbo.surefire_jobs j WHERE j.name = @job_name), 'default')), 0)
                              WHERE NOT EXISTS (SELECT 1 FROM dbo.surefire_runs WHERE id = @id)
                              """;
        }
        else
        {
            var whereClause = string.Join(" AND ", conditions);
            cmd.CommandText = $"""
                               INSERT INTO dbo.surefire_runs (
                                   id, job_name, status, arguments, result, reason, progress,
                                   created_at, started_at, completed_at, cancelled_at, node_name,
                                   attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                                   rerun_of_run_id, not_before, not_after, priority, deduplication_id,
                                   last_heartbeat_at, batch_id, queue_priority
                               )
                               SELECT
                                   @id, @job_name, @status, @arguments, @result, @reason, @progress,
                                   @created_at, @started_at, @completed_at, @cancelled_at, @node_name,
                                   @attempt, @trace_id, @span_id, @parent_trace_id, @parent_span_id, @parent_run_id, @root_run_id,
                                   @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id,
                                   @last_heartbeat_at, @batch_id,
                                   ISNULL((SELECT q.priority FROM dbo.surefire_queues q WHERE q.name = ISNULL((SELECT j.queue FROM dbo.surefire_jobs j WHERE j.name = @job_name), 'default')), 0)
                               WHERE {whereClause}
                               """;
        }

        AddRunParams(cmd, "@", run);

        try
        {
            var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);

            if (rows > 0)
            {
                await InsertEventsAsync(conn, tx, initialEvents, cancellationToken);
            }

            if (rows > 0 && lastCronFireAt is { } fireAt)
            {
                await using var fireCmd = CreateCommand(conn);
                fireCmd.Transaction = tx;
                fireCmd.CommandText =
                    "UPDATE dbo.surefire_jobs SET last_cron_fire_at = @last_cron_fire_at WHERE name = @job_name";
                fireCmd.Parameters.Add(CreateParameter("@last_cron_fire_at", fireAt));
                fireCmd.Parameters.Add(CreateParameter("@job_name", run.JobName));
                await fireCmd.ExecuteNonQueryAsync(cancellationToken);
            }

            await tx.CommitAsync(cancellationToken);
            return rows > 0;
        }
        catch (SqlException ex) when (ex.Number is 2601 or 2627)
        {
            return false;
        }
    }

    private async Task InsertEventsAsync(SqlConnection conn, SqlTransaction tx,
        IReadOnlyList<RunEvent>? events, CancellationToken cancellationToken)
    {
        if (events is null || events.Count == 0)
        {
            return;
        }

        const int paramsPerEvent = 5;
        var chunkSize = 2100 / paramsPerEvent - 1;

        for (var offset = 0; offset < events.Count; offset += chunkSize)
        {
            var chunk = events.Skip(offset).Take(chunkSize).ToList();
            var sb = new StringBuilder();
            sb.Append("INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt) VALUES ");

            await using var cmd = CreateCommand(conn);
            cmd.Transaction = tx;
            for (var i = 0; i < chunk.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(',');
                }

                sb.Append($"(@run_id_{i}, @event_type_{i}, @payload_{i}, @created_at_{i}, @attempt_{i})");
                cmd.Parameters.Add(CreateParameter($"@run_id_{i}", chunk[i].RunId));
                cmd.Parameters.Add(CreateParameter($"@event_type_{i}", (short)chunk[i].EventType));
                cmd.Parameters.Add(CreateParameter($"@payload_{i}", chunk[i].Payload));
                cmd.Parameters.Add(CreateParameter($"@created_at_{i}", chunk[i].CreatedAt));
                cmd.Parameters.Add(CreateParameter($"@attempt_{i}", chunk[i].Attempt));
            }

            cmd.CommandText = sb.ToString();
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static void BuildRunFilterWhere(RunFilter filter, List<string> parts, SqlCommand cmd)
    {
        if (filter.Status is { })
        {
            parts.Add("status = @filter_status");
            cmd.Parameters.Add(CreateParameter("@filter_status", (int)filter.Status.Value));
        }

        if (filter.JobName is { })
        {
            if (filter.ExactJobName)
            {
                parts.Add("job_name = @filter_job_name");
                cmd.Parameters.Add(CreateParameter("@filter_job_name", filter.JobName));
            }
            else
            {
                parts.Add("job_name LIKE '%' + @filter_job_name + '%'");
                cmd.Parameters.Add(CreateParameter("@filter_job_name", EscapeLike(filter.JobName)));
            }
        }

        if (filter.ParentRunId is { })
        {
            parts.Add("parent_run_id = @filter_parent");
            cmd.Parameters.Add(CreateParameter("@filter_parent", filter.ParentRunId));
        }

        if (filter.RootRunId is { })
        {
            parts.Add("root_run_id = @filter_root");
            cmd.Parameters.Add(CreateParameter("@filter_root", filter.RootRunId));
        }

        if (filter.NodeName is { })
        {
            parts.Add("node_name = @filter_node");
            cmd.Parameters.Add(CreateParameter("@filter_node", filter.NodeName));
        }

        if (filter.CreatedAfter is { })
        {
            parts.Add("created_at > @filter_created_after");
            cmd.Parameters.Add(CreateParameter("@filter_created_after", filter.CreatedAfter.Value));
        }

        if (filter.CreatedBefore is { })
        {
            parts.Add("created_at < @filter_created_before");
            cmd.Parameters.Add(CreateParameter("@filter_created_before", filter.CreatedBefore.Value));
        }

        if (filter.CompletedAfter is { })
        {
            parts.Add("completed_at > @filter_completed_after");
            cmd.Parameters.Add(CreateParameter("@filter_completed_after", filter.CompletedAfter.Value));
        }

        if (filter.LastHeartbeatBefore is { })
        {
            parts.Add("last_heartbeat_at < @filter_hb_before");
            cmd.Parameters.Add(CreateParameter("@filter_hb_before", filter.LastHeartbeatBefore.Value));
        }

        if (filter.BatchId is { })
        {
            parts.Add("batch_id = @batch_id_filter");
            cmd.Parameters.Add(CreateParameter("@batch_id_filter", filter.BatchId));
        }

        if (filter.IsTerminal is { })
        {
            parts.Add(filter.IsTerminal.Value
                ? "status IN (2, 4, 5)"
                : "status NOT IN (2, 4, 5)");
        }
    }

    private static void AddRunParams(SqlCommand cmd, string prefix, JobRun run)
    {
        cmd.Parameters.Add(CreateParameter($"{prefix}id", run.Id));
        cmd.Parameters.Add(CreateParameter($"{prefix}job_name", run.JobName));
        cmd.Parameters.Add(CreateParameter($"{prefix}status", (int)run.Status));
        cmd.Parameters.Add(CreateParameter($"{prefix}arguments", (object?)run.Arguments ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}result", (object?)run.Result ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}reason", (object?)run.Reason ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}progress", run.Progress));
        cmd.Parameters.Add(CreateParameter($"{prefix}created_at", run.CreatedAt));
        cmd.Parameters.Add(CreateParameter($"{prefix}started_at",
            run.StartedAt.HasValue ? run.StartedAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}completed_at",
            run.CompletedAt.HasValue ? run.CompletedAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}cancelled_at",
            run.CancelledAt.HasValue ? run.CancelledAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}node_name", (object?)run.NodeName ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}attempt", run.Attempt));
        cmd.Parameters.Add(CreateParameter($"{prefix}trace_id", (object?)run.TraceId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}span_id", (object?)run.SpanId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}parent_trace_id", (object?)run.ParentTraceId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}parent_span_id", (object?)run.ParentSpanId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}parent_run_id", (object?)run.ParentRunId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}root_run_id", (object?)run.RootRunId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}rerun_of_run_id", (object?)run.RerunOfRunId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}not_before", run.NotBefore));
        cmd.Parameters.Add(CreateParameter($"{prefix}not_after",
            run.NotAfter.HasValue ? run.NotAfter.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}priority", run.Priority));
        cmd.Parameters.Add(CreateParameter($"{prefix}deduplication_id", (object?)run.DeduplicationId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}last_heartbeat_at",
            run.LastHeartbeatAt.HasValue ? run.LastHeartbeatAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter($"{prefix}batch_id", (object?)run.BatchId ?? DBNull.Value));
    }

    private static JobRun ReadRun(SqlDataReader reader) => new()
    {
        Id = reader.GetString(reader.GetOrdinal("id")),
        JobName = reader.GetString(reader.GetOrdinal("job_name")),
        Status = (JobStatus)reader.GetInt32(reader.GetOrdinal("status")),
        Progress = reader.GetDouble(reader.GetOrdinal("progress")),
        CreatedAt = reader.GetDateTimeOffset(reader.GetOrdinal("created_at")),
        Attempt = reader.GetInt32(reader.GetOrdinal("attempt")),
        NotBefore = reader.GetDateTimeOffset(reader.GetOrdinal("not_before")),
        Priority = reader.GetInt32(reader.GetOrdinal("priority")),
        QueuePriority = reader.GetInt32(reader.GetOrdinal("queue_priority")),
        Arguments = reader.IsDBNull(reader.GetOrdinal("arguments"))
            ? null
            : reader.GetString(reader.GetOrdinal("arguments")),
        Result = reader.IsDBNull(reader.GetOrdinal("result")) ? null : reader.GetString(reader.GetOrdinal("result")),
        Reason = reader.IsDBNull(reader.GetOrdinal("reason")) ? null : reader.GetString(reader.GetOrdinal("reason")),
        StartedAt = reader.IsDBNull(reader.GetOrdinal("started_at"))
            ? null
            : reader.GetDateTimeOffset(reader.GetOrdinal("started_at")),
        CompletedAt = reader.IsDBNull(reader.GetOrdinal("completed_at"))
            ? null
            : reader.GetDateTimeOffset(reader.GetOrdinal("completed_at")),
        CancelledAt = reader.IsDBNull(reader.GetOrdinal("cancelled_at"))
            ? null
            : reader.GetDateTimeOffset(reader.GetOrdinal("cancelled_at")),
        NodeName = reader.IsDBNull(reader.GetOrdinal("node_name"))
            ? null
            : reader.GetString(reader.GetOrdinal("node_name")),
        TraceId = reader.IsDBNull(reader.GetOrdinal("trace_id"))
            ? null
            : reader.GetString(reader.GetOrdinal("trace_id")),
        SpanId = reader.IsDBNull(reader.GetOrdinal("span_id")) ? null : reader.GetString(reader.GetOrdinal("span_id")),
        ParentTraceId = reader.IsDBNull(reader.GetOrdinal("parent_trace_id"))
            ? null
            : reader.GetString(reader.GetOrdinal("parent_trace_id")),
        ParentSpanId = reader.IsDBNull(reader.GetOrdinal("parent_span_id"))
            ? null
            : reader.GetString(reader.GetOrdinal("parent_span_id")),
        ParentRunId = reader.IsDBNull(reader.GetOrdinal("parent_run_id"))
            ? null
            : reader.GetString(reader.GetOrdinal("parent_run_id")),
        RootRunId = reader.IsDBNull(reader.GetOrdinal("root_run_id"))
            ? null
            : reader.GetString(reader.GetOrdinal("root_run_id")),
        RerunOfRunId = reader.IsDBNull(reader.GetOrdinal("rerun_of_run_id"))
            ? null
            : reader.GetString(reader.GetOrdinal("rerun_of_run_id")),
        NotAfter = reader.IsDBNull(reader.GetOrdinal("not_after"))
            ? null
            : reader.GetDateTimeOffset(reader.GetOrdinal("not_after")),
        DeduplicationId = reader.IsDBNull(reader.GetOrdinal("deduplication_id"))
            ? null
            : reader.GetString(reader.GetOrdinal("deduplication_id")),
        LastHeartbeatAt = reader.IsDBNull(reader.GetOrdinal("last_heartbeat_at"))
            ? null
            : reader.GetDateTimeOffset(reader.GetOrdinal("last_heartbeat_at")),
        BatchId = reader.IsDBNull(reader.GetOrdinal("batch_id"))
            ? null
            : reader.GetString(reader.GetOrdinal("batch_id"))
    };

    private static JobBatch ReadBatch(SqlDataReader reader) => new()
    {
        Id = reader.GetString(reader.GetOrdinal("id")),
        Status = (JobStatus)reader.GetInt16(reader.GetOrdinal("status")),
        Total = reader.GetInt32(reader.GetOrdinal("total")),
        Succeeded = reader.GetInt32(reader.GetOrdinal("succeeded")),
        Failed = reader.GetInt32(reader.GetOrdinal("failed")),
        Cancelled = reader.GetOrdinal("cancelled") is var cCol && !reader.IsDBNull(cCol) ? reader.GetInt32(cCol) : 0,
        CreatedAt = reader.GetDateTimeOffset(reader.GetOrdinal("created_at")),
        CompletedAt = reader.IsDBNull(reader.GetOrdinal("completed_at"))
            ? null
            : reader.GetDateTimeOffset(reader.GetOrdinal("completed_at"))
    };

    private static JobDefinition ReadJob(SqlDataReader reader)
    {
        var limitCol = reader.GetOrdinal("fire_all_limit");
        var descriptionCol = reader.GetOrdinal("description");
        var cronCol = reader.GetOrdinal("cron_expression");
        var timeZoneCol = reader.GetOrdinal("time_zone_id");
        var timeoutCol = reader.GetOrdinal("timeout");
        var maxConcurrencyCol = reader.GetOrdinal("max_concurrency");
        var retryPolicyCol = reader.GetOrdinal("retry_policy");
        var queueCol = reader.GetOrdinal("queue");
        var rateLimitCol = reader.GetOrdinal("rate_limit_name");
        var schemaCol = reader.GetOrdinal("arguments_schema");
        var heartbeatCol = reader.GetOrdinal("last_heartbeat_at");
        var cronFireCol = reader.GetOrdinal("last_cron_fire_at");

        return new()
        {
            Name = reader.GetString(reader.GetOrdinal("name")),
            Tags = JsonSerializer.Deserialize<string[]>(reader.GetString(reader.GetOrdinal("tags"))) ?? [],
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            IsContinuous = reader.GetBoolean(reader.GetOrdinal("is_continuous")),
            IsEnabled = reader.GetBoolean(reader.GetOrdinal("is_enabled")),
            MisfirePolicy = (MisfirePolicy)reader.GetInt32(reader.GetOrdinal("misfire_policy")),
            FireAllLimit = reader.IsDBNull(limitCol) ? null : reader.GetInt32(limitCol),
            Description = reader.IsDBNull(descriptionCol) ? null : reader.GetString(descriptionCol),
            CronExpression = reader.IsDBNull(cronCol) ? null : reader.GetString(cronCol),
            TimeZoneId = reader.IsDBNull(timeZoneCol) ? null : reader.GetString(timeZoneCol),
            Timeout = reader.IsDBNull(timeoutCol) ? null : TimeSpan.FromTicks(reader.GetInt64(timeoutCol)),
            MaxConcurrency = reader.IsDBNull(maxConcurrencyCol) ? null : reader.GetInt32(maxConcurrencyCol),
            RetryPolicy = reader.IsDBNull(retryPolicyCol)
                ? new()
                : DeserializeRetryPolicy(reader.GetString(retryPolicyCol)),
            Queue = reader.IsDBNull(queueCol) ? null : reader.GetString(queueCol),
            RateLimitName = reader.IsDBNull(rateLimitCol) ? null : reader.GetString(rateLimitCol),
            ArgumentsSchema = reader.IsDBNull(schemaCol) ? null : reader.GetString(schemaCol),
            LastHeartbeatAt = reader.IsDBNull(heartbeatCol) ? null : reader.GetDateTimeOffset(heartbeatCol),
            LastCronFireAt = reader.IsDBNull(cronFireCol) ? null : reader.GetDateTimeOffset(cronFireCol)
        };
    }

    private static QueueDefinition ReadQueue(SqlDataReader reader)
    {
        var maxConcurrencyCol = reader.GetOrdinal("max_concurrency");
        var rateLimitCol = reader.GetOrdinal("rate_limit_name");
        var heartbeatCol = reader.GetOrdinal("last_heartbeat_at");

        return new()
        {
            Name = reader.GetString(reader.GetOrdinal("name")),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            IsPaused = reader.GetBoolean(reader.GetOrdinal("is_paused")),
            MaxConcurrency = reader.IsDBNull(maxConcurrencyCol) ? null : reader.GetInt32(maxConcurrencyCol),
            RateLimitName = reader.IsDBNull(rateLimitCol) ? null : reader.GetString(rateLimitCol),
            LastHeartbeatAt = reader.IsDBNull(heartbeatCol) ? null : reader.GetDateTimeOffset(heartbeatCol)
        };
    }

    private static NodeInfo ReadNode(SqlDataReader reader) => new()
    {
        Name = reader.GetString(reader.GetOrdinal("name")),
        StartedAt = reader.GetDateTimeOffset(reader.GetOrdinal("started_at")),
        LastHeartbeatAt = reader.GetDateTimeOffset(reader.GetOrdinal("last_heartbeat_at")),
        RunningCount = reader.GetInt32(reader.GetOrdinal("running_count")),
        RegisteredJobNames =
            JsonSerializer.Deserialize<string[]>(reader.GetString(reader.GetOrdinal("registered_job_names"))) ?? [],
        RegisteredQueueNames =
            JsonSerializer.Deserialize<string[]>(reader.GetString(reader.GetOrdinal("registered_queue_names"))) ?? []
    };

    private static RunEvent ReadEvent(SqlDataReader reader) => new()
    {
        Id = reader.GetInt64(reader.GetOrdinal("id")),
        RunId = reader.GetString(reader.GetOrdinal("run_id")),
        EventType = (RunEventType)reader.GetInt16(reader.GetOrdinal("event_type")),
        Payload = reader.GetString(reader.GetOrdinal("payload")),
        CreatedAt = reader.GetDateTimeOffset(reader.GetOrdinal("created_at")),
        Attempt = reader.GetInt32(reader.GetOrdinal("attempt"))
    };

    private async Task<JobDefinition?> GetJobInternalAsync(SqlConnection conn, SqlTransaction tx,
        string name, CancellationToken cancellationToken)
    {
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = "SELECT * FROM dbo.surefire_jobs WHERE name = @name";
        cmd.Parameters.Add(CreateParameter("@name", name));
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return ReadJob(reader);
    }

    private async Task<QueueDefinition?> GetQueueInternalAsync(SqlConnection conn, SqlTransaction tx,
        string name, CancellationToken cancellationToken)
    {
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = "SELECT * FROM dbo.surefire_queues WHERE name = @name";
        cmd.Parameters.Add(CreateParameter("@name", name));
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return ReadQueue(reader);
    }

    private async Task AcquireRateLimitAsync(SqlConnection conn, SqlTransaction tx,
        string rateLimitName, CancellationToken cancellationToken)
    {
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          DECLARE @now DATETIMEOFFSET(7) = TODATETIMEOFFSET(SYSUTCDATETIME(), 0);

                          UPDATE dbo.surefire_rate_limits SET
                              previous_count = CASE
                                  WHEN window_start IS NULL THEN 0
                                  WHEN DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10 >= [window] * 2 THEN 0
                                  WHEN DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10 >= [window] THEN current_count
                                  ELSE previous_count
                              END,
                              current_count = CASE
                                  WHEN window_start IS NULL THEN 1
                                  WHEN DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10 >= [window] THEN 1
                                  ELSE current_count + 1
                              END,
                              window_start = CASE
                                  WHEN window_start IS NULL THEN @now
                                  WHEN DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10 >= [window] * 2 THEN
                                      DATEADD(MICROSECOND,
                                          (DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10 / [window])
                                          * [window] / 10, window_start)
                                  WHEN DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10 >= [window] THEN
                                      DATEADD(MICROSECOND, [window] / 10, window_start)
                                  ELSE window_start
                              END
                          WHERE name = @name
                          """;
        cmd.Parameters.Add(CreateParameter("@name", rateLimitName));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string SerializeRetryPolicy(RetryPolicy policy) =>
        JsonSerializer.Serialize(new
        {
            maxRetries = policy.MaxRetries,
            backoffType = (int)policy.BackoffType,
            initialDelay = policy.InitialDelay.Ticks,
            maxDelay = policy.MaxDelay.Ticks,
            jitter = policy.Jitter
        });

    private static RetryPolicy DeserializeRetryPolicy(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        return new()
        {
            MaxRetries = root.GetProperty("maxRetries").GetInt32(),
            BackoffType = (BackoffType)root.GetProperty("backoffType").GetInt32(),
            InitialDelay = TimeSpan.FromTicks(root.GetProperty("initialDelay").GetInt64()),
            MaxDelay = TimeSpan.FromTicks(root.GetProperty("maxDelay").GetInt64()),
            Jitter = root.GetProperty("jitter").GetBoolean()
        };
    }

    private async Task ReleaseMigrationLockAsync(SqlConnection conn)
    {
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "EXEC sp_releaseapplock @Resource = 'surefire_migrate', @LockOwner = 'Session'";
        await cmd.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private SqlCommand CreateCommand(SqlConnection conn)
    {
        var command = conn.CreateCommand();
        if (CommandTimeoutSeconds is { } timeoutSeconds)
        {
            command.CommandTimeout = timeoutSeconds;
        }

        return command;
    }

    private static SqlParameter CreateParameter(string name, object? value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var parameter = new SqlParameter { ParameterName = name };

        if (value is null || value == DBNull.Value)
        {
            parameter.SqlDbType = SqlDbType.NVarChar;
            parameter.Size = -1;
            parameter.Value = DBNull.Value;
            return parameter;
        }

        switch (value)
        {
            case string text:
                parameter.SqlDbType = SqlDbType.NVarChar;
                parameter.Size = -1;
                parameter.Value = text;
                return parameter;

            case bool bit:
                parameter.SqlDbType = SqlDbType.Bit;
                parameter.Value = bit;
                return parameter;

            case byte tinyInt:
                parameter.SqlDbType = SqlDbType.TinyInt;
                parameter.Value = tinyInt;
                return parameter;

            case short smallInt:
                parameter.SqlDbType = SqlDbType.SmallInt;
                parameter.Value = smallInt;
                return parameter;

            case int int32:
                parameter.SqlDbType = SqlDbType.Int;
                parameter.Value = int32;
                return parameter;

            case long int64:
                parameter.SqlDbType = SqlDbType.BigInt;
                parameter.Value = int64;
                return parameter;

            case float real:
                parameter.SqlDbType = SqlDbType.Real;
                parameter.Value = real;
                return parameter;

            case double dbl:
                parameter.SqlDbType = SqlDbType.Float;
                parameter.Value = dbl;
                return parameter;

            case decimal number:
                parameter.SqlDbType = SqlDbType.Decimal;
                parameter.Value = number;
                return parameter;

            case DateTimeOffset dto:
                parameter.SqlDbType = SqlDbType.DateTimeOffset;
                parameter.Value = dto;
                return parameter;

            case DateTime dt:
                parameter.SqlDbType = SqlDbType.DateTime2;
                parameter.Value = dt;
                return parameter;

            case Guid guid:
                parameter.SqlDbType = SqlDbType.UniqueIdentifier;
                parameter.Value = guid;
                return parameter;

            case byte[] bytes:
                parameter.SqlDbType = SqlDbType.VarBinary;
                parameter.Size = -1;
                parameter.Value = bytes;
                return parameter;

            default:
                parameter.SqlDbType = SqlDbType.Variant;
                parameter.Value = value;
                return parameter;
        }
    }

    private static string EscapeLike(string input) =>
        input.Replace("[", "[[]").Replace("%", "[%]").Replace("_", "[_]");
}