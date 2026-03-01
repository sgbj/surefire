using System.Text;
using System.Text.Json;
using Npgsql;

namespace Surefire.PostgreSql;

/// <summary>
///     PostgreSQL implementation of <see cref="IJobStore" />.
/// </summary>
internal sealed class PostgreSqlJobStore(PostgreSqlOptions options, TimeProvider timeProvider)
    : IJobStore, IAsyncDisposable
{
    public ValueTask DisposeAsync() => options.DisposeAsync();

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);

        await using var lockCmd = conn.CreateCommand();
        lockCmd.CommandText = "SELECT pg_advisory_lock(hashtext('surefire_migrate'))";
        await lockCmd.ExecuteNonQueryAsync(cancellationToken);

        try
        {
            await using var migCmd = conn.CreateCommand();
            migCmd.CommandText =
                "CREATE TABLE IF NOT EXISTS surefire_schema_migrations (version INT NOT NULL PRIMARY KEY)";
            await migCmd.ExecuteNonQueryAsync(cancellationToken);

            await using var checkCmd = conn.CreateCommand();
            checkCmd.CommandText = "SELECT COUNT(*) FROM surefire_schema_migrations WHERE version = 1";
            if ((long)(await checkCmd.ExecuteScalarAsync(cancellationToken))! > 0)
            {
                return;
            }

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = """
                              CREATE TABLE IF NOT EXISTS surefire_jobs (
                                  name TEXT PRIMARY KEY,
                                  description TEXT,
                                  tags TEXT[] NOT NULL DEFAULT '{}',
                                  cron_expression TEXT,
                                  time_zone_id TEXT,
                                  timeout BIGINT,
                                  max_concurrency INT,
                                  priority INT NOT NULL DEFAULT 0,
                                  retry_policy JSONB,
                                  is_continuous BOOLEAN NOT NULL DEFAULT FALSE,
                                  queue TEXT,
                                  rate_limit_name TEXT,
                                  is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                                  misfire_policy INT NOT NULL DEFAULT 0,
                                  arguments_schema TEXT,
                                  last_heartbeat_at TIMESTAMPTZ,
                                  last_cron_fire_at TIMESTAMPTZ
                              );

                              CREATE TABLE IF NOT EXISTS surefire_runs (
                                  id TEXT PRIMARY KEY,
                                  job_name TEXT NOT NULL,
                                  status INT NOT NULL DEFAULT 0,
                                  arguments TEXT,
                                  result TEXT,
                                  error TEXT,
                                  progress DOUBLE PRECISION NOT NULL DEFAULT 0,
                                  created_at TIMESTAMPTZ NOT NULL,
                                  started_at TIMESTAMPTZ,
                                  completed_at TIMESTAMPTZ,
                                  cancelled_at TIMESTAMPTZ,
                                  node_name TEXT,
                                  attempt INT NOT NULL DEFAULT 0,
                                  trace_id TEXT,
                                  span_id TEXT,
                                  parent_run_id TEXT,
                                  root_run_id TEXT,
                                  rerun_of_run_id TEXT,
                                  not_before TIMESTAMPTZ NOT NULL,
                                  not_after TIMESTAMPTZ,
                                  priority INT NOT NULL DEFAULT 0,
                                  deduplication_id TEXT,
                                  last_heartbeat_at TIMESTAMPTZ,
                                  queue_priority INT NOT NULL DEFAULT 0,
                                  batch_total INT,
                                  batch_completed INT NOT NULL DEFAULT 0,
                                  batch_failed INT NOT NULL DEFAULT 0
                              );

                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_claim
                                  ON surefire_runs (queue_priority DESC, priority DESC, not_before, id)
                                  WHERE status = 0 AND batch_total IS NULL;

                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_root
                                  ON surefire_runs (root_run_id)
                                  WHERE root_run_id IS NOT NULL;

                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_parent
                                  ON surefire_runs (parent_run_id)
                                  WHERE parent_run_id IS NOT NULL;

                              CREATE UNIQUE INDEX IF NOT EXISTS ix_surefire_runs_dedup
                                  ON surefire_runs (job_name, deduplication_id)
                                  WHERE deduplication_id IS NOT NULL AND status NOT IN (2, 4, 5);

                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_completed
                                  ON surefire_runs (completed_at, id)
                                  WHERE completed_at IS NOT NULL;

                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_job_running
                                  ON surefire_runs (job_name)
                                  WHERE status = 1;

                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_job_nonterminal
                                  ON surefire_runs (job_name)
                                  WHERE status NOT IN (2, 4, 5);

                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_created
                                  ON surefire_runs (created_at DESC, id DESC);

                              CREATE TABLE IF NOT EXISTS surefire_events (
                                  id BIGSERIAL PRIMARY KEY,
                                  run_id TEXT NOT NULL,
                                  event_type SMALLINT NOT NULL,
                                  payload TEXT NOT NULL,
                                  created_at TIMESTAMPTZ NOT NULL,
                                  attempt INT NOT NULL DEFAULT 1,
                                  FOREIGN KEY (run_id) REFERENCES surefire_runs(id) ON DELETE CASCADE
                              );

                              CREATE INDEX IF NOT EXISTS ix_surefire_events_run
                                  ON surefire_events (run_id, id);

                              CREATE TABLE IF NOT EXISTS surefire_nodes (
                                  name TEXT PRIMARY KEY,
                                  started_at TIMESTAMPTZ NOT NULL,
                                  last_heartbeat_at TIMESTAMPTZ NOT NULL,
                                  running_count INT NOT NULL DEFAULT 0,
                                  registered_job_names TEXT[] NOT NULL DEFAULT '{}',
                                  registered_queue_names TEXT[] NOT NULL DEFAULT '{}'
                              );

                              CREATE TABLE IF NOT EXISTS surefire_queues (
                                  name TEXT PRIMARY KEY,
                                  priority INT NOT NULL DEFAULT 0,
                                  max_concurrency INT,
                                  is_paused BOOLEAN NOT NULL DEFAULT FALSE,
                                  rate_limit_name TEXT,
                                  last_heartbeat_at TIMESTAMPTZ
                              );

                              CREATE TABLE IF NOT EXISTS surefire_rate_limits (
                                  name TEXT PRIMARY KEY,
                                  type INT NOT NULL DEFAULT 0,
                                  max_permits INT NOT NULL,
                                  "window" BIGINT NOT NULL,
                                  last_heartbeat_at TIMESTAMPTZ,
                                  current_count INT NOT NULL DEFAULT 0,
                                  previous_count INT NOT NULL DEFAULT 0,
                                  window_start TIMESTAMPTZ
                              );

                              INSERT INTO surefire_schema_migrations (version) VALUES (1) ON CONFLICT DO NOTHING;
                              """;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        finally
        {
            await using var unlockCmd = conn.CreateCommand();
            unlockCmd.CommandText = "SELECT pg_advisory_unlock(hashtext('surefire_migrate'))";
            await unlockCmd.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    public async Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          INSERT INTO surefire_jobs (
                              name, description, tags, cron_expression, time_zone_id, timeout,
                              max_concurrency, priority, retry_policy, is_continuous, queue,
                              rate_limit_name, is_enabled, misfire_policy, arguments_schema,
                              last_heartbeat_at
                          ) VALUES (
                              @name, @description, @tags, @cron_expression, @time_zone_id, @timeout,
                              @max_concurrency, @priority, @retry_policy::jsonb, @is_continuous, @queue,
                              @rate_limit_name, @is_enabled, @misfire_policy, @arguments_schema,
                              NOW()
                          )
                          ON CONFLICT (name) DO UPDATE SET
                              description = EXCLUDED.description,
                              tags = EXCLUDED.tags,
                              cron_expression = EXCLUDED.cron_expression,
                              time_zone_id = EXCLUDED.time_zone_id,
                              timeout = EXCLUDED.timeout,
                              max_concurrency = EXCLUDED.max_concurrency,
                              priority = EXCLUDED.priority,
                              retry_policy = EXCLUDED.retry_policy,
                              is_continuous = EXCLUDED.is_continuous,
                              queue = EXCLUDED.queue,
                              rate_limit_name = EXCLUDED.rate_limit_name,
                              misfire_policy = EXCLUDED.misfire_policy,
                              arguments_schema = EXCLUDED.arguments_schema,
                              last_heartbeat_at = NOW()
                          """;

        cmd.Parameters.AddWithValue("name", job.Name);
        cmd.Parameters.AddWithValue("description", (object?)job.Description ?? DBNull.Value);
        cmd.Parameters.AddWithValue("tags", job.Tags);
        cmd.Parameters.AddWithValue("cron_expression", (object?)job.CronExpression ?? DBNull.Value);
        cmd.Parameters.AddWithValue("time_zone_id", (object?)job.TimeZoneId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("timeout", job.Timeout.HasValue ? job.Timeout.Value.Ticks : DBNull.Value);
        cmd.Parameters.AddWithValue("max_concurrency",
            job.MaxConcurrency.HasValue ? job.MaxConcurrency.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("priority", job.Priority);
        cmd.Parameters.AddWithValue("retry_policy", SerializeRetryPolicy(job.RetryPolicy));
        cmd.Parameters.AddWithValue("is_continuous", job.IsContinuous);
        cmd.Parameters.AddWithValue("queue", (object?)job.Queue ?? DBNull.Value);
        cmd.Parameters.AddWithValue("rate_limit_name", (object?)job.RateLimitName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("is_enabled", job.IsEnabled);
        cmd.Parameters.AddWithValue("misfire_policy", (int)job.MisfirePolicy);
        cmd.Parameters.AddWithValue("arguments_schema", (object?)job.ArgumentsSchema ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM surefire_jobs WHERE name = @name";
        cmd.Parameters.AddWithValue("name", name);

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
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();

        var sb = new StringBuilder("SELECT * FROM surefire_jobs WHERE 1=1");

        if (filter?.Name is { } nameFilter)
        {
            sb.Append(" AND name ILIKE '%' || @name || '%' ESCAPE '\\'");
            cmd.Parameters.AddWithValue("name", EscapeLike(nameFilter));
        }

        if (filter?.Tag is { } tagFilter)
        {
            sb.Append(" AND EXISTS (SELECT 1 FROM unnest(tags) t(v) WHERE LOWER(v) = LOWER(@tag))");
            cmd.Parameters.AddWithValue("tag", tagFilter);
        }

        if (filter?.IsEnabled is { } enabledFilter)
        {
            sb.Append(" AND is_enabled = @is_enabled");
            cmd.Parameters.AddWithValue("is_enabled", enabledFilter);
        }

        if (filter?.HeartbeatAfter is { } heartbeatFilter)
        {
            sb.Append(" AND last_heartbeat_at > @heartbeat_after");
            cmd.Parameters.AddWithValue("heartbeat_after", heartbeatFilter);
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
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE surefire_jobs SET is_enabled = @enabled WHERE name = @name";
        cmd.Parameters.AddWithValue("name", name);
        cmd.Parameters.AddWithValue("enabled", enabled);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateLastCronFireAtAsync(string jobName, DateTimeOffset fireAt,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE surefire_jobs SET last_cron_fire_at = @fire_at WHERE name = @name";
        cmd.Parameters.AddWithValue("name", jobName);
        cmd.Parameters.AddWithValue("fire_at", fireAt);
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
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM surefire_runs WHERE id = @id";
        cmd.Parameters.AddWithValue("id", id);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return ReadRun(reader);
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

        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);

        var whereParts = new List<string>();
        await using var cmd = conn.CreateCommand();

        BuildRunFilterWhere(filter, whereParts, cmd);
        var whereClause = whereParts.Count > 0 ? "WHERE " + string.Join(" AND ", whereParts) : "";

        var orderBy = filter.OrderBy switch
        {
            RunOrderBy.StartedAt => "started_at DESC NULLS LAST, id DESC",
            RunOrderBy.CompletedAt => "completed_at DESC NULLS LAST, id DESC",
            _ => "created_at DESC, id DESC"
        };

        cmd.CommandText =
            $"SELECT *, COUNT(*) OVER() AS total_count FROM surefire_runs {whereClause} ORDER BY {orderBy} LIMIT @take OFFSET @skip";
        cmd.Parameters.AddWithValue("take", take);
        cmd.Parameters.AddWithValue("skip", skip);

        var items = new List<JobRun>();
        var totalCount = 0;
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(ReadRun(reader));
            if (items.Count == 1)
            {
                totalCount = Convert.ToInt32(reader.GetInt64(reader.GetOrdinal("total_count")));
            }
        }

        return new() { Items = items, TotalCount = totalCount };
    }

    public async Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          UPDATE surefire_runs SET
                              progress = @progress,
                              result = @result,
                              error = @error,
                              trace_id = @trace_id,
                              span_id = @span_id,
                              last_heartbeat_at = @last_heartbeat_at
                          WHERE id = @id AND node_name IS NOT DISTINCT FROM @node_name AND status NOT IN (2, 4, 5)
                          """;

        cmd.Parameters.AddWithValue("id", run.Id);
        cmd.Parameters.AddWithValue("progress", run.Progress);
        cmd.Parameters.AddWithValue("result", (object?)run.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("error", (object?)run.Error ?? DBNull.Value);
        cmd.Parameters.AddWithValue("trace_id", (object?)run.TraceId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("span_id", (object?)run.SpanId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("last_heartbeat_at",
            run.LastHeartbeatAt.HasValue ? run.LastHeartbeatAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("node_name", (object?)run.NodeName ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<bool> TryTransitionRunAsync(RunStatusTransition transition,
        CancellationToken cancellationToken = default)
    {
        if (!RunTransitionRules.IsAllowed(transition.ExpectedStatus, transition.NewStatus)
            || !transition.HasRequiredFields())
        {
            return false;
        }

        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          UPDATE surefire_runs SET
                              status = @new_status,
                              node_name = @node_name,
                              started_at = COALESCE(@started_at, started_at),
                              completed_at = COALESCE(@completed_at, completed_at),
                              cancelled_at = COALESCE(@cancelled_at, cancelled_at),
                              error = @error,
                              result = @result,
                              progress = @progress,
                              not_before = @not_before,
                              last_heartbeat_at = COALESCE(@last_heartbeat_at, last_heartbeat_at)
                          WHERE id = @id
                              AND status = @expected_status
                              AND attempt = @expected_attempt
                              AND status NOT IN (2, 4, 5)
                          """;

        cmd.Parameters.AddWithValue("id", transition.RunId);
        cmd.Parameters.AddWithValue("new_status", (int)transition.NewStatus);
        cmd.Parameters.AddWithValue("node_name", (object?)transition.NodeName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("started_at",
            transition.StartedAt.HasValue ? transition.StartedAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("completed_at",
            transition.CompletedAt.HasValue ? transition.CompletedAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("cancelled_at",
            transition.CancelledAt.HasValue ? transition.CancelledAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("error", (object?)transition.Error ?? DBNull.Value);
        cmd.Parameters.AddWithValue("result", (object?)transition.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("progress", transition.Progress);
        cmd.Parameters.AddWithValue("not_before", transition.NotBefore);
        cmd.Parameters.AddWithValue("last_heartbeat_at",
            transition.LastHeartbeatAt.HasValue ? transition.LastHeartbeatAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("expected_status", (int)transition.ExpectedStatus);
        cmd.Parameters.AddWithValue("expected_attempt", transition.ExpectedAttempt);

        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0;
    }

    public async Task<bool> TryCancelRunAsync(string runId,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          UPDATE surefire_runs SET
                              status = 4,
                              cancelled_at = NOW(),
                              completed_at = NOW()
                          WHERE id = @id AND status NOT IN (2, 4, 5)
                          """;

        cmd.Parameters.AddWithValue("id", runId);

        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows > 0;
    }

    public async Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, CancellationToken cancellationToken = default)
    {
        if (jobNames.Count == 0 || queueNames.Count == 0)
        {
            return null;
        }

        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        await using var lockJobCmd = conn.CreateCommand();
        lockJobCmd.Transaction = tx;
        lockJobCmd.CommandText =
            "SELECT name FROM surefire_jobs WHERE name = ANY(@job_names) AND (max_concurrency IS NOT NULL OR rate_limit_name IS NOT NULL) FOR UPDATE";
        lockJobCmd.Parameters.AddWithValue("job_names", jobNames.ToArray());
        await lockJobCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var lockQueueCmd = conn.CreateCommand();
        lockQueueCmd.Transaction = tx;
        lockQueueCmd.CommandText = """
                                   SELECT name FROM surefire_queues
                                   WHERE name IN (
                                       SELECT COALESCE(j.queue, 'default') FROM surefire_jobs j WHERE j.name = ANY(@job_names)
                                   )
                                   AND max_concurrency IS NOT NULL
                                   FOR UPDATE
                                   """;
        lockQueueCmd.Parameters.AddWithValue("job_names", jobNames.ToArray());
        await lockQueueCmd.ExecuteNonQueryAsync(cancellationToken);

        // Lock rate limit rows to serialize concurrent claims
        await using var lockRlCmd = conn.CreateCommand();
        lockRlCmd.Transaction = tx;
        lockRlCmd.CommandText = """
                                SELECT * FROM surefire_rate_limits
                                WHERE name IN (
                                    SELECT j.rate_limit_name FROM surefire_jobs j
                                    WHERE j.name = ANY(@job_names) AND j.rate_limit_name IS NOT NULL
                                    UNION
                                    SELECT q.rate_limit_name FROM surefire_jobs j
                                    JOIN surefire_queues q ON q.name = COALESCE(j.queue, 'default')
                                    WHERE j.name = ANY(@job_names) AND q.rate_limit_name IS NOT NULL
                                )
                                FOR UPDATE
                                """;
        lockRlCmd.Parameters.AddWithValue("job_names", jobNames.ToArray());
        await lockRlCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;

        cmd.CommandText = """
                          WITH candidate AS (
                              SELECT r.id, r.job_name
                              FROM surefire_runs r
                              JOIN surefire_jobs j ON j.name = r.job_name
                              LEFT JOIN surefire_queues q ON q.name = COALESCE(j.queue, 'default')
                              WHERE r.status = 0
                                  AND r.batch_total IS NULL
                                  AND r.not_before <= NOW()
                                  AND (r.not_after IS NULL OR r.not_after > NOW())
                                  AND r.job_name = ANY(@job_names)
                                  AND COALESCE(j.queue, 'default') = ANY(@queue_names)
                                  AND COALESCE(q.is_paused, FALSE) = FALSE
                                  AND (j.max_concurrency IS NULL OR (
                                      SELECT COUNT(*) FROM surefire_runs cr
                                      WHERE cr.job_name = r.job_name AND cr.status = 1
                                  ) < j.max_concurrency)
                                  AND (q.max_concurrency IS NULL OR (
                                      SELECT COUNT(*) FROM surefire_runs cr
                                      JOIN surefire_jobs cj ON cj.name = cr.job_name
                                      WHERE COALESCE(cj.queue, 'default') = COALESCE(j.queue, 'default') AND cr.status = 1
                                  ) < q.max_concurrency)
                                  AND (j.rate_limit_name IS NULL OR (
                                      SELECT CASE
                                          WHEN rl.type = 1 THEN
                                              CASE
                                                  WHEN rl.window_start IS NULL THEN 0
                                                  WHEN EXTRACT(EPOCH FROM (NOW() - rl.window_start)) * 10000000 >= rl."window" * 2 THEN 0
                                                  WHEN EXTRACT(EPOCH FROM (NOW() - rl.window_start)) * 10000000 >= rl."window" THEN
                                                      rl.current_count * GREATEST(0, 1.0 - (EXTRACT(EPOCH FROM (NOW() - rl.window_start)) * 10000000 - rl."window") / rl."window")
                                                  ELSE
                                                      rl.current_count + rl.previous_count * GREATEST(0, 1.0 - (EXTRACT(EPOCH FROM (NOW() - rl.window_start)) * 10000000 / rl."window"))
                                              END
                                          ELSE
                                              CASE
                                                  WHEN rl.window_start IS NULL THEN 0
                                                  WHEN EXTRACT(EPOCH FROM (NOW() - rl.window_start)) * 10000000 >= rl."window" THEN 0
                                                  ELSE rl.current_count
                                              END
                                      END < rl.max_permits
                                      FROM surefire_rate_limits rl WHERE rl.name = j.rate_limit_name
                                  ) IS NOT FALSE)
                                  AND (q.rate_limit_name IS NULL OR q.rate_limit_name = j.rate_limit_name OR (
                                      SELECT CASE
                                          WHEN rl.type = 1 THEN
                                              CASE
                                                  WHEN rl.window_start IS NULL THEN 0
                                                  WHEN EXTRACT(EPOCH FROM (NOW() - rl.window_start)) * 10000000 >= rl."window" * 2 THEN 0
                                                  WHEN EXTRACT(EPOCH FROM (NOW() - rl.window_start)) * 10000000 >= rl."window" THEN
                                                      rl.current_count * GREATEST(0, 1.0 - (EXTRACT(EPOCH FROM (NOW() - rl.window_start)) * 10000000 - rl."window") / rl."window")
                                                  ELSE
                                                      rl.current_count + rl.previous_count * GREATEST(0, 1.0 - (EXTRACT(EPOCH FROM (NOW() - rl.window_start)) * 10000000 / rl."window"))
                                              END
                                          ELSE
                                              CASE
                                                  WHEN rl.window_start IS NULL THEN 0
                                                  WHEN EXTRACT(EPOCH FROM (NOW() - rl.window_start)) * 10000000 >= rl."window" THEN 0
                                                  ELSE rl.current_count
                                              END
                                      END < rl.max_permits
                                      FROM surefire_rate_limits rl WHERE rl.name = q.rate_limit_name
                                  ) IS NOT FALSE)
                              ORDER BY r.queue_priority DESC, r.priority DESC, r.not_before ASC, r.id ASC
                              LIMIT 1
                              FOR UPDATE OF r SKIP LOCKED
                          )
                          UPDATE surefire_runs SET
                              status = 1,
                              node_name = @node_name,
                              started_at = NOW(),
                              last_heartbeat_at = NOW(),
                              attempt = surefire_runs.attempt + 1
                          FROM candidate
                          WHERE surefire_runs.id = candidate.id
                          RETURNING surefire_runs.*
                          """;

        cmd.Parameters.AddWithValue("node_name", nodeName);
        cmd.Parameters.AddWithValue("job_names", jobNames.ToArray());
        cmd.Parameters.AddWithValue("queue_names", queueNames.ToArray());

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        JobRun? claimed = null;
        if (await reader.ReadAsync(cancellationToken))
        {
            claimed = ReadRun(reader);
        }

        await reader.CloseAsync();

        if (claimed is { })
        {
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

    public async Task<BatchCounters> IncrementBatchCounterAsync(string batchRunId, bool isFailed,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          UPDATE surefire_runs SET
                              batch_completed = batch_completed + CASE WHEN @is_failed = FALSE THEN 1 ELSE 0 END,
                              batch_failed = batch_failed + CASE WHEN @is_failed THEN 1 ELSE 0 END,
                              progress = CASE WHEN batch_total = 0 THEN 1.0 ELSE CAST(COALESCE(batch_completed, 0) + COALESCE(batch_failed, 0) + 1 AS DOUBLE PRECISION) / batch_total END
                          WHERE id = @id AND status NOT IN (2, 4, 5) AND batch_total IS NOT NULL
                          RETURNING batch_total, batch_completed, batch_failed
                          """;

        cmd.Parameters.AddWithValue("id", batchRunId);
        cmd.Parameters.AddWithValue("is_failed", isFailed);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new(
                reader.GetInt32(reader.GetOrdinal("batch_total")),
                reader.GetInt32(reader.GetOrdinal("batch_completed")),
                reader.GetInt32(reader.GetOrdinal("batch_failed")));
        }

        await reader.CloseAsync();

        await using var fallback = conn.CreateCommand();
        fallback.CommandText = """
                               SELECT batch_total, batch_completed, batch_failed
                               FROM surefire_runs WHERE id = @id
                               """;
        fallback.Parameters.AddWithValue("id", batchRunId);

        await using var fr = await fallback.ExecuteReaderAsync(cancellationToken);
        if (!await fr.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Run '{batchRunId}' not found.");
        }

        if (fr.IsDBNull(fr.GetOrdinal("batch_total")))
        {
            throw new InvalidOperationException($"Run '{batchRunId}' is not a batch coordinator.");
        }

        return new(
            fr.GetInt32(fr.GetOrdinal("batch_total")),
            fr.GetInt32(fr.GetOrdinal("batch_completed")),
            fr.GetInt32(fr.GetOrdinal("batch_failed")));
    }

    public async Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default)
    {
        if (events.Count == 0)
        {
            return;
        }

        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        await InsertEventsAsync(conn, tx, events, cancellationToken);

        await tx.CommitAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0,
        RunEventType[]? types = null, int? attempt = null, CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();

        var sb = new StringBuilder("SELECT * FROM surefire_events WHERE run_id = @run_id AND id > @since_id");
        cmd.Parameters.AddWithValue("run_id", runId);
        cmd.Parameters.AddWithValue("since_id", sinceId);

        if (types is { Length: > 0 })
        {
            sb.Append(" AND event_type = ANY(@types)");
            cmd.Parameters.AddWithValue("types", types.Select(t => (short)t).ToArray());
        }

        if (attempt is { })
        {
            sb.Append(" AND (attempt = @attempt OR attempt = 0)");
            cmd.Parameters.AddWithValue("attempt", attempt.Value);
        }

        sb.Append(" ORDER BY id");
        cmd.CommandText = sb.ToString();

        var results = new List<RunEvent>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadEvent(reader));
        }

        return results;
    }

    public async Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchRunId, long sinceEventId = 0,
        int take = 200, CancellationToken cancellationToken = default)
    {
        if (take <= 0)
        {
            return [];
        }

        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          SELECT e.*
                          FROM surefire_events e
                          JOIN surefire_runs r ON r.id = e.run_id
                          WHERE r.parent_run_id = @batch_run_id
                              AND e.event_type = @event_type
                              AND e.id > @since_event_id
                          ORDER BY e.id
                          LIMIT @take
                          """;
        cmd.Parameters.AddWithValue("batch_run_id", batchRunId);
        cmd.Parameters.AddWithValue("event_type", (short)RunEventType.Output);
        cmd.Parameters.AddWithValue("since_event_id", sinceEventId);
        cmd.Parameters.AddWithValue("take", take);

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
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        await using var nodeCmd = conn.CreateCommand();
        nodeCmd.Transaction = tx;
        nodeCmd.CommandText = """
                              INSERT INTO surefire_nodes (name, started_at, last_heartbeat_at, running_count, registered_job_names, registered_queue_names)
                              VALUES (@name, NOW(), NOW(), @running_count, @job_names, @queue_names)
                              ON CONFLICT (name) DO UPDATE SET
                                  last_heartbeat_at = NOW(),
                                  running_count = EXCLUDED.running_count,
                                  registered_job_names = EXCLUDED.registered_job_names,
                                  registered_queue_names = EXCLUDED.registered_queue_names
                              """;
        nodeCmd.Parameters.AddWithValue("name", nodeName);
        nodeCmd.Parameters.AddWithValue("running_count", activeRunIds.Count);
        nodeCmd.Parameters.AddWithValue("job_names", jobNames.ToArray());
        nodeCmd.Parameters.AddWithValue("queue_names", queueNames.ToArray());
        await nodeCmd.ExecuteNonQueryAsync(cancellationToken);

        if (activeRunIds.Count > 0)
        {
            await using var runCmd = conn.CreateCommand();
            runCmd.Transaction = tx;
            runCmd.CommandText = """
                                 UPDATE surefire_runs SET last_heartbeat_at = NOW()
                                 WHERE id = ANY(@ids) AND node_name = @node AND status NOT IN (2, 4, 5)
                                 """;
            runCmd.Parameters.AddWithValue("ids", activeRunIds.ToArray());
            runCmd.Parameters.AddWithValue("node", nodeName);
            await runCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await tx.CommitAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM surefire_nodes";

        var results = new List<NodeInfo>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadNode(reader));
        }

        return results;
    }

    public async Task UpsertQueueAsync(QueueDefinition queue, CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          INSERT INTO surefire_queues (name, priority, max_concurrency, rate_limit_name, last_heartbeat_at)
                          VALUES (@name, @priority, @max_concurrency, @rate_limit_name, NOW())
                          ON CONFLICT (name) DO UPDATE SET
                              priority = EXCLUDED.priority,
                              max_concurrency = EXCLUDED.max_concurrency,
                              rate_limit_name = EXCLUDED.rate_limit_name,
                              last_heartbeat_at = NOW();

                          UPDATE surefire_runs SET queue_priority = @priority
                          WHERE status = 0 AND batch_total IS NULL
                              AND job_name IN (SELECT name FROM surefire_jobs WHERE COALESCE(queue, 'default') = @name)
                              AND queue_priority != @priority;
                          """;

        cmd.Parameters.AddWithValue("name", queue.Name);
        cmd.Parameters.AddWithValue("priority", queue.Priority);
        cmd.Parameters.AddWithValue("max_concurrency",
            queue.MaxConcurrency.HasValue ? queue.MaxConcurrency.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("rate_limit_name", (object?)queue.RateLimitName ?? DBNull.Value);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT * FROM surefire_queues";

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
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE surefire_queues SET is_paused = @is_paused WHERE name = @name";
        cmd.Parameters.AddWithValue("name", name);
        cmd.Parameters.AddWithValue("is_paused", isPaused);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          INSERT INTO surefire_rate_limits (name, type, max_permits, "window", last_heartbeat_at)
                          VALUES (@name, @type, @max_permits, @window, NOW())
                          ON CONFLICT (name) DO UPDATE SET
                              type = EXCLUDED.type,
                              max_permits = EXCLUDED.max_permits,
                              "window" = EXCLUDED."window",
                              last_heartbeat_at = NOW()
                          """;

        cmd.Parameters.AddWithValue("name", rateLimit.Name);
        cmd.Parameters.AddWithValue("type", (int)rateLimit.Type);
        cmd.Parameters.AddWithValue("max_permits", rateLimit.MaxPermits);
        cmd.Parameters.AddWithValue("window", rateLimit.Window.Ticks);

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> CancelExpiredRunsAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          UPDATE surefire_runs SET
                              status = 4,
                              cancelled_at = NOW(),
                              completed_at = NOW()
                          WHERE status IN (0, 3)
                              AND batch_total IS NULL
                              AND not_after IS NOT NULL
                              AND not_after < NOW()
                          """;

        return await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task PurgeAsync(DateTimeOffset threshold, CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);

        while (true)
        {
            await using var runCmd = conn.CreateCommand();
            runCmd.CommandText = """
                                 DELETE FROM surefire_runs WHERE id IN (
                                     SELECT id FROM surefire_runs
                                     WHERE status IN (2, 4, 5)
                                         AND completed_at < @threshold
                                         AND NOT EXISTS (
                                             SELECT 1
                                             FROM surefire_runs parent
                                             WHERE parent.id = surefire_runs.parent_run_id
                                                 AND parent.batch_total IS NOT NULL
                                                 AND (
                                                     parent.status NOT IN (2, 4, 5)
                                                     OR parent.completed_at IS NULL
                                                     OR parent.completed_at >= @threshold
                                                 )
                                         )
                                     LIMIT 1000
                                 )
                                 """;
            runCmd.Parameters.AddWithValue("threshold", threshold);
            if (await runCmd.ExecuteNonQueryAsync(cancellationToken) == 0)
            {
                break;
            }
        }

        while (true)
        {
            await using var runCmd2 = conn.CreateCommand();
            runCmd2.CommandText = """
                                  DELETE FROM surefire_runs WHERE id IN (
                                      SELECT id FROM surefire_runs
                                      WHERE status IN (0, 3) AND not_before < @threshold
                                      LIMIT 1000
                                  )
                                  """;
            runCmd2.Parameters.AddWithValue("threshold", threshold);
            if (await runCmd2.ExecuteNonQueryAsync(cancellationToken) == 0)
            {
                break;
            }
        }

        await using var jobCmd = conn.CreateCommand();
        jobCmd.CommandText = """
                             DELETE FROM surefire_jobs
                             WHERE last_heartbeat_at < @threshold
                                 AND NOT EXISTS (SELECT 1 FROM surefire_runs r WHERE r.job_name = surefire_jobs.name AND r.status NOT IN (2, 4, 5))
                             """;
        jobCmd.Parameters.AddWithValue("threshold", threshold);
        await jobCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var queueCmd = conn.CreateCommand();
        queueCmd.CommandText = "DELETE FROM surefire_queues WHERE last_heartbeat_at < @threshold";
        queueCmd.Parameters.AddWithValue("threshold", threshold);
        await queueCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var rlCmd = conn.CreateCommand();
        rlCmd.CommandText = "DELETE FROM surefire_rate_limits WHERE last_heartbeat_at < @threshold";
        rlCmd.Parameters.AddWithValue("threshold", threshold);
        await rlCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var nodeCmd = conn.CreateCommand();
        nodeCmd.CommandText = "DELETE FROM surefire_nodes WHERE last_heartbeat_at < @threshold";
        nodeCmd.Parameters.AddWithValue("threshold", threshold);
        await nodeCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60,
        CancellationToken cancellationToken = default)
    {
        if (bucketMinutes <= 0)
        {
            bucketMinutes = 60;
        }

        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);

        var now = timeProvider.GetUtcNow();
        var rawSince = since ?? now.AddHours(-24);
        var sinceTime = new DateTimeOffset(rawSince.Ticks / 10 * 10, rawSince.Offset);

        await using var statsCmd = conn.CreateCommand();
        statsCmd.CommandText = """
                               SELECT
                                   (SELECT COUNT(*) FROM surefire_jobs) AS total_jobs,
                                   (SELECT COUNT(*) FROM surefire_nodes WHERE last_heartbeat_at >= @now - INTERVAL '2 minutes') AS node_count,
                                   COUNT(*) AS total_runs,
                                   COUNT(*) FILTER (WHERE status = 0) AS pending,
                                   COUNT(*) FILTER (WHERE status = 1) AS running,
                                   COUNT(*) FILTER (WHERE status = 2) AS completed,
                                   COUNT(*) FILTER (WHERE status = 3) AS retrying,
                                   COUNT(*) FILTER (WHERE status = 4) AS cancelled,
                                   COUNT(*) FILTER (WHERE status = 5) AS dead_letter
                               FROM surefire_runs
                               """;
        statsCmd.Parameters.AddWithValue("now", now);

        int totalJobs = 0, totalRuns = 0, nodeCount = 0;
        int pending = 0, running = 0, completed = 0, retrying = 0, cancelled = 0, deadLetter = 0;
        await using (var reader = await statsCmd.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                totalJobs = (int)reader.GetInt64(0);
                nodeCount = (int)reader.GetInt64(1);
                totalRuns = (int)reader.GetInt64(2);
                pending = (int)reader.GetInt64(3);
                running = (int)reader.GetInt64(4);
                completed = (int)reader.GetInt64(5);
                retrying = (int)reader.GetInt64(6);
                cancelled = (int)reader.GetInt64(7);
                deadLetter = (int)reader.GetInt64(8);
            }
        }

        var activeRuns = pending + running + retrying;
        var runsByStatus = new Dictionary<string, int>();
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

        var terminalCount = completed + cancelled + deadLetter;
        var successRate = terminalCount > 0 ? completed / (double)terminalCount : 0.0;

        await using var bucketCmd = conn.CreateCommand();
        bucketCmd.CommandText = """
                                SELECT
                                    date_bin(@interval::interval, created_at, @since) AS bucket_start,
                                    COUNT(*) FILTER (WHERE status = 0) AS pending,
                                    COUNT(*) FILTER (WHERE status = 1) AS running,
                                    COUNT(*) FILTER (WHERE status = 2) AS completed,
                                    COUNT(*) FILTER (WHERE status = 3) AS retrying,
                                    COUNT(*) FILTER (WHERE status = 4) AS cancelled,
                                    COUNT(*) FILTER (WHERE status = 5) AS dead_letter
                                FROM surefire_runs
                                WHERE created_at >= @since AND created_at < @now
                                GROUP BY bucket_start
                                ORDER BY bucket_start
                                """;
        bucketCmd.Parameters.AddWithValue("since", sinceTime);
        bucketCmd.Parameters.AddWithValue("now", now);
        bucketCmd.Parameters.AddWithValue("interval", TimeSpan.FromMinutes(bucketMinutes));

        var bucketMap = new Dictionary<DateTimeOffset, TimelineBucket>();
        await using (var reader = await bucketCmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var start = reader.GetFieldValue<DateTimeOffset>(0);
                bucketMap[start] = new()
                {
                    Start = start,
                    Pending = (int)reader.GetInt64(1),
                    Running = (int)reader.GetInt64(2),
                    Completed = (int)reader.GetInt64(3),
                    Retrying = (int)reader.GetInt64(4),
                    Cancelled = (int)reader.GetInt64(5),
                    DeadLetter = (int)reader.GetInt64(6)
                };
            }
        }

        var buckets = new List<TimelineBucket>();
        var bucketStart = sinceTime;
        var bucketSpan = TimeSpan.FromMinutes(bucketMinutes);
        while (bucketStart < now)
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
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          SELECT
                              COUNT(*) AS total_runs,
                              COUNT(*) FILTER (WHERE status = 2) AS succeeded,
                              COUNT(*) FILTER (WHERE status = 5) AS failed,
                              CASE
                                  WHEN COUNT(*) FILTER (WHERE status IN (2, 4, 5)) > 0
                                  THEN COUNT(*) FILTER (WHERE status = 2)::DOUBLE PRECISION / COUNT(*) FILTER (WHERE status IN (2, 4, 5))
                                  ELSE 0
                              END AS success_rate,
                              AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) FILTER (WHERE status = 2 AND started_at IS NOT NULL AND completed_at IS NOT NULL) AS avg_duration_secs,
                              MAX(started_at) AS last_run_at
                          FROM surefire_runs WHERE job_name = @job_name
                          """;
        cmd.Parameters.AddWithValue("job_name", jobName);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);

        return new()
        {
            TotalRuns = (int)reader.GetInt64(0),
            SucceededRuns = (int)reader.GetInt64(1),
            FailedRuns = (int)reader.GetInt64(2),
            SuccessRate = reader.GetDouble(3),
            AvgDuration = !reader.IsDBNull(4)
                ? TimeSpan.FromSeconds(reader.GetDouble(4))
                : null,
            LastRunAt = !reader.IsDBNull(5)
                ? reader.GetFieldValue<DateTimeOffset>(5)
                : null
        };
    }

    public async Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
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
                              COALESCE(pending.cnt, 0) AS pending_count,
                              COALESCE(running.cnt, 0) AS running_count
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
                PendingCount = (int)reader.GetInt64(1),
                RunningCount = (int)reader.GetInt64(2)
            };
        }

        return results;
    }

    private async Task CreateRunsCoreAsync(IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        if (runs.Count == 0 && (initialEvents is null || initialEvents.Count == 0))
        {
            return;
        }

        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        const int paramsPerRun = 26;
        const int maxParams = 65535;
        var chunkSize = maxParams / paramsPerRun;

        for (var offset = 0; offset < runs.Count; offset += chunkSize)
        {
            var chunk = runs.Skip(offset).Take(chunkSize).ToList();
            var sb = new StringBuilder();
            sb.Append("""
                      INSERT INTO surefire_runs (
                          id, job_name, status, arguments, result, error, progress,
                          created_at, started_at, completed_at, cancelled_at, node_name,
                          attempt, trace_id, span_id, parent_run_id, root_run_id,
                          rerun_of_run_id, not_before, not_after, priority, deduplication_id,
                          last_heartbeat_at, queue_priority, batch_total, batch_completed, batch_failed
                      ) VALUES
                      """);

            await using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;

            for (var i = 0; i < chunk.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(',');
                }

                var p = $"p{i}_";
                sb.Append($"""
                           (
                               @{p}id, @{p}job_name, @{p}status, @{p}arguments, @{p}result, @{p}error, @{p}progress,
                               @{p}created_at, @{p}started_at, @{p}completed_at, @{p}cancelled_at, @{p}node_name,
                               @{p}attempt, @{p}trace_id, @{p}span_id, @{p}parent_run_id, @{p}root_run_id,
                               @{p}rerun_of_run_id, @{p}not_before, @{p}not_after, @{p}priority, @{p}deduplication_id,
                               @{p}last_heartbeat_at,
                               COALESCE((SELECT q.priority FROM surefire_queues q WHERE q.name = COALESCE((SELECT j.queue FROM surefire_jobs j WHERE j.name = @{p}job_name), 'default')), 0),
                               @{p}batch_total, @{p}batch_completed, @{p}batch_failed
                           )
                           """);

                var run = chunk[i];
                AddRunParams(cmd, p, run);
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
        await using var conn = await options.DataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);
        await using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;

        if (maxActiveForJob is { })
        {
            await using var lockCmd = conn.CreateCommand();
            lockCmd.Transaction = tx;
            lockCmd.CommandText = "SELECT 1 FROM surefire_jobs WHERE name = @name FOR UPDATE";
            lockCmd.Parameters.AddWithValue("name", run.JobName);
            await lockCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        var conditions = new List<string>();

        if (run.DeduplicationId is { })
        {
            conditions.Add("""
                           NOT EXISTS (
                               SELECT 1 FROM surefire_runs
                               WHERE job_name = @job_name AND deduplication_id = @dedup_id AND status NOT IN (2, 4, 5)
                           )
                           """);
            cmd.Parameters.AddWithValue("dedup_id", run.DeduplicationId);
        }

        if (maxActiveForJob is { })
        {
            conditions.Add("""
                           (SELECT COUNT(*) FROM surefire_runs
                            WHERE job_name = @job_name AND status NOT IN (2, 4, 5)) < @max_active
                           """);
            cmd.Parameters.AddWithValue("max_active", maxActiveForJob.Value);
            conditions.Add("""
                           COALESCE((SELECT is_enabled FROM surefire_jobs WHERE name = @job_name), TRUE) = TRUE
                           """);
        }

        if (conditions.Count == 0)
        {
            cmd.CommandText = """
                              INSERT INTO surefire_runs (
                                  id, job_name, status, arguments, result, error, progress,
                                  created_at, started_at, completed_at, cancelled_at, node_name,
                                  attempt, trace_id, span_id, parent_run_id, root_run_id,
                                  rerun_of_run_id, not_before, not_after, priority, deduplication_id,
                                  last_heartbeat_at, queue_priority, batch_total, batch_completed, batch_failed
                              ) VALUES (
                                  @id, @job_name, @status, @arguments, @result, @error, @progress,
                                  @created_at, @started_at, @completed_at, @cancelled_at, @node_name,
                                  @attempt, @trace_id, @span_id, @parent_run_id, @root_run_id,
                                  @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id,
                                  @last_heartbeat_at,
                                  COALESCE((SELECT q.priority FROM surefire_queues q WHERE q.name = COALESCE((SELECT j.queue FROM surefire_jobs j WHERE j.name = @job_name), 'default')), 0),
                                  @batch_total, @batch_completed, @batch_failed
                              )
                              ON CONFLICT DO NOTHING
                              """;
        }
        else
        {
            var whereClause = string.Join(" AND ", conditions);
            cmd.CommandText = $"""
                               INSERT INTO surefire_runs (
                                   id, job_name, status, arguments, result, error, progress,
                                   created_at, started_at, completed_at, cancelled_at, node_name,
                                   attempt, trace_id, span_id, parent_run_id, root_run_id,
                                   rerun_of_run_id, not_before, not_after, priority, deduplication_id,
                                   last_heartbeat_at, queue_priority, batch_total, batch_completed, batch_failed
                               )
                               SELECT
                                   @id, @job_name, @status, @arguments, @result, @error, @progress,
                                   @created_at, @started_at, @completed_at, @cancelled_at, @node_name,
                                   @attempt, @trace_id, @span_id, @parent_run_id, @root_run_id,
                                   @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id,
                                   @last_heartbeat_at,
                                   COALESCE((SELECT q.priority FROM surefire_queues q WHERE q.name = COALESCE((SELECT j.queue FROM surefire_jobs j WHERE j.name = @job_name), 'default')), 0),
                                   @batch_total, @batch_completed, @batch_failed
                               WHERE {whereClause}
                               ON CONFLICT DO NOTHING
                               """;
        }

        AddRunParams(cmd, "", run);

        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);

        if (rows > 0)
        {
            await InsertEventsAsync(conn, tx, initialEvents, cancellationToken);
        }

        if (rows > 0 && lastCronFireAt is { } fireAt)
        {
            await using var updateCmd = conn.CreateCommand();
            updateCmd.Transaction = tx;
            updateCmd.CommandText =
                "UPDATE surefire_jobs SET last_cron_fire_at = @last_cron_fire_at WHERE name = @job_name";
            updateCmd.Parameters.AddWithValue("last_cron_fire_at", fireAt);
            updateCmd.Parameters.AddWithValue("job_name", run.JobName);
            await updateCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await tx.CommitAsync(cancellationToken);
        return rows > 0;
    }

    private static async Task InsertEventsAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        IReadOnlyList<RunEvent>? events, CancellationToken cancellationToken)
    {
        if (events is null || events.Count == 0)
        {
            return;
        }

        const int paramsPerEvent = 5;
        const int maxParams = 65535;
        var chunkSize = maxParams / paramsPerEvent;

        for (var offset = 0; offset < events.Count; offset += chunkSize)
        {
            var chunk = events.Skip(offset).Take(chunkSize).ToList();
            var sb = new StringBuilder();
            sb.Append("INSERT INTO surefire_events (run_id, event_type, payload, created_at, attempt) VALUES ");

            await using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            for (var i = 0; i < chunk.Count; i++)
            {
                if (i > 0)
                {
                    sb.Append(',');
                }

                sb.Append($"(@run_id_{i}, @event_type_{i}, @payload_{i}, @created_at_{i}, @attempt_{i})");
                cmd.Parameters.AddWithValue($"run_id_{i}", chunk[i].RunId);
                cmd.Parameters.AddWithValue($"event_type_{i}", (short)chunk[i].EventType);
                cmd.Parameters.AddWithValue($"payload_{i}", chunk[i].Payload);
                cmd.Parameters.AddWithValue($"created_at_{i}", chunk[i].CreatedAt);
                cmd.Parameters.AddWithValue($"attempt_{i}", chunk[i].Attempt);
            }

            cmd.CommandText = sb.ToString();
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static void BuildRunFilterWhere(RunFilter filter, List<string> parts, NpgsqlCommand cmd)
    {
        if (filter.Status is { })
        {
            parts.Add("status = @filter_status");
            cmd.Parameters.AddWithValue("filter_status", (int)filter.Status.Value);
        }

        if (filter.JobName is { })
        {
            if (filter.ExactJobName)
            {
                parts.Add("job_name = @filter_job_name");
                cmd.Parameters.AddWithValue("filter_job_name", filter.JobName);
            }
            else
            {
                parts.Add("job_name ILIKE '%' || @filter_job_name || '%' ESCAPE '\\'");
                cmd.Parameters.AddWithValue("filter_job_name", EscapeLike(filter.JobName));
            }
        }

        if (filter.ParentRunId is { })
        {
            parts.Add("parent_run_id = @filter_parent");
            cmd.Parameters.AddWithValue("filter_parent", filter.ParentRunId);
        }

        if (filter.RootRunId is { })
        {
            parts.Add("root_run_id = @filter_root");
            cmd.Parameters.AddWithValue("filter_root", filter.RootRunId);
        }

        if (filter.NodeName is { })
        {
            parts.Add("node_name = @filter_node");
            cmd.Parameters.AddWithValue("filter_node", filter.NodeName);
        }

        if (filter.CreatedAfter is { })
        {
            parts.Add("created_at > @filter_created_after");
            cmd.Parameters.AddWithValue("filter_created_after", filter.CreatedAfter.Value);
        }

        if (filter.CreatedBefore is { })
        {
            parts.Add("created_at < @filter_created_before");
            cmd.Parameters.AddWithValue("filter_created_before", filter.CreatedBefore.Value);
        }

        if (filter.CompletedAfter is { })
        {
            parts.Add("completed_at >= @filter_completed_after");
            cmd.Parameters.AddWithValue("filter_completed_after", filter.CompletedAfter.Value);
        }

        if (filter.LastHeartbeatBefore is { })
        {
            parts.Add("last_heartbeat_at < @filter_hb_before");
            cmd.Parameters.AddWithValue("filter_hb_before", filter.LastHeartbeatBefore.Value);
        }

        if (filter.IsBatchCoordinator is { })
        {
            parts.Add(filter.IsBatchCoordinator.Value
                ? "batch_total IS NOT NULL"
                : "batch_total IS NULL");
        }

        if (filter.IsTerminal is { })
        {
            parts.Add(filter.IsTerminal.Value
                ? "status IN (2, 4, 5)"
                : "status NOT IN (2, 4, 5)");
        }
    }

    private static void AddRunParams(NpgsqlCommand cmd, string prefix, JobRun run)
    {
        cmd.Parameters.AddWithValue($"{prefix}id", run.Id);
        cmd.Parameters.AddWithValue($"{prefix}job_name", run.JobName);
        cmd.Parameters.AddWithValue($"{prefix}status", (int)run.Status);
        cmd.Parameters.AddWithValue($"{prefix}arguments", (object?)run.Arguments ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}result", (object?)run.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}error", (object?)run.Error ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}progress", run.Progress);
        cmd.Parameters.AddWithValue($"{prefix}created_at", run.CreatedAt);
        cmd.Parameters.AddWithValue($"{prefix}started_at", run.StartedAt.HasValue ? run.StartedAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}completed_at",
            run.CompletedAt.HasValue ? run.CompletedAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}cancelled_at",
            run.CancelledAt.HasValue ? run.CancelledAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}node_name", (object?)run.NodeName ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}attempt", run.Attempt);
        cmd.Parameters.AddWithValue($"{prefix}trace_id", (object?)run.TraceId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}span_id", (object?)run.SpanId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}parent_run_id", (object?)run.ParentRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}root_run_id", (object?)run.RootRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}rerun_of_run_id", (object?)run.RerunOfRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}not_before", run.NotBefore);
        cmd.Parameters.AddWithValue($"{prefix}not_after", run.NotAfter.HasValue ? run.NotAfter.Value : DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}priority", run.Priority);
        cmd.Parameters.AddWithValue($"{prefix}deduplication_id", (object?)run.DeduplicationId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}last_heartbeat_at",
            run.LastHeartbeatAt.HasValue ? run.LastHeartbeatAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}batch_total",
            run.BatchTotal.HasValue ? run.BatchTotal.Value : DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}batch_completed", run.BatchCompleted ?? 0);
        cmd.Parameters.AddWithValue($"{prefix}batch_failed", run.BatchFailed ?? 0);
    }

    private static JobRun ReadRun(NpgsqlDataReader reader)
    {
        var run = new JobRun
        {
            Id = reader.GetString(reader.GetOrdinal("id")),
            JobName = reader.GetString(reader.GetOrdinal("job_name")),
            Status = (JobStatus)reader.GetInt32(reader.GetOrdinal("status")),
            Progress = reader.GetDouble(reader.GetOrdinal("progress")),
            CreatedAt = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("created_at")),
            Attempt = reader.GetInt32(reader.GetOrdinal("attempt")),
            NotBefore = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("not_before")),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            QueuePriority = reader.GetInt32(reader.GetOrdinal("queue_priority"))
        };

        var col = reader.GetOrdinal("arguments");
        if (!reader.IsDBNull(col))
        {
            run.Arguments = reader.GetString(col);
        }

        col = reader.GetOrdinal("result");
        if (!reader.IsDBNull(col))
        {
            run.Result = reader.GetString(col);
        }

        col = reader.GetOrdinal("error");
        if (!reader.IsDBNull(col))
        {
            run.Error = reader.GetString(col);
        }

        col = reader.GetOrdinal("started_at");
        if (!reader.IsDBNull(col))
        {
            run.StartedAt = reader.GetFieldValue<DateTimeOffset>(col);
        }

        col = reader.GetOrdinal("completed_at");
        if (!reader.IsDBNull(col))
        {
            run.CompletedAt = reader.GetFieldValue<DateTimeOffset>(col);
        }

        col = reader.GetOrdinal("cancelled_at");
        if (!reader.IsDBNull(col))
        {
            run.CancelledAt = reader.GetFieldValue<DateTimeOffset>(col);
        }

        col = reader.GetOrdinal("node_name");
        if (!reader.IsDBNull(col))
        {
            run.NodeName = reader.GetString(col);
        }

        col = reader.GetOrdinal("trace_id");
        if (!reader.IsDBNull(col))
        {
            run.TraceId = reader.GetString(col);
        }

        col = reader.GetOrdinal("span_id");
        if (!reader.IsDBNull(col))
        {
            run.SpanId = reader.GetString(col);
        }

        col = reader.GetOrdinal("parent_run_id");
        if (!reader.IsDBNull(col))
        {
            run.ParentRunId = reader.GetString(col);
        }

        col = reader.GetOrdinal("root_run_id");
        if (!reader.IsDBNull(col))
        {
            run.RootRunId = reader.GetString(col);
        }

        col = reader.GetOrdinal("rerun_of_run_id");
        if (!reader.IsDBNull(col))
        {
            run.RerunOfRunId = reader.GetString(col);
        }

        col = reader.GetOrdinal("not_after");
        if (!reader.IsDBNull(col))
        {
            run.NotAfter = reader.GetFieldValue<DateTimeOffset>(col);
        }

        col = reader.GetOrdinal("deduplication_id");
        if (!reader.IsDBNull(col))
        {
            run.DeduplicationId = reader.GetString(col);
        }

        col = reader.GetOrdinal("last_heartbeat_at");
        if (!reader.IsDBNull(col))
        {
            run.LastHeartbeatAt = reader.GetFieldValue<DateTimeOffset>(col);
        }

        col = reader.GetOrdinal("batch_total");
        if (!reader.IsDBNull(col))
        {
            run.BatchTotal = reader.GetInt32(col);
            run.BatchCompleted = reader.GetInt32(reader.GetOrdinal("batch_completed"));
            run.BatchFailed = reader.GetInt32(reader.GetOrdinal("batch_failed"));
        }

        return run;
    }

    private static JobDefinition ReadJob(NpgsqlDataReader reader)
    {
        var job = new JobDefinition
        {
            Name = reader.GetString(reader.GetOrdinal("name")),
            Tags = reader.GetFieldValue<string[]>(reader.GetOrdinal("tags")),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            IsContinuous = reader.GetBoolean(reader.GetOrdinal("is_continuous")),
            IsEnabled = reader.GetBoolean(reader.GetOrdinal("is_enabled")),
            MisfirePolicy = (MisfirePolicy)reader.GetInt32(reader.GetOrdinal("misfire_policy"))
        };

        var col = reader.GetOrdinal("description");
        if (!reader.IsDBNull(col))
        {
            job.Description = reader.GetString(col);
        }

        col = reader.GetOrdinal("cron_expression");
        if (!reader.IsDBNull(col))
        {
            job.CronExpression = reader.GetString(col);
        }

        col = reader.GetOrdinal("time_zone_id");
        if (!reader.IsDBNull(col))
        {
            job.TimeZoneId = reader.GetString(col);
        }

        col = reader.GetOrdinal("timeout");
        if (!reader.IsDBNull(col))
        {
            job.Timeout = TimeSpan.FromTicks(reader.GetInt64(col));
        }

        col = reader.GetOrdinal("max_concurrency");
        if (!reader.IsDBNull(col))
        {
            job.MaxConcurrency = reader.GetInt32(col);
        }

        col = reader.GetOrdinal("retry_policy");
        if (!reader.IsDBNull(col))
        {
            job.RetryPolicy = DeserializeRetryPolicy(reader.GetString(col));
        }

        col = reader.GetOrdinal("queue");
        if (!reader.IsDBNull(col))
        {
            job.Queue = reader.GetString(col);
        }

        col = reader.GetOrdinal("rate_limit_name");
        if (!reader.IsDBNull(col))
        {
            job.RateLimitName = reader.GetString(col);
        }

        col = reader.GetOrdinal("arguments_schema");
        if (!reader.IsDBNull(col))
        {
            job.ArgumentsSchema = reader.GetString(col);
        }

        col = reader.GetOrdinal("last_heartbeat_at");
        if (!reader.IsDBNull(col))
        {
            job.LastHeartbeatAt = reader.GetFieldValue<DateTimeOffset>(col);
        }

        col = reader.GetOrdinal("last_cron_fire_at");
        if (!reader.IsDBNull(col))
        {
            job.LastCronFireAt = reader.GetFieldValue<DateTimeOffset>(col);
        }

        return job;
    }

    private static QueueDefinition ReadQueue(NpgsqlDataReader reader)
    {
        var queue = new QueueDefinition
        {
            Name = reader.GetString(reader.GetOrdinal("name")),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            IsPaused = reader.GetBoolean(reader.GetOrdinal("is_paused"))
        };

        var col = reader.GetOrdinal("max_concurrency");
        if (!reader.IsDBNull(col))
        {
            queue.MaxConcurrency = reader.GetInt32(col);
        }

        col = reader.GetOrdinal("rate_limit_name");
        if (!reader.IsDBNull(col))
        {
            queue.RateLimitName = reader.GetString(col);
        }

        col = reader.GetOrdinal("last_heartbeat_at");
        if (!reader.IsDBNull(col))
        {
            queue.LastHeartbeatAt = reader.GetFieldValue<DateTimeOffset>(col);
        }

        return queue;
    }

    private static NodeInfo ReadNode(NpgsqlDataReader reader) => new()
    {
        Name = reader.GetString(reader.GetOrdinal("name")),
        StartedAt = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("started_at")),
        LastHeartbeatAt = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("last_heartbeat_at")),
        RunningCount = reader.GetInt32(reader.GetOrdinal("running_count")),
        RegisteredJobNames = reader.GetFieldValue<string[]>(reader.GetOrdinal("registered_job_names")),
        RegisteredQueueNames = reader.GetFieldValue<string[]>(reader.GetOrdinal("registered_queue_names"))
    };

    private static RunEvent ReadEvent(NpgsqlDataReader reader) => new()
    {
        Id = reader.GetInt64(reader.GetOrdinal("id")),
        RunId = reader.GetString(reader.GetOrdinal("run_id")),
        EventType = (RunEventType)reader.GetInt16(reader.GetOrdinal("event_type")),
        Payload = reader.GetString(reader.GetOrdinal("payload")),
        CreatedAt = reader.GetFieldValue<DateTimeOffset>(reader.GetOrdinal("created_at")),
        Attempt = reader.GetInt32(reader.GetOrdinal("attempt"))
    };

    private async Task<JobDefinition?> GetJobInternalAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        string name, CancellationToken cancellationToken)
    {
        await using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "SELECT * FROM surefire_jobs WHERE name = @name";
        cmd.Parameters.AddWithValue("name", name);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return ReadJob(reader);
    }

    private async Task<QueueDefinition?> GetQueueInternalAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        string name, CancellationToken cancellationToken)
    {
        await using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = "SELECT * FROM surefire_queues WHERE name = @name";
        cmd.Parameters.AddWithValue("name", name);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return ReadQueue(reader);
    }

    private async Task AcquireRateLimitAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        string rateLimitName, CancellationToken cancellationToken)
    {
        await using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = """
                          UPDATE surefire_rate_limits SET
                              previous_count = CASE
                                  WHEN window_start IS NULL THEN 0
                                  WHEN EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 >= "window" * 2 THEN 0
                                  WHEN EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 >= "window" THEN current_count
                                  ELSE previous_count
                              END,
                              current_count = CASE
                                  WHEN window_start IS NULL THEN 1
                                  WHEN EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 >= "window" THEN 1
                                  ELSE current_count + 1
                              END,
                              window_start = CASE
                                  WHEN window_start IS NULL THEN NOW()
                                  WHEN EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 >= "window" * 2 THEN
                                      window_start + (FLOOR(EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 / "window") * "window" / 10000000.0) * INTERVAL '1 second'
                                  WHEN EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 >= "window" THEN
                                      window_start + ("window" / 10000000.0) * INTERVAL '1 second'
                                  ELSE window_start
                              END
                          WHERE name = @name
                          """;
        cmd.Parameters.AddWithValue("name", rateLimitName);
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

    private static string EscapeLike(string input) =>
        input.Replace(@"\", @"\\").Replace("%", @"\%").Replace("_", @"\_");
}