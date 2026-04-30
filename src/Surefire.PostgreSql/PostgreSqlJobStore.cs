using System.Text;
using System.Text.Json;
using Npgsql;

namespace Surefire.PostgreSql;

/// <summary>
///     PostgreSQL implementation of <see cref="IJobStore" />.
/// </summary>
internal sealed class PostgreSqlJobStore(
    NpgsqlDataSource dataSource,
    TimeSpan? commandTimeout,
    TimeProvider timeProvider) : IJobStore
{
    private static readonly TimeSpan MigrationLockRetryDelay = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan MigrationLockWaitTimeout = TimeSpan.FromSeconds(30);
    private OrdinalCache<BatchOrdinals>? _batchOrdinals;
    private OrdinalCache<EventOrdinals>? _eventOrdinals;
    private OrdinalCache<NodeOrdinals>? _nodeOrdinals;

    // GetOrdinal is a linear scan on NpgsqlDataReader; caching removes O(columns) per row.
    // Wrapped in a reference so concurrent callers can't observe a torn struct.
    private OrdinalCache<RunOrdinals>? _runOrdinals;

    internal int? CommandTimeoutSeconds { get; } =
        CommandTimeouts.ToSeconds(commandTimeout, nameof(commandTimeout));

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);

        await AcquireMigrationLockAsync(conn, cancellationToken);

        try
        {
            await using var migCmd = CreateCommand(conn);
            migCmd.CommandText =
                "CREATE TABLE IF NOT EXISTS surefire_schema_migrations (version INT NOT NULL PRIMARY KEY)";
            await migCmd.ExecuteNonQueryAsync(cancellationToken);

            await using var checkCmd = CreateCommand(conn);
            checkCmd.CommandText = "SELECT COALESCE(MAX(version), 0) FROM surefire_schema_migrations";
            var currentVersion = Convert.ToInt32(await checkCmd.ExecuteScalarAsync(cancellationToken));
            if (currentVersion >= 1)
            {
                await ReleaseMigrationLockAsync(conn);
                return;
            }

            await using var cmd = CreateCommand(conn);
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
                                  fire_all_limit INT,
                                  arguments_schema TEXT,
                                  last_heartbeat_at TIMESTAMPTZ,
                                  last_cron_fire_at TIMESTAMPTZ,
                                  running_count INT NOT NULL DEFAULT 0,
                                  non_terminal_count INT NOT NULL DEFAULT 0
                              );

                              CREATE TABLE IF NOT EXISTS surefire_runs (
                                  id TEXT PRIMARY KEY,
                                  job_name TEXT NOT NULL,
                                  status INT NOT NULL DEFAULT 0,
                                  arguments TEXT,
                                  result TEXT,
                                  reason TEXT,
                                  progress DOUBLE PRECISION NOT NULL DEFAULT 0,
                                  created_at TIMESTAMPTZ NOT NULL,
                                  started_at TIMESTAMPTZ,
                                  completed_at TIMESTAMPTZ,
                                  canceled_at TIMESTAMPTZ,
                                  node_name TEXT,
                                  attempt INT NOT NULL DEFAULT 0,
                                  trace_id TEXT,
                                  span_id TEXT,
                                  parent_trace_id TEXT,
                                  parent_span_id TEXT,
                                  parent_run_id TEXT,
                                  root_run_id TEXT,
                                  rerun_of_run_id TEXT,
                                  not_before TIMESTAMPTZ NOT NULL,
                                  not_after TIMESTAMPTZ,
                                  priority INT NOT NULL DEFAULT 0,
                                  deduplication_id TEXT,
                                  last_heartbeat_at TIMESTAMPTZ,
                                  batch_id TEXT
                              );

                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_claim
                                  ON surefire_runs (priority DESC, not_before, id)
                                  WHERE status = 0;

                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_batch_id
                                  ON surefire_runs (batch_id)
                                  WHERE batch_id IS NOT NULL;

                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_root
                                  ON surefire_runs (root_run_id)
                                  WHERE root_run_id IS NOT NULL;

                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_parent
                                  ON surefire_runs (parent_run_id, created_at, id)
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

                              -- Backs GetStaleRunningRunIdsAsync: oldest-heartbeat-first range scan
                              -- over Running rows, bounded by the result size.
                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_stale_heartbeat
                                  ON surefire_runs (last_heartbeat_at)
                                  WHERE status = 1;

                              -- Backs CancelExpiredRunsWithIdsAsync: range seek over pending runs
                              -- with a deadline, bounded by expired count rather than total pending.
                              CREATE INDEX IF NOT EXISTS ix_surefire_runs_expiring
                                  ON surefire_runs (not_after)
                                  WHERE status = 0 AND not_after IS NOT NULL;

                              CREATE TABLE IF NOT EXISTS surefire_batches (
                                  id TEXT NOT NULL PRIMARY KEY,
                                  status SMALLINT NOT NULL DEFAULT 0,
                                  total INT NOT NULL DEFAULT 0,
                                  succeeded INT NOT NULL DEFAULT 0,
                                  failed INT NOT NULL DEFAULT 0,
                                  canceled INT NOT NULL DEFAULT 0,
                                  created_at TIMESTAMPTZ NOT NULL,
                                  completed_at TIMESTAMPTZ
                              );

                              CREATE TABLE IF NOT EXISTS surefire_events (
                                  id BIGSERIAL PRIMARY KEY,
                                  run_id TEXT NOT NULL,
                                  event_type SMALLINT NOT NULL,
                                  payload TEXT NOT NULL,
                                  created_at TIMESTAMPTZ NOT NULL,
                                  attempt INT NOT NULL DEFAULT 1,
                                  -- Stamped with the inserting transaction's xid8 so event-tail readers
                                  -- can clamp paging to a commit-stable horizon. Postgres allocates `id`
                                  -- (BIGSERIAL) at INSERT time but the row only becomes visible at COMMIT,
                                  -- so concurrent writers commit out of id order. A naive `id > @since`
                                  -- cursor advances past in-flight low ids and silently skips them when
                                  -- they finally commit. By recording xact_id and excluding any read at or
                                  -- past the lowest in-flight transaction's first row id, we guarantee the
                                  -- cursor only crosses rows whose entire commit prefix is visible.
                                  xact_id XID8 NOT NULL DEFAULT pg_current_xact_id(),
                                  FOREIGN KEY (run_id) REFERENCES surefire_runs(id) ON DELETE CASCADE
                              );

                              CREATE INDEX IF NOT EXISTS ix_surefire_events_run
                                  ON surefire_events (run_id, id);

                              -- Supports the in-flight horizon probe used by event-tail readers:
                              -- SELECT MIN(id) FROM surefire_events WHERE xact_id >= pg_snapshot_xmin(...).
                              CREATE INDEX IF NOT EXISTS ix_surefire_events_xact_id
                                  ON surefire_events (xact_id, id);

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
                                  last_heartbeat_at TIMESTAMPTZ,
                                  running_count INT NOT NULL DEFAULT 0
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

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT 1";
        _ = await cmd.ExecuteScalarAsync(cancellationToken);
    }

    public async Task UpsertJobsAsync(IReadOnlyList<JobDefinition> jobs,
        CancellationToken cancellationToken = default)
    {
        if (jobs.Count == 0)
        {
            return;
        }

        // One JSON payload, one jsonb_array_elements, one INSERT...ON CONFLICT, one round trip.
        // is_enabled and last_cron_fire_at are omitted from DO UPDATE SET so existing rows
        // preserve them; the input value (or NULL) only applies on first insert.
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          INSERT INTO surefire_jobs (
                              name, description, tags, cron_expression, time_zone_id, timeout,
                              max_concurrency, priority, retry_policy, is_continuous, queue,
                              rate_limit_name, is_enabled, misfire_policy, fire_all_limit, arguments_schema,
                              last_heartbeat_at
                          )
                          SELECT
                              e->>'name',
                              e->>'description',
                              ARRAY(SELECT jsonb_array_elements_text(e->'tags')),
                              e->>'cronExpression',
                              e->>'timeZoneId',
                              (e->>'timeout')::bigint,
                              (e->>'maxConcurrency')::int,
                              (e->>'priority')::int,
                              e->'retryPolicy',
                              (e->>'isContinuous')::boolean,
                              e->>'queue',
                              e->>'rateLimitName',
                              (e->>'isEnabled')::boolean,
                              (e->>'misfirePolicy')::int,
                              (e->>'fireAllLimit')::int,
                              e->>'argumentsSchema',
                              NOW()
                          FROM jsonb_array_elements(@payload::jsonb) AS e
                          ORDER BY e->>'name'
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
                              fire_all_limit = EXCLUDED.fire_all_limit,
                              arguments_schema = EXCLUDED.arguments_schema,
                              last_heartbeat_at = NOW()
                          """;
        cmd.Parameters.AddWithValue("payload", UpsertPayloadFactory.SerializeJobs(jobs));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
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
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);

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
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "UPDATE surefire_jobs SET is_enabled = @enabled WHERE name = @name";
        cmd.Parameters.AddWithValue("name", name);
        cmd.Parameters.AddWithValue("enabled", enabled);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateLastCronFireAtAsync(string jobName, DateTimeOffset fireAt,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "UPDATE surefire_jobs SET last_cron_fire_at = @fire_at WHERE name = @name";
        cmd.Parameters.AddWithValue("name", jobName);
        cmd.Parameters.AddWithValue("fire_at", fireAt);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public Task CreateRunsAsync(IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
        => CreateRunsAsyncCore(runs, initialEvents, cancellationToken);

    public Task<bool> TryCreateRunAsync(JobRun run, int? maxActiveForJob = null,
        DateTimeOffset? lastCronFireAt = null,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
        => TryCreateRunAsyncCore(run, maxActiveForJob, lastCronFireAt, initialEvents, cancellationToken);

    public async Task<JobRun?> GetRunAsync(string id, CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM surefire_runs WHERE id = @id";
        cmd.Parameters.AddWithValue("id", id);

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

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM surefire_runs WHERE id = ANY(@ids)";
        cmd.Parameters.AddWithValue("ids", ids.ToArray());

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
                  SELECT * FROM surefire_runs
                  WHERE parent_run_id = @parent
                    AND (created_at, id) < (@cts, @cid)
                  ORDER BY created_at DESC, id DESC
                  LIMIT @take
                  """;
        }
        else if (after is { })
        {
            sql = """
                  SELECT * FROM surefire_runs
                  WHERE parent_run_id = @parent
                    AND (created_at, id) > (@cts, @cid)
                  ORDER BY created_at, id
                  LIMIT @take
                  """;
        }
        else
        {
            sql = """
                  SELECT * FROM surefire_runs
                  WHERE parent_run_id = @parent
                  ORDER BY created_at, id
                  LIMIT @take
                  """;
        }

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("parent", parentRunId);
        // take+1 lookahead: NextCursor is non-null iff a row exists beyond the page boundary.
        cmd.Parameters.AddWithValue("take", take + 1);
        if ((after ?? before) is { } c)
        {
            cmd.Parameters.AddWithValue("cts", c.CreatedAt);
            cmd.Parameters.AddWithValue("cid", c.Id);
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
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        // Parent IDs are immutable; recursion terminates when parent_run_id is null.
        cmd.CommandText = """
                          WITH RECURSIVE ancestors(depth, id) AS (
                              SELECT 0, parent_run_id FROM surefire_runs WHERE id = @id
                              UNION ALL
                              SELECT a.depth + 1, r.parent_run_id
                              FROM ancestors a
                              JOIN surefire_runs r ON r.id = a.id
                              WHERE a.id IS NOT NULL
                          )
                          SELECT r.* FROM ancestors a
                          JOIN surefire_runs r ON r.id = a.id
                          WHERE a.id IS NOT NULL
                          ORDER BY a.depth DESC
                          """;
        cmd.Parameters.AddWithValue("id", runId);
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

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);

        var whereParts = new List<string>();
        await using var countCmd = CreateCommand(conn);
        BuildRunFilterWhere(filter, whereParts, countCmd);
        var whereClause = whereParts.Count > 0 ? "WHERE " + string.Join(" AND ", whereParts) : "";

        countCmd.CommandText = $"SELECT COUNT(*) FROM surefire_runs {whereClause}";
        var totalCount = (int)(long)(await countCmd.ExecuteScalarAsync(cancellationToken))!;

        if (totalCount == 0 || skip >= totalCount)
        {
            return new() { Items = [], TotalCount = totalCount };
        }

        var orderBy = filter.OrderBy switch
        {
            RunOrderBy.StartedAt => "started_at DESC NULLS LAST, id DESC",
            RunOrderBy.CompletedAt => "completed_at DESC NULLS LAST, id DESC",
            _ => "created_at DESC, id DESC"
        };

        await using var cmd = CreateCommand(conn);
        BuildRunFilterWhere(filter, [], cmd);
        cmd.CommandText =
            $"SELECT * FROM surefire_runs {whereClause} ORDER BY {orderBy} LIMIT @take OFFSET @skip";
        cmd.Parameters.AddWithValue("take", take);
        cmd.Parameters.AddWithValue("skip", skip);

        var items = new List<JobRun>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(ReadRun(reader));
        }

        return new() { Items = items, TotalCount = totalCount };
    }

    public async Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          UPDATE surefire_runs SET
                              progress = @progress,
                              result = @result,
                              reason = @reason,
                              trace_id = @trace_id,
                              span_id = @span_id,
                              last_heartbeat_at = @last_heartbeat_at
                          WHERE id = @id AND node_name IS NOT DISTINCT FROM @node_name AND status NOT IN (2, 4, 5)
                          """;

        cmd.Parameters.AddWithValue("id", run.Id);
        cmd.Parameters.AddWithValue("progress", run.Progress);
        cmd.Parameters.AddWithValue("result", (object?)run.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("reason", (object?)run.Reason ?? DBNull.Value);
        cmd.Parameters.AddWithValue("trace_id", (object?)run.TraceId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("span_id", (object?)run.SpanId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("last_heartbeat_at",
            run.LastHeartbeatAt.HasValue ? run.LastHeartbeatAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("node_name", (object?)run.NodeName ?? DBNull.Value);

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

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        // Pre-lock the run's job and queue rows in canonical order (jobs, then queues) so we
        // don't deadlock with concurrent claims or enqueues. A separate statement is required
        // because Postgres doesn't guarantee execution order between sibling DML CTEs.
        // INNER JOIN: FOR UPDATE can't apply to the nullable side of a LEFT JOIN. If no queue
        // row exists, the downstream queue_dec UPDATE matches nothing anyway.
        await using var lockCmd = CreateCommand(conn);
        lockCmd.Transaction = tx;
        lockCmd.CommandText = """
                              WITH job_lock AS (
                                  SELECT j.name AS job_name, COALESCE(j.queue, 'default') AS queue_name
                                  FROM surefire_jobs j
                                  WHERE j.name = (SELECT job_name FROM surefire_runs WHERE id = @id)
                                  FOR UPDATE OF j
                              )
                              SELECT q.name
                              FROM job_lock jl
                              JOIN surefire_queues q ON q.name = jl.queue_name
                              FOR UPDATE OF q
                              """;
        lockCmd.Parameters.AddWithValue("id", transition.RunId);
        await lockCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;

        // running_count decrements on transition OUT of Running; non_terminal_count decrements
        // on transition INTO a terminal status. Both surefire_jobs decrements are merged into
        // one UPDATE because PG forbids two DML CTEs targeting the same row.
        var decrementRunning = transition.ExpectedStatus == JobStatus.Running;
        var decrementNonTerminal = transition.NewStatus is JobStatus.Succeeded or JobStatus.Failed
            or JobStatus.Canceled;

        var sb = new StringBuilder();
        sb.Append("""
                  WITH updated AS (
                      UPDATE surefire_runs SET
                          status = @new_status,
                          node_name = @node_name,
                          started_at = COALESCE(@started_at, started_at),
                          completed_at = COALESCE(@completed_at, completed_at),
                          canceled_at = COALESCE(@canceled_at, canceled_at),
                          reason = @reason,
                          result = @result,
                          progress = @progress,
                          not_before = @not_before,
                          last_heartbeat_at = COALESCE(@last_heartbeat_at, last_heartbeat_at)
                      WHERE id = @id
                          AND status = @expected_status
                          AND attempt = @expected_attempt
                          AND status NOT IN (2, 4, 5)
                      RETURNING job_name
                  )
                  """);

        if (decrementRunning || decrementNonTerminal)
        {
            var setClauses = new List<string>();
            if (decrementRunning)
            {
                setClauses.Add("running_count = GREATEST(0, surefire_jobs.running_count - 1)");
            }

            if (decrementNonTerminal)
            {
                setClauses.Add("non_terminal_count = GREATEST(0, surefire_jobs.non_terminal_count - 1)");
            }

            sb.Append($"""
                       , job_dec AS (
                           UPDATE surefire_jobs SET {string.Join(", ", setClauses)}
                           FROM updated WHERE surefire_jobs.name = updated.job_name
                           RETURNING 1
                       )
                       """);
        }

        if (decrementRunning)
        {
            sb.Append("""
                      , queue_dec AS (
                          UPDATE surefire_queues SET running_count = GREATEST(0, surefire_queues.running_count - 1)
                          FROM updated u
                          JOIN surefire_jobs j ON j.name = u.job_name
                          WHERE surefire_queues.name = COALESCE(j.queue, 'default')
                          RETURNING 1
                      )
                      """);
        }

        sb.Append(" SELECT COUNT(*) FROM updated");
        cmd.CommandText = sb.ToString();

        cmd.Parameters.AddWithValue("id", transition.RunId);
        cmd.Parameters.AddWithValue("new_status", (int)transition.NewStatus);
        cmd.Parameters.AddWithValue("node_name", (object?)transition.NodeName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("started_at",
            transition.StartedAt.HasValue ? transition.StartedAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("completed_at",
            transition.CompletedAt.HasValue ? transition.CompletedAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("canceled_at",
            transition.CanceledAt.HasValue ? transition.CanceledAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("reason", (object?)transition.Reason ?? DBNull.Value);
        cmd.Parameters.AddWithValue("result", (object?)transition.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("progress", transition.Progress);
        cmd.Parameters.AddWithValue("not_before", transition.NotBefore);
        cmd.Parameters.AddWithValue("last_heartbeat_at",
            transition.LastHeartbeatAt.HasValue ? transition.LastHeartbeatAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("expected_status", (int)transition.ExpectedStatus);
        cmd.Parameters.AddWithValue("expected_attempt", transition.ExpectedAttempt);

        var updated = Convert.ToInt32(await cmd.ExecuteScalarAsync(cancellationToken)) > 0;
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
        if (updated && (newStatus == JobStatus.Succeeded || newStatus == JobStatus.Canceled ||
                        newStatus == JobStatus.Failed))
        {
            await using var batchIdCmd = CreateCommand(conn);
            batchIdCmd.Transaction = tx;
            batchIdCmd.CommandText = "SELECT batch_id FROM surefire_runs WHERE id = @id";
            batchIdCmd.Parameters.AddWithValue("id", transition.RunId);
            var batchIdObj = await batchIdCmd.ExecuteScalarAsync(cancellationToken);
            var batchId = batchIdObj is string s ? s : null;

            if (batchId is { })
            {
                await using var incrCmd = CreateCommand(conn);
                incrCmd.Transaction = tx;
                incrCmd.CommandText = """
                                      UPDATE surefire_batches
                                      SET succeeded = succeeded + CASE WHEN @status = 2 THEN 1 ELSE 0 END,
                                          failed    = failed    + CASE WHEN @status = 5 THEN 1 ELSE 0 END,
                                          Canceled = Canceled + CASE WHEN @status = 4 THEN 1 ELSE 0 END
                                      WHERE id = @id AND status NOT IN (2, 4, 5)
                                      RETURNING total, succeeded, failed, canceled
                                      """;
                incrCmd.Parameters.AddWithValue("id", batchId);
                incrCmd.Parameters.AddWithValue("status", (int)newStatus);
                await using var reader = await incrCmd.ExecuteReaderAsync(cancellationToken);

                if (await reader.ReadAsync(cancellationToken))
                {
                    var total = reader.GetInt32(0);
                    var succeeded = reader.GetInt32(1);
                    var failed = reader.GetInt32(2);
                    var Canceled = reader.GetInt32(3);

                    if (succeeded + failed + Canceled >= total)
                    {
                        var batchStatus = failed > 0 ? JobStatus.Failed
                            : Canceled > 0 ? JobStatus.Canceled
                            : JobStatus.Succeeded;
                        var completedAt = timeProvider.GetUtcNow();

                        await reader.CloseAsync();

                        await using var completeCmd = CreateCommand(conn);
                        completeCmd.Transaction = tx;
                        completeCmd.CommandText = """
                                                  UPDATE surefire_batches
                                                  SET status = @status, completed_at = @completed_at
                                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                                  """;
                        completeCmd.Parameters.AddWithValue("id", batchId);
                        completeCmd.Parameters.AddWithValue("status", (short)batchStatus);
                        completeCmd.Parameters.AddWithValue("completed_at", completedAt);
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
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        // Pre-lock config rows in canonical order (jobs, then queues). See TryTransitionRunAsync
        // for the rationale. INNER JOIN: FOR UPDATE can't apply to a LEFT JOIN's nullable side,
        // and a missing queue row needs no lock anyway.
        await using var lockCmd = CreateCommand(conn);
        lockCmd.Transaction = tx;
        lockCmd.CommandText = """
                              WITH job_lock AS (
                                  SELECT j.name AS job_name, COALESCE(j.queue, 'default') AS queue_name
                                  FROM surefire_jobs j
                                  WHERE j.name = (SELECT job_name FROM surefire_runs WHERE id = @id)
                                  FOR UPDATE OF j
                              )
                              SELECT q.name
                              FROM job_lock jl
                              JOIN surefire_queues q ON q.name = jl.queue_name
                              FOR UPDATE OF q
                              """;
        lockCmd.Parameters.AddWithValue("id", runId);
        await lockCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;

        // Cancellation applies to Pending (0) or Running (1). The `prior` CTE locks the row and
        // exposes its prior status so downstream CTEs decrement running_count only when it was
        // Running. No SKIP LOCKED: targeting a specific run, we wait rather than silently miss.
        cmd.CommandText = """
                          WITH prior AS (
                              SELECT id, status, attempt, job_name, batch_id
                              FROM surefire_runs
                              WHERE id = @id
                                AND status NOT IN (2, 4, 5)
                                AND (@expected_attempt::int IS NULL OR attempt = @expected_attempt)
                              FOR UPDATE
                          ),
                          upd AS (
                              UPDATE surefire_runs SET
                                  status = 4, canceled_at = NOW(), completed_at = NOW(),
                                  reason = COALESCE(@reason, surefire_runs.reason)
                              FROM prior
                              WHERE surefire_runs.id = prior.id
                              RETURNING surefire_runs.id, prior.attempt, surefire_runs.batch_id,
                                        prior.status AS prior_status, prior.job_name
                          ),
                          -- Merged into one UPDATE because PG forbids two DML CTEs targeting
                          -- the same row. non_terminal_count always decrements; running_count
                          -- decrements only when the prior status was Running.
                          job_dec AS (
                              UPDATE surefire_jobs SET
                                  non_terminal_count = GREATEST(0, surefire_jobs.non_terminal_count - 1),
                                  running_count = CASE WHEN upd.prior_status = 1
                                      THEN GREATEST(0, surefire_jobs.running_count - 1)
                                      ELSE surefire_jobs.running_count END
                              FROM upd WHERE surefire_jobs.name = upd.job_name
                              RETURNING 1
                          ),
                          queue_dec AS (
                              UPDATE surefire_queues SET running_count = GREATEST(0, surefire_queues.running_count - 1)
                              FROM upd u
                              JOIN surefire_jobs j ON j.name = u.job_name
                              WHERE surefire_queues.name = COALESCE(j.queue, 'default')
                                AND u.prior_status = 1
                              RETURNING 1
                          )
                          SELECT id, attempt, batch_id FROM upd
                          """;

        cmd.Parameters.AddWithValue("id", runId);
        cmd.Parameters.AddWithValue("reason", (object?)reason ?? DBNull.Value);
        cmd.Parameters.AddWithValue("expected_attempt",
            expectedAttempt.HasValue ? expectedAttempt.Value : DBNull.Value);

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
        allEvents.Add(RunStatusEvents.Create(runId, attempt.Value, JobStatus.Canceled, timeProvider.GetUtcNow()));
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
                                  UPDATE surefire_batches
                                  SET canceled = canceled + 1
                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                  RETURNING total, succeeded, failed, canceled
                              """;
            incrCmd.Parameters.AddWithValue("id", batchId);
            await using var batchReader = await incrCmd.ExecuteReaderAsync(cancellationToken);

            if (await batchReader.ReadAsync(cancellationToken))
            {
                var total = batchReader.GetInt32(0);
                var succeeded = batchReader.GetInt32(1);
                var failed = batchReader.GetInt32(2);
                var Canceled = batchReader.GetInt32(3);

                if (succeeded + failed + Canceled >= total)
                {
                    var batchStatus = failed > 0 ? JobStatus.Failed
                        : Canceled > 0 ? JobStatus.Canceled
                        : JobStatus.Succeeded;
                    var completedAt = timeProvider.GetUtcNow();

                    await batchReader.CloseAsync();

                    await using var completeCmd = CreateCommand(conn);
                    completeCmd.Transaction = tx;
                    completeCmd.CommandText = """
                                              UPDATE surefire_batches
                                              SET status = @status, completed_at = @completed_at
                                              WHERE id = @id AND status NOT IN (2, 4, 5)
                                              """;
                    completeCmd.Parameters.AddWithValue("id", batchId);
                    completeCmd.Parameters.AddWithValue("status", (short)batchStatus);
                    completeCmd.Parameters.AddWithValue("completed_at", completedAt);
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);

                    batchCompletion = new(batchId, batchStatus, completedAt);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return new(true, batchCompletion);
    }

    public Task<IReadOnlyList<JobRun>> ClaimRunsAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, int maxCount, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCount, 1);

        if (jobNames.Count == 0 || queueNames.Count == 0)
        {
            return Task.FromResult<IReadOnlyList<JobRun>>(Array.Empty<JobRun>());
        }

        return ClaimRunsAsyncCore(nodeName, jobNames, queueNames, maxCount, cancellationToken);
    }

    public async Task CreateBatchAsync(JobBatch batch, IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        await using var batchCmd = CreateCommand(conn);
        batchCmd.Transaction = tx;
        batchCmd.CommandText = """
                               INSERT INTO surefire_batches (id, status, total, succeeded, failed, canceled, created_at, completed_at)
                               VALUES (@id, @status, @total, @succeeded, @failed, @Canceled, @created_at, @completed_at)
                               """;
        batchCmd.Parameters.AddWithValue("id", batch.Id);
        batchCmd.Parameters.AddWithValue("status", (short)batch.Status);
        batchCmd.Parameters.AddWithValue("total", batch.Total);
        batchCmd.Parameters.AddWithValue("succeeded", batch.Succeeded);
        batchCmd.Parameters.AddWithValue("failed", batch.Failed);
        batchCmd.Parameters.AddWithValue("Canceled", batch.Canceled);
        batchCmd.Parameters.AddWithValue("created_at", batch.CreatedAt);
        batchCmd.Parameters.AddWithValue("completed_at",
            batch.CompletedAt.HasValue ? batch.CompletedAt.Value : DBNull.Value);
        await batchCmd.ExecuteNonQueryAsync(cancellationToken);

        await CreateRunsCoreInTransactionAsync(conn, tx, runs, cancellationToken);
        await InsertEventsAsync(conn, tx, initialEvents, cancellationToken);

        await tx.CommitAsync(cancellationToken);
    }

    public async Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM surefire_batches WHERE id = @id";
        cmd.Parameters.AddWithValue("id", batchId);

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
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          UPDATE surefire_batches
                          SET status = @status, completed_at = @completed_at
                          WHERE id = @id AND status NOT IN (2, 4, 5)
                          RETURNING id
                          """;

        cmd.Parameters.AddWithValue("id", batchId);
        cmd.Parameters.AddWithValue("status", (short)status);
        cmd.Parameters.AddWithValue("completed_at", completedAt);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> CancelBatchRunsAsync(string batchId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          WITH prior AS (
                              SELECT id, status, attempt, job_name
                              FROM surefire_runs
                              WHERE batch_id = @batch_id AND status IN (0, 1)
                              FOR UPDATE
                          ),
                          upd AS (
                              UPDATE surefire_runs SET
                                  status = 4,
                                  canceled_at = NOW(),
                                  completed_at = NOW(),
                                  reason = COALESCE(@reason, surefire_runs.reason)
                              FROM prior
                              WHERE surefire_runs.id = prior.id
                              RETURNING surefire_runs.id, prior.attempt, prior.status AS prior_status, prior.job_name
                          ),
                          running_queues AS (
                              SELECT COALESCE(j.queue, 'default') AS queue_name, COUNT(*)::int AS cnt
                              FROM upd u JOIN surefire_jobs j ON j.name = u.job_name
                              WHERE u.prior_status = 1
                              GROUP BY COALESCE(j.queue, 'default')
                          ),
                          -- Per-job aggregates: non_terminal_count decrements by total Canceled
                          -- count regardless of prior status, running_count decrements only for
                          -- rows whose prior status was Running. Merged into a single UPDATE
                          -- against surefire_jobs per the Postgres "one row per statement" rule.
                          nt_by_job AS (
                              SELECT job_name,
                                  COUNT(*)::int AS nt_cnt,
                                  COUNT(*) FILTER (WHERE prior_status = 1)::int AS running_cnt
                              FROM upd GROUP BY job_name
                          ),
                          job_dec AS (
                              UPDATE surefire_jobs SET
                                  non_terminal_count = GREATEST(0, surefire_jobs.non_terminal_count - nt.nt_cnt),
                                  running_count = GREATEST(0, surefire_jobs.running_count - nt.running_cnt)
                              FROM nt_by_job nt WHERE surefire_jobs.name = nt.job_name
                              RETURNING 1
                          ),
                          queue_dec AS (
                              UPDATE surefire_queues SET running_count = GREATEST(0, surefire_queues.running_count - rq.cnt)
                              FROM running_queues rq WHERE surefire_queues.name = rq.queue_name
                              RETURNING 1
                          )
                          SELECT id, attempt FROM upd
                          """;
        cmd.Parameters.AddWithValue("batch_id", batchId);
        cmd.Parameters.AddWithValue("reason", (object?)reason ?? DBNull.Value);

        var CanceledIds = new List<string>();
        var statusEvents = new List<RunEvent>();
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var runId = reader.GetString(0);
                CanceledIds.Add(runId);
                statusEvents.Add(RunStatusEvents.Create(
                    runId,
                    reader.GetInt32(1),
                    JobStatus.Canceled,
                    timeProvider.GetUtcNow()));
            }
        }

        await InsertEventsAsync(conn, tx, statusEvents, cancellationToken);

        if (CanceledIds.Count > 0)
        {
            await using var incrCmd = CreateCommand(conn);
            incrCmd.Transaction = tx;
            incrCmd.CommandText = """
                                  UPDATE surefire_batches
                                  SET canceled = canceled + @cnt
                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                  RETURNING total, succeeded, failed, canceled
                              """;
            incrCmd.Parameters.AddWithValue("id", batchId);
            incrCmd.Parameters.AddWithValue("cnt", CanceledIds.Count);
            await using var batchReader = await incrCmd.ExecuteReaderAsync(cancellationToken);

            if (await batchReader.ReadAsync(cancellationToken))
            {
                var total = batchReader.GetInt32(0);
                var succeeded = batchReader.GetInt32(1);
                var failed = batchReader.GetInt32(2);
                var Canceled = batchReader.GetInt32(3);

                if (succeeded + failed + Canceled >= total)
                {
                    var batchStatus = failed > 0 ? JobStatus.Failed
                        : Canceled > 0 ? JobStatus.Canceled
                        : JobStatus.Succeeded;
                    var completedAt = timeProvider.GetUtcNow();

                    await batchReader.CloseAsync();

                    await using var completeCmd = CreateCommand(conn);
                    completeCmd.Transaction = tx;
                    completeCmd.CommandText = """
                                              UPDATE surefire_batches
                                              SET status = @status, completed_at = @completed_at
                                              WHERE id = @id AND status NOT IN (2, 4, 5)
                                              """;
                    completeCmd.Parameters.AddWithValue("id", batchId);
                    completeCmd.Parameters.AddWithValue("status", (short)batchStatus);
                    completeCmd.Parameters.AddWithValue("completed_at", completedAt);
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return CanceledIds;
    }

    public async Task<IReadOnlyList<string>> CancelChildRunsAsync(string parentRunId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        // Pre-lock config rows in canonical order (jobs, then queues) before locking child
        // run rows. Claims lock jobs first and runs later; doing the inverse here can deadlock
        // when retry cleanup races a worker claiming a pending child.
        await using (var lockJobCmd = CreateCommand(conn))
        {
            lockJobCmd.Transaction = tx;
            lockJobCmd.CommandText = """
                                     WITH affected_jobs AS (
                                         SELECT DISTINCT job_name
                                         FROM surefire_runs
                                         WHERE parent_run_id = @parent_run_id AND status IN (0, 1)
                                     )
                                     SELECT j.name
                                     FROM surefire_jobs j
                                     JOIN affected_jobs aj ON aj.job_name = j.name
                                     ORDER BY j.name
                                     FOR UPDATE OF j
                                     """;
            lockJobCmd.Parameters.AddWithValue("parent_run_id", parentRunId);
            await lockJobCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var lockQueueCmd = CreateCommand(conn))
        {
            lockQueueCmd.Transaction = tx;
            lockQueueCmd.CommandText = """
                                       WITH affected_queues AS (
                                           SELECT DISTINCT COALESCE(j.queue, 'default') AS queue_name
                                           FROM surefire_runs r
                                           JOIN surefire_jobs j ON j.name = r.job_name
                                           WHERE r.parent_run_id = @parent_run_id AND r.status IN (0, 1)
                                       )
                                       SELECT q.name
                                       FROM surefire_queues q
                                       JOIN affected_queues aq ON aq.queue_name = q.name
                                       ORDER BY q.name
                                       FOR UPDATE OF q
                                       """;
            lockQueueCmd.Parameters.AddWithValue("parent_run_id", parentRunId);
            await lockQueueCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        // Capture prior status under U-lock so running_count only decrements for rows that
        // were Running; aggregate per job/queue into single UPDATEs in the same statement.
        cmd.CommandText = """
                          WITH prior AS (
                              SELECT id, status, attempt, batch_id, job_name
                              FROM surefire_runs
                              WHERE parent_run_id = @parent_run_id AND status IN (0, 1)
                              ORDER BY job_name, id
                              FOR UPDATE
                          ),
                          upd AS (
                              UPDATE surefire_runs SET
                                  status = 4,
                                  canceled_at = NOW(),
                                  completed_at = NOW(),
                                  reason = COALESCE(@reason, surefire_runs.reason)
                              FROM prior
                              WHERE surefire_runs.id = prior.id
                              RETURNING surefire_runs.id, prior.attempt, surefire_runs.batch_id,
                                        prior.status AS prior_status, prior.job_name
                          ),
                          running_queues AS (
                              SELECT COALESCE(j.queue, 'default') AS queue_name, COUNT(*)::int AS cnt
                              FROM upd u JOIN surefire_jobs j ON j.name = u.job_name
                              WHERE u.prior_status = 1
                              GROUP BY COALESCE(j.queue, 'default')
                          ),
                          -- Per-job aggregates: non_terminal_count decrements by total Canceled
                          -- count, running_count decrements only for Running priors. Merged into
                          -- one UPDATE per the Postgres "one row per statement" rule.
                          nt_by_job AS (
                              SELECT job_name,
                                  COUNT(*)::int AS nt_cnt,
                                  COUNT(*) FILTER (WHERE prior_status = 1)::int AS running_cnt
                              FROM upd GROUP BY job_name
                          ),
                          job_dec AS (
                              UPDATE surefire_jobs SET
                                  non_terminal_count = GREATEST(0, surefire_jobs.non_terminal_count - nt.nt_cnt),
                                  running_count = GREATEST(0, surefire_jobs.running_count - nt.running_cnt)
                              FROM nt_by_job nt WHERE surefire_jobs.name = nt.job_name
                              RETURNING 1
                          ),
                          queue_dec AS (
                              UPDATE surefire_queues SET running_count = GREATEST(0, surefire_queues.running_count - rq.cnt)
                              FROM running_queues rq WHERE surefire_queues.name = rq.queue_name
                              RETURNING 1
                          )
                          SELECT id, attempt, batch_id FROM upd
                          """;
        cmd.Parameters.AddWithValue("parent_run_id", parentRunId);
        cmd.Parameters.AddWithValue("reason", (object?)reason ?? DBNull.Value);

        var CanceledIds = new List<string>();
        var CanceledBatchIds = new Dictionary<string, int>();
        var statusEvents = new List<RunEvent>();
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var runId = reader.GetString(0);
                CanceledIds.Add(runId);
                statusEvents.Add(RunStatusEvents.Create(
                    runId,
                    reader.GetInt32(1),
                    JobStatus.Canceled,
                    timeProvider.GetUtcNow()));
                var bId = reader.IsDBNull(2) ? null : reader.GetString(2);
                if (bId is { })
                {
                    CanceledBatchIds[bId] = CanceledBatchIds.GetValueOrDefault(bId) + 1;
                }
            }
        }

        await InsertEventsAsync(conn, tx, statusEvents, cancellationToken);

        foreach (var (batchId, cnt) in CanceledBatchIds)
        {
            await using var incrCmd = CreateCommand(conn);
            incrCmd.Transaction = tx;
            incrCmd.CommandText = """
                                  UPDATE surefire_batches
                                  SET canceled = canceled + @cnt
                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                  RETURNING total, succeeded, failed, canceled
                              """;
            incrCmd.Parameters.AddWithValue("id", batchId);
            incrCmd.Parameters.AddWithValue("cnt", cnt);
            await using var batchReader = await incrCmd.ExecuteReaderAsync(cancellationToken);

            if (await batchReader.ReadAsync(cancellationToken))
            {
                var total = batchReader.GetInt32(0);
                var succeeded = batchReader.GetInt32(1);
                var failed = batchReader.GetInt32(2);
                var Canceled = batchReader.GetInt32(3);

                if (succeeded + failed + Canceled >= total)
                {
                    var batchStatus = failed > 0 ? JobStatus.Failed
                        : Canceled > 0 ? JobStatus.Canceled
                        : JobStatus.Succeeded;
                    var completedAt = timeProvider.GetUtcNow();

                    await batchReader.CloseAsync();

                    await using var completeCmd = CreateCommand(conn);
                    completeCmd.Transaction = tx;
                    completeCmd.CommandText = """
                                              UPDATE surefire_batches
                                              SET status = @status, completed_at = @completed_at
                                              WHERE id = @id AND status NOT IN (2, 4, 5)
                                              """;
                    completeCmd.Parameters.AddWithValue("id", batchId);
                    completeCmd.Parameters.AddWithValue("status", (short)batchStatus);
                    completeCmd.Parameters.AddWithValue("completed_at", completedAt);
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return CanceledIds;
    }

    public async Task<IReadOnlyList<string>> GetCompletableBatchIdsAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SELECT b.id FROM surefire_batches b
                          WHERE b.status NOT IN (2, 4, 5)
                          AND NOT EXISTS (
                              SELECT 1 FROM surefire_runs r
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

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        await InsertEventsAsync(conn, tx, events, cancellationToken);

        await tx.CommitAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0,
        RunEventType[]? types = null, int? attempt = null, int? take = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);

        // The xmin clamp prevents skipping in-flight low-id rows. It is gated on run status
        // because a terminal run has no further writers; applying it anyway would hide already-
        // committed rows when an unrelated long TX holds xmin back, letting the reader drain
        // zero events and exit with a hole. (Same shape as GetBatchEventsAsync.)
        var sb = new StringBuilder("""
                                   SELECT e.* FROM surefire_events e
                                   JOIN surefire_runs r ON r.id = e.run_id
                                   WHERE e.run_id = @run_id
                                     AND e.id > @since_id
                                     AND (
                                         r.status IN (2, 4, 5)
                                         OR e.id < COALESCE(
                                             (SELECT MIN(id) FROM surefire_events
                                              WHERE xact_id >= pg_snapshot_xmin(pg_current_snapshot())),
                                             9223372036854775807)
                                     )
                                   """);
        cmd.Parameters.AddWithValue("run_id", runId);
        cmd.Parameters.AddWithValue("since_id", sinceId);

        if (types is { Length: > 0 })
        {
            sb.Append(" AND e.event_type = ANY(@types)");
            cmd.Parameters.AddWithValue("types", types.Select(t => (short)t).ToArray());
        }

        if (attempt is { })
        {
            sb.Append(" AND (e.attempt = @attempt OR e.attempt = 0)");
            cmd.Parameters.AddWithValue("attempt", attempt.Value);
        }

        sb.Append(" ORDER BY e.id");

        if (take is { })
        {
            sb.Append(" LIMIT @take");
            cmd.Parameters.AddWithValue("take", take.Value);
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

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        // See GetBatchEventsAsync for the conditional-clamp rationale.
        cmd.CommandText = """
                          SELECT e.*
                          FROM surefire_events e
                          JOIN surefire_runs r ON r.id = e.run_id
                          JOIN surefire_batches b ON b.id = r.batch_id
                          WHERE r.batch_id = @batch_id
                              AND e.event_type = @event_type
                              AND e.id > @since_event_id
                              AND (
                                  b.status IN (2, 4, 5)
                                  OR e.id < COALESCE(
                                      (SELECT MIN(id) FROM surefire_events
                                       WHERE xact_id >= pg_snapshot_xmin(pg_current_snapshot())),
                                      9223372036854775807)
                              )
                          ORDER BY e.id
                          LIMIT @take
                          """;
        cmd.Parameters.AddWithValue("batch_id", batchId);
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

    public async Task<IReadOnlyList<RunEvent>> GetBatchEventsAsync(string batchId, long sinceEventId = 0,
        int take = 200, CancellationToken cancellationToken = default)
    {
        if (take <= 0)
        {
            return [];
        }

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        // The xmin clamp prevents skipping in-flight low-id rows. It is gated on batch status
        // because a terminal batch has no further writers; applying it anyway would hide
        // already-committed rows when an unrelated long TX holds xmin back, letting the reader
        // drain zero events and exit with a hole.
        cmd.CommandText = """
                          SELECT e.*
                          FROM surefire_events e
                          JOIN surefire_runs r ON r.id = e.run_id
                          JOIN surefire_batches b ON b.id = r.batch_id
                          WHERE r.batch_id = @batch_id
                              AND e.id > @since_event_id
                              AND (
                                  b.status IN (2, 4, 5)
                                  OR e.id < COALESCE(
                                      (SELECT MIN(id) FROM surefire_events
                                       WHERE xact_id >= pg_snapshot_xmin(pg_current_snapshot())),
                                      9223372036854775807)
                              )
                          ORDER BY e.id
                          LIMIT @take
                          """;
        cmd.Parameters.AddWithValue("batch_id", batchId);
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
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        await using var nodeCmd = CreateCommand(conn);
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
            await using var runCmd = CreateCommand(conn);
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

    public async Task<IReadOnlyList<string>> GetExternallyStoppedRunIdsAsync(IReadOnlyCollection<string> runIds,
        CancellationToken cancellationToken = default)
    {
        if (runIds.Count == 0)
        {
            return [];
        }

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);

        // Returns input IDs that no longer correspond to a Running row, including IDs that were
        // deleted entirely (the LEFT JOIN's r.id IS NULL branch).
        cmd.CommandText = """
                          SELECT input_id
                          FROM unnest(@ids) AS input_id
                          LEFT JOIN surefire_runs r ON r.id = input_id
                          WHERE r.id IS NULL OR r.status <> 1
                          """;
        cmd.Parameters.AddWithValue("ids", runIds.ToArray());

        var results = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(reader.GetString(0));
        }

        return results;
    }

    public async Task<IReadOnlyList<string>> GetStaleRunningRunIdsAsync(DateTimeOffset staleBefore, int take,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(take, 1);

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        // Backed by ix_surefire_runs_stale_heartbeat. Oldest-first so the caller's loop makes
        // monotonic progress against a shrinking filter set.
        cmd.CommandText = """
                          SELECT id FROM surefire_runs
                          WHERE status = 1 AND last_heartbeat_at < @stale_before
                          ORDER BY last_heartbeat_at ASC, id ASC
                          LIMIT @take
                          """;
        cmd.Parameters.AddWithValue("take", take);
        cmd.Parameters.AddWithValue("stale_before", staleBefore);

        var ids = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ids.Add(reader.GetString(0));
        }

        return ids;
    }

    public async Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM surefire_nodes";

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
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM surefire_nodes WHERE name = @name LIMIT 1";
        cmd.Parameters.AddWithValue("name", name);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return ReadNode(reader);
        }

        return null;
    }

    public async Task UpsertQueuesAsync(IReadOnlyList<QueueDefinition> queues,
        CancellationToken cancellationToken = default)
    {
        if (queues.Count == 0)
        {
            return;
        }

        // is_paused is omitted from DO UPDATE SET so dashboard pauses survive re-upserts;
        // first insert takes is_paused from the payload.
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          INSERT INTO surefire_queues (name, priority, max_concurrency, is_paused, rate_limit_name, last_heartbeat_at)
                          SELECT
                              e->>'name',
                              (e->>'priority')::int,
                              (e->>'maxConcurrency')::int,
                              (e->>'isPaused')::boolean,
                              e->>'rateLimitName',
                              NOW()
                          FROM jsonb_array_elements(@payload::jsonb) AS e
                          ORDER BY e->>'name'
                          ON CONFLICT (name) DO UPDATE SET
                              priority = EXCLUDED.priority,
                              max_concurrency = EXCLUDED.max_concurrency,
                              rate_limit_name = EXCLUDED.rate_limit_name,
                              last_heartbeat_at = NOW();
                          """;
        cmd.Parameters.AddWithValue("payload", UpsertPayloadFactory.SerializeQueues(queues));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM surefire_queues";

        var results = new List<QueueDefinition>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadQueue(reader));
        }

        return results;
    }

    public async Task<bool> SetQueuePausedAsync(string name, bool isPaused,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "UPDATE surefire_queues SET is_paused = @is_paused WHERE name = @name";
        cmd.Parameters.AddWithValue("name", name);
        cmd.Parameters.AddWithValue("is_paused", isPaused);
        return await cmd.ExecuteNonQueryAsync(cancellationToken) > 0;
    }

    public async Task UpsertRateLimitsAsync(IReadOnlyList<RateLimitDefinition> rateLimits,
        CancellationToken cancellationToken = default)
    {
        if (rateLimits.Count == 0)
        {
            return;
        }

        // Runtime counters are absent from the statement, so preserved verbatim on update.
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          INSERT INTO surefire_rate_limits (name, type, max_permits, "window", last_heartbeat_at)
                          SELECT
                              e->>'name',
                              (e->>'type')::int,
                              (e->>'maxPermits')::int,
                              (e->>'window')::bigint,
                              NOW()
                          FROM jsonb_array_elements(@payload::jsonb) AS e
                          ORDER BY e->>'name'
                          ON CONFLICT (name) DO UPDATE SET
                              type = EXCLUDED.type,
                              max_permits = EXCLUDED.max_permits,
                              "window" = EXCLUDED."window",
                              last_heartbeat_at = NOW()
                          """;
        cmd.Parameters.AddWithValue("payload", UpsertPayloadFactory.SerializeRateLimits(rateLimits));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> CancelExpiredRunsWithIdsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        // Only pending rows are touched, so running_count is unaffected; non_terminal_count
        // decrements once per Canceled row, grouped by job_name.
        cmd.CommandText = """
                          WITH upd AS (
                              UPDATE surefire_runs SET
                                  status = 4,
                                  canceled_at = NOW(),
                                  completed_at = NOW(),
                                  reason = @reason
                              WHERE status = 0
                                  AND not_after IS NOT NULL
                                  AND not_after < NOW()
                              RETURNING id, attempt, batch_id, job_name
                          ),
                          nt_by_job AS (
                              SELECT job_name, COUNT(*)::int AS cnt FROM upd GROUP BY job_name
                          ),
                          nt_dec AS (
                              UPDATE surefire_jobs SET non_terminal_count = GREATEST(0, surefire_jobs.non_terminal_count - nt.cnt)
                              FROM nt_by_job nt WHERE surefire_jobs.name = nt.job_name
                              RETURNING 1
                          )
                          SELECT id, attempt, batch_id FROM upd
                          """;
        cmd.Parameters.AddWithValue("reason", "Run expired past NotAfter deadline.");

        var CanceledIds = new List<string>();
        var CanceledBatchIds = new Dictionary<string, int>();
        var statusEvents = new List<RunEvent>();
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                var runId = reader.GetString(0);
                CanceledIds.Add(runId);
                statusEvents.Add(RunStatusEvents.Create(runId, reader.GetInt32(1), JobStatus.Canceled,
                    timeProvider.GetUtcNow()));
                var bId = reader.IsDBNull(2) ? null : reader.GetString(2);
                if (bId is { })
                {
                    CanceledBatchIds[bId] = CanceledBatchIds.GetValueOrDefault(bId) + 1;
                }
            }
        }

        await InsertEventsAsync(conn, tx, statusEvents, cancellationToken);

        foreach (var (batchId, cnt) in CanceledBatchIds)
        {
            await using var incrCmd = CreateCommand(conn);
            incrCmd.Transaction = tx;
            incrCmd.CommandText = """
                                  UPDATE surefire_batches
                                  SET canceled = canceled + @cnt
                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                  RETURNING total, succeeded, failed, canceled
                              """;
            incrCmd.Parameters.AddWithValue("id", batchId);
            incrCmd.Parameters.AddWithValue("cnt", cnt);
            await using var batchReader = await incrCmd.ExecuteReaderAsync(cancellationToken);

            if (await batchReader.ReadAsync(cancellationToken))
            {
                var total = batchReader.GetInt32(0);
                var succeeded = batchReader.GetInt32(1);
                var failed = batchReader.GetInt32(2);
                var Canceled = batchReader.GetInt32(3);

                if (succeeded + failed + Canceled >= total)
                {
                    var batchStatus = failed > 0 ? JobStatus.Failed
                        : Canceled > 0 ? JobStatus.Canceled
                        : JobStatus.Succeeded;
                    var completedAt = timeProvider.GetUtcNow();

                    await batchReader.CloseAsync();

                    await using var completeCmd = CreateCommand(conn);
                    completeCmd.Transaction = tx;
                    completeCmd.CommandText = """
                                              UPDATE surefire_batches
                                              SET status = @status, completed_at = @completed_at
                                              WHERE id = @id AND status NOT IN (2, 4, 5)
                                              """;
                    completeCmd.Parameters.AddWithValue("id", batchId);
                    completeCmd.Parameters.AddWithValue("status", (short)batchStatus);
                    completeCmd.Parameters.AddWithValue("completed_at", completedAt);
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return CanceledIds;
    }

    public async Task PurgeAsync(DateTimeOffset threshold, CancellationToken cancellationToken = default)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);

        while (true)
        {
            // DELETE + counter adjustment in one statement via CTEs so the counter stays
            // consistent with surefire_runs. Only non-terminal deletions decrement.
            await using var runCmd = CreateCommand(conn);
            runCmd.CommandText = """
                                 WITH deleted AS (
                                     DELETE FROM surefire_runs WHERE id IN (
                                         SELECT id FROM surefire_runs
                                         WHERE (status IN (2, 4, 5)
                                             AND completed_at < @threshold
                                             AND (batch_id IS NULL OR EXISTS (
                                                 SELECT 1 FROM surefire_batches b
                                                 WHERE b.id = surefire_runs.batch_id
                                                     AND b.status IN (2, 4, 5)
                                                     AND b.completed_at IS NOT NULL
                                                     AND b.completed_at < @threshold
                                             )))
                                             OR (status = 0 AND not_before < @threshold)
                                         LIMIT 1000
                                     )
                                     RETURNING id, status, job_name
                                 ),
                                 nt_by_job AS (
                                     SELECT job_name, COUNT(*)::int AS cnt FROM deleted
                                     WHERE status NOT IN (2, 4, 5)
                                     GROUP BY job_name
                                 ),
                                 nt_dec AS (
                                     UPDATE surefire_jobs SET non_terminal_count = GREATEST(0, surefire_jobs.non_terminal_count - nt.cnt)
                                     FROM nt_by_job nt WHERE surefire_jobs.name = nt.job_name
                                     RETURNING 1
                                 )
                                 SELECT COUNT(*) FROM deleted
                                 """;
            runCmd.Parameters.AddWithValue("threshold", threshold);
            var deletedCount = Convert.ToInt32(await runCmd.ExecuteScalarAsync(cancellationToken));
            if (deletedCount == 0)
            {
                break;
            }
        }

        await using var batchCmd = CreateCommand(conn);
        batchCmd.CommandText = """
                               DELETE FROM surefire_batches
                               WHERE status IN (2, 4, 5) AND completed_at IS NOT NULL AND completed_at < @threshold
                               """;
        batchCmd.Parameters.AddWithValue("threshold", threshold);
        await batchCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var jobCmd = CreateCommand(conn);
        jobCmd.CommandText = """
                             DELETE FROM surefire_jobs
                             WHERE last_heartbeat_at < @threshold
                                 AND NOT EXISTS (SELECT 1 FROM surefire_runs r WHERE r.job_name = surefire_jobs.name AND r.status NOT IN (2, 4, 5))
                             """;
        jobCmd.Parameters.AddWithValue("threshold", threshold);
        await jobCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var queueCmd = CreateCommand(conn);
        queueCmd.CommandText = "DELETE FROM surefire_queues WHERE last_heartbeat_at < @threshold";
        queueCmd.Parameters.AddWithValue("threshold", threshold);
        await queueCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var rlCmd = CreateCommand(conn);
        rlCmd.CommandText = "DELETE FROM surefire_rate_limits WHERE last_heartbeat_at < @threshold";
        rlCmd.Parameters.AddWithValue("threshold", threshold);
        await rlCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var nodeCmd = CreateCommand(conn);
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

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);

        var now = timeProvider.GetUtcNow();
        var rawSince = since ?? now.AddHours(-24);
        var sinceTime = new DateTimeOffset(
            rawSince.Ticks / TimeSpan.TicksPerMinute * TimeSpan.TicksPerMinute,
            rawSince.Offset);

        await using var statsCmd = CreateCommand(conn);
        statsCmd.CommandText = """
                               SELECT
                                   (SELECT COUNT(*) FROM surefire_jobs) AS total_jobs,
                                   (SELECT COUNT(*) FROM surefire_nodes WHERE last_heartbeat_at >= @now - INTERVAL '2 minutes') AS node_count,
                                   COUNT(*) AS total_runs,
                                   COUNT(*) FILTER (WHERE status = 0) AS pending,
                                   COUNT(*) FILTER (WHERE status = 1) AS running,
                                   COUNT(*) FILTER (WHERE status = 2) AS succeeded,
                                   COUNT(*) FILTER (WHERE status = 4) AS canceled,
                                   COUNT(*) FILTER (WHERE status = 5) AS failed
                               FROM surefire_runs
                               WHERE created_at >= @since AND created_at <= @now
                               """;
        statsCmd.Parameters.AddWithValue("now", now);
        statsCmd.Parameters.AddWithValue("since", sinceTime);

        int totalJobs = 0, totalRuns = 0, nodeCount = 0;
        int pending = 0, running = 0, completed = 0, Canceled = 0, deadLetter = 0;
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
                Canceled = (int)reader.GetInt64(6);
                deadLetter = (int)reader.GetInt64(7);
            }
        }

        var activeRuns = pending + running;
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
            runsByStatus["Succeeded"] = completed;
        }

        if (Canceled > 0)
        {
            runsByStatus["Canceled"] = Canceled;
        }

        if (deadLetter > 0)
        {
            runsByStatus["Failed"] = deadLetter;
        }

        var terminalCount = completed + Canceled + deadLetter;
        var successRate = terminalCount > 0 ? completed / (double)terminalCount : 0.0;

        await using var bucketCmd = CreateCommand(conn);
        bucketCmd.CommandText = """
                                WITH bucketed_runs AS (
                                     SELECT
                                         CASE
                                            WHEN status = 1 THEN COALESCE(started_at, created_at)
                                            WHEN status = 2 THEN COALESCE(completed_at, started_at, created_at)
                                            WHEN status = 4 THEN COALESCE(canceled_at, completed_at, created_at)
                                            WHEN status = 5 THEN COALESCE(completed_at, started_at, created_at)
                                            ELSE created_at
                                         END AS bucket_time,
                                         status
                                     FROM surefire_runs
                                 )
                                 SELECT
                                     date_bin(@interval::interval, bucket_time, @since) AS bucket_start,
                                     COUNT(*) FILTER (WHERE status = 0) AS pending,
                                     COUNT(*) FILTER (WHERE status = 1) AS running,
                                     COUNT(*) FILTER (WHERE status = 2) AS succeeded,
                                     COUNT(*) FILTER (WHERE status = 4) AS canceled,
                                     COUNT(*) FILTER (WHERE status = 5) AS failed
                                FROM bucketed_runs
                                WHERE bucket_time >= @since AND bucket_time <= @now
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
                    Succeeded = (int)reader.GetInt64(3),
                    Canceled = (int)reader.GetInt64(4),
                    Failed = (int)reader.GetInt64(5)
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
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
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
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          WITH queue_names AS (
                              SELECT name FROM surefire_queues
                              UNION
                              SELECT COALESCE(j.queue, 'default') AS name
                              FROM surefire_runs r
                              JOIN surefire_jobs j ON j.name = r.job_name
                              WHERE r.status = 0
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
                              WHERE r.status = 0
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

    // NpgsqlException.IsTransient covers connection failures but excludes 40P01 (deadlock) and
    // 40001 (serialization), which are safe to retry once the victim rolls back. Adding them
    // here lets callers retry through lock-cycle races instead of failing the run.
    public bool IsTransientException(Exception ex) =>
        ex is NpgsqlException { IsTransient: true }
        || ex is PostgresException { SqlState: "40P01" or "40001" };

    private NpgsqlCommand CreateCommand(NpgsqlConnection conn)
    {
        var cmd = conn.CreateCommand();
        if (CommandTimeoutSeconds is { } seconds)
        {
            cmd.CommandTimeout = seconds;
        }

        return cmd;
    }

    private async Task ReleaseMigrationLockAsync(NpgsqlConnection connection)
    {
        await using var cmd = CreateCommand(connection);
        cmd.CommandText = "SELECT pg_advisory_unlock(hashtext('surefire_migrate'))";
        await cmd.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private async Task AcquireMigrationLockAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(MigrationLockWaitTimeout);

        try
        {
            while (true)
            {
                timeoutCts.Token.ThrowIfCancellationRequested();

                await using var lockCmd = CreateCommand(connection);
                lockCmd.CommandText = "SELECT pg_try_advisory_lock(hashtext('surefire_migrate'))";
                var acquired = (bool)(await lockCmd.ExecuteScalarAsync(timeoutCts.Token))!;
                if (acquired)
                {
                    return;
                }

                await Task.Delay(MigrationLockRetryDelay, timeProvider, timeoutCts.Token);
            }
        }
        catch (OperationCanceledException)
            when (timeoutCts.Token.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            // Only translate to TimeoutException when the local timer fired and the caller
            // didn't cancel. If both fire concurrently, propagate the original OCE.
            throw new TimeoutException(
                $"Timed out waiting {MigrationLockWaitTimeout.TotalSeconds:0}s to acquire the Surefire PostgreSQL migration lock.");
        }
    }

    private async Task<IReadOnlyList<JobRun>> ClaimRunsAsyncCore(string nodeName,
        IReadOnlyCollection<string> jobNames, IReadOnlyCollection<string> queueNames, int maxCount,
        CancellationToken cancellationToken)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        // Lock config rows so two nodes can't independently compute capacity from the same
        // snapshot and over-claim. Locked unconditionally because running_count is maintained
        // per-row and we need the lock to safely increment it inside this statement.
        //
        // Three invariants for deadlock-free operation:
        //   (1) Canonical order: jobs -> queues -> rate_limits -> runs. Every mutating path
        //       (claim, enqueue, transition, cancel) obeys it.
        //   (2) ORDER BY on multi-row FOR UPDATE forces deterministic acquisition order.
        //   (3) FOR UPDATE OF <alias> scopes to the outer table; without OF, PG also locks
        //       subquery-referenced rows in nondeterministic order.
        await using var lockJobCmd = CreateCommand(conn);
        lockJobCmd.Transaction = tx;
        lockJobCmd.CommandText =
            "SELECT name FROM surefire_jobs WHERE name = ANY(@job_names) ORDER BY name FOR UPDATE";
        lockJobCmd.Parameters.AddWithValue("job_names", jobNames.ToArray());
        await lockJobCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var lockQueueCmd = CreateCommand(conn);
        lockQueueCmd.Transaction = tx;
        lockQueueCmd.CommandText = """
                                   SELECT q.name FROM surefire_queues q
                                   WHERE q.name IN (
                                       SELECT COALESCE(j.queue, 'default') FROM surefire_jobs j WHERE j.name = ANY(@job_names)
                                   )
                                   ORDER BY q.name
                                   FOR UPDATE OF q
                                   """;
        lockQueueCmd.Parameters.AddWithValue("job_names", jobNames.ToArray());
        await lockQueueCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var lockRlCmd = CreateCommand(conn);
        lockRlCmd.Transaction = tx;
        lockRlCmd.CommandText = """
                                SELECT rl.name FROM surefire_rate_limits rl
                                WHERE rl.name IN (
                                    SELECT j.rate_limit_name FROM surefire_jobs j
                                    WHERE j.name = ANY(@job_names) AND j.rate_limit_name IS NOT NULL
                                    UNION
                                    SELECT q.rate_limit_name FROM surefire_jobs j
                                    JOIN surefire_queues q ON q.name = COALESCE(j.queue, 'default')
                                    WHERE j.name = ANY(@job_names) AND q.rate_limit_name IS NOT NULL
                                )
                                ORDER BY rl.name
                                FOR UPDATE OF rl
                                """;
        lockRlCmd.Parameters.AddWithValue("job_names", jobNames.ToArray());
        await lockRlCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;

        // ranked uses materialized running_count instead of scanning surefire_runs. After the
        // claim UPDATE, downstream CTEs aggregate per-job/queue/rate-limit increments in the
        // same statement (one round trip). ROW_NUMBER PARTITION BY caps per-bucket strictly;
        // queue_rl_rn is skipped when the queue shares the job's rate limiter.
        cmd.CommandText = """
                          WITH rl_state AS (
                              SELECT rl.name,
                                  CEIL(GREATEST(0, rl.max_permits - (
                                      CASE
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
                                      END
                                  )))::int AS available
                              FROM surefire_rate_limits rl
                          ),
                          ranked AS (
                              SELECT r.id, COALESCE(q.priority, 0) AS queue_priority, r.priority, r.not_before,
                                  r.job_name AS run_job_name, COALESCE(j.queue, 'default') AS run_queue_name,
                                  j.max_concurrency AS j_max, q.max_concurrency AS q_max,
                                  j.running_count AS j_running, COALESCE(q.running_count, 0) AS q_running,
                                  j.rate_limit_name AS j_rl, q.rate_limit_name AS q_rl,
                                  ROW_NUMBER() OVER (PARTITION BY r.job_name
                                      ORDER BY COALESCE(q.priority, 0) DESC, r.priority DESC, r.not_before ASC, r.id ASC) AS j_rn,
                                  ROW_NUMBER() OVER (PARTITION BY COALESCE(j.queue, 'default')
                                      ORDER BY COALESCE(q.priority, 0) DESC, r.priority DESC, r.not_before ASC, r.id ASC) AS q_rn,
                                  ROW_NUMBER() OVER (PARTITION BY j.rate_limit_name
                                      ORDER BY COALESCE(q.priority, 0) DESC, r.priority DESC, r.not_before ASC, r.id ASC) AS j_rl_rn,
                                  ROW_NUMBER() OVER (PARTITION BY q.rate_limit_name
                                      ORDER BY COALESCE(q.priority, 0) DESC, r.priority DESC, r.not_before ASC, r.id ASC) AS q_rl_rn
                              FROM surefire_runs r
                              JOIN surefire_jobs j ON j.name = r.job_name
                              LEFT JOIN surefire_queues q ON q.name = COALESCE(j.queue, 'default')
                              WHERE r.status = 0
                                  AND r.not_before <= NOW()
                                  AND (r.not_after IS NULL OR r.not_after > NOW())
                                  AND r.job_name = ANY(@job_names)
                                  AND COALESCE(j.queue, 'default') = ANY(@queue_names)
                                  AND COALESCE(q.is_paused, FALSE) = FALSE
                          ),
                          eligible AS (
                              SELECT r.id, r.run_job_name, r.run_queue_name, r.j_rl, r.q_rl,
                                     r.queue_priority, r.priority, r.not_before
                              FROM ranked r
                              LEFT JOIN rl_state jrl ON jrl.name = r.j_rl
                              LEFT JOIN rl_state qrl ON qrl.name = r.q_rl
                              WHERE (r.j_max IS NULL OR r.j_rn <= r.j_max - r.j_running)
                                  AND (r.q_max IS NULL OR r.q_rn <= r.q_max - r.q_running)
                                  AND (r.j_rl IS NULL OR jrl.available IS NULL OR r.j_rl_rn <= jrl.available)
                                  AND (r.q_rl IS NULL OR r.q_rl = r.j_rl OR qrl.available IS NULL OR r.q_rl_rn <= qrl.available)
                              ORDER BY r.queue_priority DESC, r.priority DESC, r.not_before ASC, r.id ASC
                              LIMIT @max_count
                          ),
                          locked AS (
                              -- status=0 / not_before re-checks are critical: Postgres EvalPlanQual
                              -- re-evaluates these predicates when SKIP LOCKED encounters a row whose
                              -- state changed since the statement snapshot. Without them, a row another
                              -- tx claimed between eligible and this lock could be re-claimed.
                              SELECT r.id, e.run_job_name, e.run_queue_name, e.j_rl, e.q_rl
                              FROM surefire_runs r
                              JOIN eligible e ON e.id = r.id
                              WHERE r.status = 0
                                AND r.not_before <= NOW()
                                AND (r.not_after IS NULL OR r.not_after > NOW())
                              FOR UPDATE OF r SKIP LOCKED
                          ),
                          claimed AS (
                              UPDATE surefire_runs SET
                                  status = 1,
                                  node_name = @node_name,
                                  started_at = NOW(),
                                  last_heartbeat_at = NOW(),
                                  attempt = surefire_runs.attempt + 1
                              FROM locked
                              WHERE surefire_runs.id = locked.id
                              RETURNING surefire_runs.*,
                                  locked.run_job_name AS claim_job_name,
                                  locked.run_queue_name AS claim_queue_name,
                                  locked.j_rl AS claim_j_rl,
                                  locked.q_rl AS claim_q_rl
                          ),
                          job_increments AS (
                              SELECT claim_job_name, COUNT(*)::int AS cnt FROM claimed GROUP BY claim_job_name
                          ),
                          queue_increments AS (
                              SELECT claim_queue_name, COUNT(*)::int AS cnt FROM claimed GROUP BY claim_queue_name
                          ),
                          rl_pairs AS (
                              SELECT claim_j_rl AS rl_name FROM claimed WHERE claim_j_rl IS NOT NULL
                              UNION ALL
                              SELECT claim_q_rl FROM claimed
                              WHERE claim_q_rl IS NOT NULL AND claim_q_rl <> COALESCE(claim_j_rl, '')
                          ),
                          rl_increments AS (
                              SELECT rl_name, COUNT(*)::int AS cnt FROM rl_pairs GROUP BY rl_name
                          ),
                          job_inc AS (
                              UPDATE surefire_jobs SET running_count = surefire_jobs.running_count + ji.cnt
                              FROM job_increments ji WHERE surefire_jobs.name = ji.claim_job_name
                              RETURNING 1
                          ),
                          queue_inc AS (
                              UPDATE surefire_queues SET running_count = surefire_queues.running_count + qi.cnt
                              FROM queue_increments qi WHERE surefire_queues.name = qi.claim_queue_name
                              RETURNING 1
                          ),
                          rl_inc AS (
                              UPDATE surefire_rate_limits SET
                                  previous_count = CASE
                                      WHEN window_start IS NULL THEN 0
                                      WHEN EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 >= "window" * 2 THEN 0
                                      WHEN EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 >= "window" THEN current_count
                                      ELSE previous_count
                                  END,
                                  current_count = CASE
                                      WHEN window_start IS NULL THEN i.cnt
                                      WHEN EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 >= "window" THEN i.cnt
                                      ELSE current_count + i.cnt
                                  END,
                                  window_start = CASE
                                      WHEN window_start IS NULL THEN NOW()
                                      WHEN EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 >= "window" * 2 THEN
                                          window_start + (FLOOR(EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 / "window") * "window" / 10000000.0) * INTERVAL '1 second'
                                      WHEN EXTRACT(EPOCH FROM (NOW() - window_start)) * 10000000 >= "window" THEN
                                          window_start + ("window" / 10000000.0) * INTERVAL '1 second'
                                      ELSE window_start
                                  END
                              FROM rl_increments i WHERE surefire_rate_limits.name = i.rl_name
                              RETURNING 1
                          )
                          SELECT id, job_name, status, arguments, result, reason, progress,
                              created_at, started_at, completed_at, canceled_at, node_name, attempt,
                              trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id,
                              root_run_id, rerun_of_run_id, not_before, not_after, priority,
                              deduplication_id, last_heartbeat_at, batch_id
                          FROM claimed
                          """;

        cmd.Parameters.AddWithValue("node_name", nodeName);
        cmd.Parameters.AddWithValue("job_names", jobNames.ToArray());
        cmd.Parameters.AddWithValue("queue_names", queueNames.ToArray());
        cmd.Parameters.AddWithValue("max_count", maxCount);

        var claimed = new List<JobRun>();
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                claimed.Add(ReadRun(reader));
            }
        }

        if (claimed.Count > 0)
        {
            var now = timeProvider.GetUtcNow();
            var statusEvents = new List<RunEvent>(claimed.Count);
            foreach (var run in claimed)
            {
                statusEvents.Add(RunStatusEvents.Create(run.Id, run.Attempt, run.Status, now));
            }

            await InsertEventsAsync(conn, tx, statusEvents, cancellationToken);
        }

        await tx.CommitAsync(cancellationToken);
        return claimed;
    }

    private async Task CreateRunsAsyncCore(IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        if (runs.Count == 0 && (initialEvents is null || initialEvents.Count == 0))
        {
            return;
        }

        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);

        await CreateRunsCoreInTransactionAsync(conn, tx, runs, cancellationToken);
        await InsertEventsAsync(conn, tx, initialEvents, cancellationToken);

        await tx.CommitAsync(cancellationToken);
    }

    private async Task CreateRunsCoreInTransactionAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        IReadOnlyList<JobRun> runs, CancellationToken cancellationToken)
    {
        if (runs.Count == 0)
        {
            return;
        }

        // Pre-lock jobs rows whose counters this statement mutates, in name order, before the
        // INSERT. ORDER BY ensures concurrent batch creators serialize against claim/transition
        // paths instead of deadlocking.
        var lockNames = runs
            .Where(r => !r.Status.IsTerminal)
            .Select(r => r.JobName)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (lockNames.Length > 0)
        {
            await using var lockCmd = CreateCommand(conn);
            lockCmd.Transaction = tx;
            lockCmd.CommandText =
                "SELECT 1 FROM surefire_jobs WHERE name = ANY(@names) ORDER BY name FOR UPDATE";
            lockCmd.Parameters.AddWithValue("names", lockNames);
            await lockCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        var n = runs.Count;
        var ids = new string[n];
        var jobNames = new string[n];
        var statuses = new int[n];
        var arguments = new string?[n];
        var results = new string?[n];
        var reasons = new string?[n];
        var progresses = new double[n];
        var createdAts = new DateTimeOffset[n];
        var startedAts = new DateTimeOffset?[n];
        var completedAts = new DateTimeOffset?[n];
        var CanceledAts = new DateTimeOffset?[n];
        var nodeNames = new string?[n];
        var attempts = new int[n];
        var traceIds = new string?[n];
        var spanIds = new string?[n];
        var parentTraceIds = new string?[n];
        var parentSpanIds = new string?[n];
        var parentRunIds = new string?[n];
        var rootRunIds = new string?[n];
        var rerunOfRunIds = new string?[n];
        var notBefores = new DateTimeOffset[n];
        var notAfters = new DateTimeOffset?[n];
        var priorities = new int[n];
        var deduplicationIds = new string?[n];
        var lastHeartbeatAts = new DateTimeOffset?[n];
        var batchIds = new string?[n];

        for (var i = 0; i < n; i++)
        {
            var r = runs[i];
            ids[i] = r.Id;
            jobNames[i] = r.JobName;
            statuses[i] = (int)r.Status;
            arguments[i] = r.Arguments;
            results[i] = r.Result;
            reasons[i] = r.Reason;
            progresses[i] = r.Progress;
            createdAts[i] = r.CreatedAt;
            startedAts[i] = r.StartedAt;
            completedAts[i] = r.CompletedAt;
            CanceledAts[i] = r.CanceledAt;
            nodeNames[i] = r.NodeName;
            attempts[i] = r.Attempt;
            traceIds[i] = r.TraceId;
            spanIds[i] = r.SpanId;
            parentTraceIds[i] = r.ParentTraceId;
            parentSpanIds[i] = r.ParentSpanId;
            parentRunIds[i] = r.ParentRunId;
            rootRunIds[i] = r.RootRunId;
            rerunOfRunIds[i] = r.RerunOfRunId;
            notBefores[i] = r.NotBefore;
            notAfters[i] = r.NotAfter;
            priorities[i] = r.Priority;
            deduplicationIds[i] = r.DeduplicationId;
            lastHeartbeatAts[i] = r.LastHeartbeatAt;
            batchIds[i] = r.BatchId;
        }

        // UNNEST keeps the SQL text stable across input sizes so PG's plan cache hits.
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          INSERT INTO surefire_runs (
                              id, job_name, status, arguments, result, reason, progress,
                              created_at, started_at, completed_at, canceled_at, node_name,
                              attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                              rerun_of_run_id, not_before, not_after, priority, deduplication_id,
                              last_heartbeat_at, batch_id
                          )
                          SELECT
                              input.id, input.job_name, input.status, input.arguments, input.result, input.reason, input.progress,
                              input.created_at, input.started_at, input.completed_at, input.canceled_at, input.node_name,
                              input.attempt, input.trace_id, input.span_id, input.parent_trace_id, input.parent_span_id,
                              input.parent_run_id, input.root_run_id, input.rerun_of_run_id, input.not_before, input.not_after,
                              input.priority, input.deduplication_id, input.last_heartbeat_at,
                              input.batch_id
                          FROM UNNEST(
                              @ids, @job_names, @statuses, @arguments, @results, @reasons, @progresses,
                              @created_ats, @started_ats, @completed_ats, @canceled_ats, @node_names,
                              @attempts, @trace_ids, @span_ids, @parent_trace_ids, @parent_span_ids, @parent_run_ids, @root_run_ids,
                              @rerun_of_run_ids, @not_befores, @not_afters, @priorities, @deduplication_ids,
                              @last_heartbeat_ats, @batch_ids
                          ) AS input(
                              id, job_name, status, arguments, result, reason, progress,
                              created_at, started_at, completed_at, canceled_at, node_name,
                              attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                              rerun_of_run_id, not_before, not_after, priority, deduplication_id,
                              last_heartbeat_at, batch_id
                          )
                          """;

        cmd.Parameters.AddWithValue("ids", ids);
        cmd.Parameters.AddWithValue("job_names", jobNames);
        cmd.Parameters.AddWithValue("statuses", statuses);
        cmd.Parameters.AddWithValue("arguments", arguments);
        cmd.Parameters.AddWithValue("results", results);
        cmd.Parameters.AddWithValue("reasons", reasons);
        cmd.Parameters.AddWithValue("progresses", progresses);
        cmd.Parameters.AddWithValue("created_ats", createdAts);
        cmd.Parameters.AddWithValue("started_ats", startedAts);
        cmd.Parameters.AddWithValue("completed_ats", completedAts);
        cmd.Parameters.AddWithValue("canceled_ats", CanceledAts);
        cmd.Parameters.AddWithValue("node_names", nodeNames);
        cmd.Parameters.AddWithValue("attempts", attempts);
        cmd.Parameters.AddWithValue("trace_ids", traceIds);
        cmd.Parameters.AddWithValue("span_ids", spanIds);
        cmd.Parameters.AddWithValue("parent_trace_ids", parentTraceIds);
        cmd.Parameters.AddWithValue("parent_span_ids", parentSpanIds);
        cmd.Parameters.AddWithValue("parent_run_ids", parentRunIds);
        cmd.Parameters.AddWithValue("root_run_ids", rootRunIds);
        cmd.Parameters.AddWithValue("rerun_of_run_ids", rerunOfRunIds);
        cmd.Parameters.AddWithValue("not_befores", notBefores);
        cmd.Parameters.AddWithValue("not_afters", notAfters);
        cmd.Parameters.AddWithValue("priorities", priorities);
        cmd.Parameters.AddWithValue("deduplication_ids", deduplicationIds);
        cmd.Parameters.AddWithValue("last_heartbeat_ats", lastHeartbeatAts);
        cmd.Parameters.AddWithValue("batch_ids", batchIds);
        await cmd.ExecuteNonQueryAsync(cancellationToken);

        // Maintain non_terminal_count atomically with the run inserts. Group inserted non-terminal
        // runs per job and apply a single UPDATE per unique job_name.
        var increments = runs
            .Where(r => !r.Status.IsTerminal)
            .GroupBy(r => r.JobName, StringComparer.Ordinal)
            .Select(g => (JobName: g.Key, Count: g.Count()))
            .ToList();
        if (increments.Count > 0)
        {
            var incJobNames = new string[increments.Count];
            var incCounts = new int[increments.Count];
            for (var i = 0; i < increments.Count; i++)
            {
                incJobNames[i] = increments[i].JobName;
                incCounts[i] = increments[i].Count;
            }

            await using var incCmd = CreateCommand(conn);
            incCmd.Transaction = tx;
            incCmd.CommandText = """
                                 UPDATE surefire_jobs SET non_terminal_count = surefire_jobs.non_terminal_count + v.cnt
                                 FROM (SELECT * FROM UNNEST(@inc_names, @inc_counts) AS t(name, cnt)) v
                                 WHERE surefire_jobs.name = v.name
                                 """;
            incCmd.Parameters.AddWithValue("inc_names", incJobNames);
            incCmd.Parameters.AddWithValue("inc_counts", incCounts);
            await incCmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task<bool> TryCreateRunAsyncCore(JobRun run, int? maxActiveForJob,
        DateTimeOffset? lastCronFireAt,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var tx = await conn.BeginTransactionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;

        // Canonical jobs-first lock so the counter UPDATE never upgrades a lock against a
        // concurrent claim/transition. Missing job row (late registration) is a no-op.
        await using (var lockCmd = CreateCommand(conn))
        {
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
            // Capacity check reads the maintained counter on the locked job row (no scan,
            // no deadlock vs concurrent transitions). Missing job row maps via COALESCE to
            // the "enabled, zero active" defaults per the TryCreateRunAsync contract.
            conditions.Add("""
                           COALESCE((SELECT non_terminal_count FROM surefire_jobs WHERE name = @job_name), 0) < @max_active
                           AND COALESCE((SELECT is_enabled FROM surefire_jobs WHERE name = @job_name), TRUE) = TRUE
                           """);
            cmd.Parameters.AddWithValue("max_active", maxActiveForJob.Value);
        }

        if (conditions.Count == 0)
        {
            cmd.CommandText = """
                              INSERT INTO surefire_runs (
                                  id, job_name, status, arguments, result, reason, progress,
                                  created_at, started_at, completed_at, canceled_at, node_name,
                                  attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                                  rerun_of_run_id, not_before, not_after, priority, deduplication_id,
                                  last_heartbeat_at, batch_id
                              ) VALUES (
                                  @id, @job_name, @status, @arguments, @result, @reason, @progress,
                                  @created_at, @started_at, @completed_at, @canceled_at, @node_name,
                                  @attempt, @trace_id, @span_id, @parent_trace_id, @parent_span_id, @parent_run_id, @root_run_id,
                                  @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id,
                                  @last_heartbeat_at, @batch_id
                              )
                              ON CONFLICT DO NOTHING
                              """;
        }
        else
        {
            var whereClause = string.Join(" AND ", conditions);
            cmd.CommandText = $"""
                               INSERT INTO surefire_runs (
                                   id, job_name, status, arguments, result, reason, progress,
                                   created_at, started_at, completed_at, canceled_at, node_name,
                                   attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                                   rerun_of_run_id, not_before, not_after, priority, deduplication_id,
                                   last_heartbeat_at, batch_id
                               )
                               SELECT
                                   @id, @job_name, @status, @arguments, @result, @reason, @progress,
                                   @created_at, @started_at, @completed_at, @canceled_at, @node_name,
                                   @attempt, @trace_id, @span_id, @parent_trace_id, @parent_span_id, @parent_run_id, @root_run_id,
                                   @rerun_of_run_id, @not_before, @not_after, @priority, @deduplication_id,
                                   @last_heartbeat_at, @batch_id
                               WHERE {whereClause}
                               ON CONFLICT DO NOTHING
                               """;
        }

        AddRunParams(cmd, "", run);

        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);

        if (rows > 0)
        {
            await InsertEventsAsync(conn, tx, initialEvents, cancellationToken);

            // Maintain non_terminal_count atomically with the insert.
            if (!run.Status.IsTerminal)
            {
                await using var countCmd = CreateCommand(conn);
                countCmd.Transaction = tx;
                countCmd.CommandText =
                    "UPDATE surefire_jobs SET non_terminal_count = non_terminal_count + 1 WHERE name = @job_name";
                countCmd.Parameters.AddWithValue("job_name", run.JobName);
                await countCmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        if (rows > 0 && lastCronFireAt is { } fireAt)
        {
            await using var updateCmd = CreateCommand(conn);
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

    private async Task InsertEventsAsync(NpgsqlConnection conn, NpgsqlTransaction tx,
        IReadOnlyList<RunEvent>? events, CancellationToken cancellationToken)
    {
        if (events is null || events.Count == 0)
        {
            return;
        }

        var runIds = new string[events.Count];
        var types = new short[events.Count];
        var payloads = new string[events.Count];
        var createdAts = new DateTimeOffset[events.Count];
        var attempts = new int[events.Count];

        for (var i = 0; i < events.Count; i++)
        {
            var e = events[i];
            runIds[i] = e.RunId;
            types[i] = (short)e.EventType;
            payloads[i] = e.Payload;
            createdAts[i] = e.CreatedAt;
            attempts[i] = e.Attempt;
        }

        // UNNEST keeps the SQL text stable across input sizes so PG's plan cache hits.
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          INSERT INTO surefire_events (run_id, event_type, payload, created_at, attempt)
                          SELECT * FROM UNNEST(@run_ids, @types, @payloads, @created_ats, @attempts)
                          """;
        cmd.Parameters.AddWithValue("run_ids", runIds);
        cmd.Parameters.AddWithValue("types", types);
        cmd.Parameters.AddWithValue("payloads", payloads);
        cmd.Parameters.AddWithValue("created_ats", createdAts);
        cmd.Parameters.AddWithValue("attempts", attempts);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
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
            parts.Add("job_name = @filter_job_name");
            cmd.Parameters.AddWithValue("filter_job_name", filter.JobName);
        }

        if (filter.JobNameContains is { })
        {
            parts.Add("job_name ILIKE '%' || @filter_job_name_contains || '%' ESCAPE '\\'");
            cmd.Parameters.AddWithValue("filter_job_name_contains", EscapeLike(filter.JobNameContains));
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
            parts.Add("completed_at > @filter_completed_after");
            cmd.Parameters.AddWithValue("filter_completed_after", filter.CompletedAfter.Value);
        }

        if (filter.LastHeartbeatBefore is { })
        {
            parts.Add("last_heartbeat_at < @filter_hb_before");
            cmd.Parameters.AddWithValue("filter_hb_before", filter.LastHeartbeatBefore.Value);
        }

        if (filter.BatchId is { })
        {
            parts.Add("batch_id = @batch_id_filter");
            cmd.Parameters.AddWithValue("batch_id_filter", filter.BatchId);
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
        cmd.Parameters.AddWithValue($"{prefix}reason", (object?)run.Reason ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}progress", run.Progress);
        cmd.Parameters.AddWithValue($"{prefix}created_at", run.CreatedAt);
        cmd.Parameters.AddWithValue($"{prefix}started_at", run.StartedAt.HasValue ? run.StartedAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}completed_at",
            run.CompletedAt.HasValue ? run.CompletedAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}canceled_at",
            run.CanceledAt.HasValue ? run.CanceledAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}node_name", (object?)run.NodeName ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}attempt", run.Attempt);
        cmd.Parameters.AddWithValue($"{prefix}trace_id", (object?)run.TraceId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}span_id", (object?)run.SpanId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}parent_trace_id", (object?)run.ParentTraceId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}parent_span_id", (object?)run.ParentSpanId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}parent_run_id", (object?)run.ParentRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}root_run_id", (object?)run.RootRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}rerun_of_run_id", (object?)run.RerunOfRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}not_before", run.NotBefore);
        cmd.Parameters.AddWithValue($"{prefix}not_after", run.NotAfter.HasValue ? run.NotAfter.Value : DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}priority", run.Priority);
        cmd.Parameters.AddWithValue($"{prefix}deduplication_id", (object?)run.DeduplicationId ?? DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}last_heartbeat_at",
            run.LastHeartbeatAt.HasValue ? run.LastHeartbeatAt.Value : DBNull.Value);
        cmd.Parameters.AddWithValue($"{prefix}batch_id", (object?)run.BatchId ?? DBNull.Value);
    }

    private JobRun ReadRun(NpgsqlDataReader reader)
    {
        var ord = (_runOrdinals ??= new(RunOrdinals.From(reader))).Value;
        return new()
        {
            Id = reader.GetString(ord.Id),
            JobName = reader.GetString(ord.JobName),
            Status = (JobStatus)reader.GetInt32(ord.Status),
            Progress = reader.GetDouble(ord.Progress),
            CreatedAt = reader.GetFieldValue<DateTimeOffset>(ord.CreatedAt),
            Attempt = reader.GetInt32(ord.Attempt),
            NotBefore = reader.GetFieldValue<DateTimeOffset>(ord.NotBefore),
            Priority = reader.GetInt32(ord.Priority),
            Arguments = reader.IsDBNull(ord.Arguments) ? null : reader.GetString(ord.Arguments),
            Result = reader.IsDBNull(ord.Result) ? null : reader.GetString(ord.Result),
            Reason = reader.IsDBNull(ord.Reason) ? null : reader.GetString(ord.Reason),
            StartedAt = reader.IsDBNull(ord.StartedAt) ? null : reader.GetFieldValue<DateTimeOffset>(ord.StartedAt),
            CompletedAt = reader.IsDBNull(ord.CompletedAt)
                ? null
                : reader.GetFieldValue<DateTimeOffset>(ord.CompletedAt),
            CanceledAt = reader.IsDBNull(ord.CanceledAt)
                ? null
                : reader.GetFieldValue<DateTimeOffset>(ord.CanceledAt),
            NodeName = reader.IsDBNull(ord.NodeName) ? null : reader.GetString(ord.NodeName),
            TraceId = reader.IsDBNull(ord.TraceId) ? null : reader.GetString(ord.TraceId),
            SpanId = reader.IsDBNull(ord.SpanId) ? null : reader.GetString(ord.SpanId),
            ParentTraceId = reader.IsDBNull(ord.ParentTraceId) ? null : reader.GetString(ord.ParentTraceId),
            ParentSpanId = reader.IsDBNull(ord.ParentSpanId) ? null : reader.GetString(ord.ParentSpanId),
            ParentRunId = reader.IsDBNull(ord.ParentRunId) ? null : reader.GetString(ord.ParentRunId),
            RootRunId = reader.IsDBNull(ord.RootRunId) ? null : reader.GetString(ord.RootRunId),
            RerunOfRunId = reader.IsDBNull(ord.RerunOfRunId) ? null : reader.GetString(ord.RerunOfRunId),
            NotAfter = reader.IsDBNull(ord.NotAfter) ? null : reader.GetFieldValue<DateTimeOffset>(ord.NotAfter),
            DeduplicationId = reader.IsDBNull(ord.DeduplicationId) ? null : reader.GetString(ord.DeduplicationId),
            LastHeartbeatAt = reader.IsDBNull(ord.LastHeartbeatAt)
                ? null
                : reader.GetFieldValue<DateTimeOffset>(ord.LastHeartbeatAt),
            BatchId = reader.IsDBNull(ord.BatchId) ? null : reader.GetString(ord.BatchId)
        };
    }

    private JobBatch ReadBatch(NpgsqlDataReader reader)
    {
        var ord = (_batchOrdinals ??= new(BatchOrdinals.From(reader))).Value;
        return new()
        {
            Id = reader.GetString(ord.Id),
            Status = (JobStatus)reader.GetInt16(ord.Status),
            Total = reader.GetInt32(ord.Total),
            Succeeded = reader.GetInt32(ord.Succeeded),
            Failed = reader.GetInt32(ord.Failed),
            Canceled = reader.IsDBNull(ord.Canceled) ? 0 : reader.GetInt32(ord.Canceled),
            CreatedAt = reader.GetFieldValue<DateTimeOffset>(ord.CreatedAt),
            CompletedAt = reader.IsDBNull(ord.CompletedAt)
                ? null
                : reader.GetFieldValue<DateTimeOffset>(ord.CompletedAt)
        };
    }

    private static JobDefinition ReadJob(NpgsqlDataReader reader)
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
            Tags = reader.GetFieldValue<string[]>(reader.GetOrdinal("tags")),
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
                : JsonSerializer.Deserialize(reader.GetString(retryPolicyCol),
                      SurefireJsonContext.Default.RetryPolicy)
                  ?? throw new InvalidOperationException("Retry policy payload was null."),
            Queue = reader.IsDBNull(queueCol) ? null : reader.GetString(queueCol),
            RateLimitName = reader.IsDBNull(rateLimitCol) ? null : reader.GetString(rateLimitCol),
            ArgumentsSchema = reader.IsDBNull(schemaCol) ? null : reader.GetString(schemaCol),
            LastHeartbeatAt = reader.IsDBNull(heartbeatCol) ? null : reader.GetFieldValue<DateTimeOffset>(heartbeatCol),
            LastCronFireAt = reader.IsDBNull(cronFireCol) ? null : reader.GetFieldValue<DateTimeOffset>(cronFireCol)
        };
    }

    private static QueueDefinition ReadQueue(NpgsqlDataReader reader)
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
            LastHeartbeatAt = reader.IsDBNull(heartbeatCol) ? null : reader.GetFieldValue<DateTimeOffset>(heartbeatCol)
        };
    }

    private NodeInfo ReadNode(NpgsqlDataReader reader)
    {
        var ord = (_nodeOrdinals ??= new(NodeOrdinals.From(reader))).Value;
        return new()
        {
            Name = reader.GetString(ord.Name),
            StartedAt = reader.GetFieldValue<DateTimeOffset>(ord.StartedAt),
            LastHeartbeatAt = reader.GetFieldValue<DateTimeOffset>(ord.LastHeartbeatAt),
            RunningCount = reader.GetInt32(ord.RunningCount),
            RegisteredJobNames = reader.GetFieldValue<string[]>(ord.RegisteredJobNames),
            RegisteredQueueNames = reader.GetFieldValue<string[]>(ord.RegisteredQueueNames)
        };
    }

    private RunEvent ReadEvent(NpgsqlDataReader reader)
    {
        var ord = (_eventOrdinals ??= new(EventOrdinals.From(reader))).Value;
        return new()
        {
            Id = reader.GetInt64(ord.Id),
            RunId = reader.GetString(ord.RunId),
            EventType = (RunEventType)reader.GetInt16(ord.EventType),
            Payload = reader.GetString(ord.Payload),
            CreatedAt = reader.GetFieldValue<DateTimeOffset>(ord.CreatedAt),
            Attempt = reader.GetInt32(ord.Attempt)
        };
    }

    private static string EscapeLike(string input) =>
        input.Replace(@"\", @"\\").Replace("%", @"\%").Replace("_", @"\_");

    private sealed class OrdinalCache<T>(T value) where T : struct
    {
        public T Value { get; } = value;
    }

    private readonly record struct RunOrdinals(
        int Id,
        int JobName,
        int Status,
        int Progress,
        int CreatedAt,
        int Attempt,
        int NotBefore,
        int Priority,
        int Arguments,
        int Result,
        int Reason,
        int StartedAt,
        int CompletedAt,
        int CanceledAt,
        int NodeName,
        int TraceId,
        int SpanId,
        int ParentTraceId,
        int ParentSpanId,
        int ParentRunId,
        int RootRunId,
        int RerunOfRunId,
        int NotAfter,
        int DeduplicationId,
        int LastHeartbeatAt,
        int BatchId)
    {
        public static RunOrdinals From(NpgsqlDataReader r) => new(
            r.GetOrdinal("id"), r.GetOrdinal("job_name"), r.GetOrdinal("status"),
            r.GetOrdinal("progress"), r.GetOrdinal("created_at"), r.GetOrdinal("attempt"),
            r.GetOrdinal("not_before"), r.GetOrdinal("priority"),
            r.GetOrdinal("arguments"), r.GetOrdinal("result"), r.GetOrdinal("reason"),
            r.GetOrdinal("started_at"), r.GetOrdinal("completed_at"), r.GetOrdinal("canceled_at"),
            r.GetOrdinal("node_name"), r.GetOrdinal("trace_id"), r.GetOrdinal("span_id"),
            r.GetOrdinal("parent_trace_id"), r.GetOrdinal("parent_span_id"),
            r.GetOrdinal("parent_run_id"), r.GetOrdinal("root_run_id"),
            r.GetOrdinal("rerun_of_run_id"), r.GetOrdinal("not_after"),
            r.GetOrdinal("deduplication_id"), r.GetOrdinal("last_heartbeat_at"),
            r.GetOrdinal("batch_id"));
    }

    private readonly record struct BatchOrdinals(
        int Id,
        int Status,
        int Total,
        int Succeeded,
        int Failed,
        int Canceled,
        int CreatedAt,
        int CompletedAt)
    {
        public static BatchOrdinals From(NpgsqlDataReader r) => new(
            r.GetOrdinal("id"), r.GetOrdinal("status"), r.GetOrdinal("total"),
            r.GetOrdinal("succeeded"), r.GetOrdinal("failed"), r.GetOrdinal("canceled"),
            r.GetOrdinal("created_at"), r.GetOrdinal("completed_at"));
    }

    private readonly record struct EventOrdinals(
        int Id,
        int RunId,
        int EventType,
        int Payload,
        int CreatedAt,
        int Attempt)
    {
        public static EventOrdinals From(NpgsqlDataReader r) => new(
            r.GetOrdinal("id"), r.GetOrdinal("run_id"), r.GetOrdinal("event_type"),
            r.GetOrdinal("payload"), r.GetOrdinal("created_at"), r.GetOrdinal("attempt"));
    }

    private readonly record struct NodeOrdinals(
        int Name,
        int StartedAt,
        int LastHeartbeatAt,
        int RunningCount,
        int RegisteredJobNames,
        int RegisteredQueueNames)
    {
        public static NodeOrdinals From(NpgsqlDataReader r) => new(
            r.GetOrdinal("name"), r.GetOrdinal("started_at"),
            r.GetOrdinal("last_heartbeat_at"), r.GetOrdinal("running_count"),
            r.GetOrdinal("registered_job_names"), r.GetOrdinal("registered_queue_names"));
    }
}
