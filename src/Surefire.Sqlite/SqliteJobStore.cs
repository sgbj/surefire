using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Surefire.Sqlite;

internal sealed class SqliteJobStore(
    string connectionString,
    TimeSpan? commandTimeout,
    TimeProvider timeProvider) : IJobStore
{
    // Connection-scoped pragmas, applied on every connection open. Values here are tuned for
    // a read/write balanced production workload; they're intentionally not user-configurable
    // until a real deployment surfaces a case where the defaults don't fit.
    //  - foreign_keys=ON: foreign-key enforcement defaults off per connection in SQLite.
    //  - busy_timeout=24000: retry-on-busy at the SQLite layer; paired with IsTransientException.
    //  - temp_store=MEMORY: large ORDER BY / stats / retention sorts stay resident.
    //  - mmap_size=268435456 (256 MiB): hot pages served from mmap on read-heavy queries.
    //  - wal_autocheckpoint=1000: bounds WAL window size between automatic checkpoints.
    private const string ConnectionPragmas = """
                                             PRAGMA foreign_keys=ON;
                                             PRAGMA busy_timeout=24000;
                                             PRAGMA temp_store=MEMORY;
                                             PRAGMA mmap_size=268435456;
                                             PRAGMA wal_autocheckpoint=1000;
                                             """;

    // Database-scoped pragmas, applied during migration. These are intentionally not set on
    // each open to avoid toggling safety settings while another operation may hold a transaction.
    //  - synchronous=NORMAL: right balance under WAL (fsync on checkpoint, not every commit).
    //  - journal_size_limit=67108864 (64 MiB): caps WAL file growth after a write burst.
    private const string DatabasePragmas = """
                                           PRAGMA synchronous=NORMAL;
                                           PRAGMA journal_size_limit=67108864;
                                           """;

    private readonly string _connectionString = BuildConnectionString(connectionString, commandTimeout);

    internal int? CommandTimeoutSeconds { get; } =
        CommandTimeouts.ToSeconds(commandTimeout, nameof(commandTimeout));

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, "SELECT 1;");
        _ = await cmd.ExecuteScalarAsync(cancellationToken);
    }

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);

        // WAL is per-file persistent and cannot be set inside a transaction. Database pragmas
        // are idempotent and safe to apply before the migration lock.
        await using (var walCmd = CreateCommand(conn, "PRAGMA journal_mode=WAL;"))
        {
            await walCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var dbPragmaCmd = CreateCommand(conn, DatabasePragmas))
        {
            await dbPragmaCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // Bootstrap outside the lock so the optimistic check below is safe on a fresh database.
        // CREATE TABLE IF NOT EXISTS is idempotent and serializes through SQLite's writer lock.
        await using (var schemaCmd = CreateCommand(conn,
                         "CREATE TABLE IF NOT EXISTS surefire_schema_migrations (version INTEGER NOT NULL PRIMARY KEY);"))
        {
            await schemaCmd.ExecuteNonQueryAsync(cancellationToken);
        }

        // Optimistic fast path: typical boots find v1 already present and return without
        // acquiring the writer lock. The slow path below re-checks under BEGIN IMMEDIATE.
        if (await HasMigrationVersionAsync(conn, 1, cancellationToken))
        {
            return;
        }

        // BEGIN IMMEDIATE acquires the writer lock so two nodes serialize before touching
        // schema. v1's IF NOT EXISTS DDL would race-survive without the lock, but future
        // migrations (e.g. ALTER TABLE ADD COLUMN, no IF NOT EXISTS variant) won't.
        // busy_timeout makes the BEGIN wait rather than throw SQLITE_BUSY.
        await using var tx = conn.BeginTransaction(false);

        // Re-check under the lock; another node may have completed v1 between the optimistic
        // read and BEGIN IMMEDIATE acquiring the writer lock.
        if (await HasMigrationVersionAsync(conn, 1, cancellationToken, tx))
        {
            await tx.CommitAsync(cancellationToken);
            return;
        }

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
                                                         last_cron_fire_at TEXT,
                                                         running_count INTEGER NOT NULL DEFAULT 0,
                                                         non_terminal_count INTEGER NOT NULL DEFAULT 0
                                                     );
                                                     CREATE TABLE IF NOT EXISTS surefire_runs (
                                                         id TEXT PRIMARY KEY,
                                                         job_name TEXT NOT NULL,
                                                         status INTEGER NOT NULL DEFAULT 0,
                                                         arguments TEXT,
                                                         result TEXT,
                                                         reason TEXT,
                                                         progress REAL NOT NULL DEFAULT 0,
                                                         created_at TEXT NOT NULL,
                                                         started_at TEXT,
                                                         completed_at TEXT,
                                                         canceled_at TEXT,
                                                         node_name TEXT,
                                                         attempt INTEGER NOT NULL DEFAULT 0,
                                                         trace_id TEXT,
                                                         span_id TEXT,
                                                         parent_trace_id TEXT,
                                                         parent_span_id TEXT,
                                                         parent_run_id TEXT,
                                                         root_run_id TEXT,
                                                         rerun_of_run_id TEXT,
                                                         not_before TEXT NOT NULL,
                                                         not_after TEXT,
                                                         priority INTEGER NOT NULL DEFAULT 0,
                                                         deduplication_id TEXT,
                                                         last_heartbeat_at TEXT,
                                                         batch_id TEXT
                                                     );
                                                     CREATE TABLE IF NOT EXISTS surefire_batches (
                                                         id TEXT PRIMARY KEY,
                                                         status INTEGER NOT NULL DEFAULT 0,
                                                         total INTEGER NOT NULL DEFAULT 0,
                                                         succeeded INTEGER NOT NULL DEFAULT 0,
                                                         failed INTEGER NOT NULL DEFAULT 0,
                                                         canceled INTEGER NOT NULL DEFAULT 0,
                                                         created_at TEXT NOT NULL,
                                                         completed_at TEXT
                                                     );
                                                     CREATE INDEX IF NOT EXISTS ix_runs_claim
                                                         ON surefire_runs (priority DESC, not_before, id) WHERE status = 0;
                                                     CREATE INDEX IF NOT EXISTS ix_runs_job_name_status
                                                         ON surefire_runs (job_name, status);
                                                     CREATE INDEX IF NOT EXISTS ix_runs_created_at
                                                         ON surefire_runs (created_at DESC);
                                                     CREATE INDEX IF NOT EXISTS ix_runs_parent_run_id
                                                         ON surefire_runs (parent_run_id, created_at, id);
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
                                                         ON surefire_runs (not_after) WHERE status = 0 AND not_after IS NOT NULL;
                                                     CREATE INDEX IF NOT EXISTS ix_runs_batch_id
                                                         ON surefire_runs (batch_id);
                                                     -- Backs GetStaleRunningRunIdsAsync: oldest-heartbeat-first
                                                     -- range scan over Running rows, bounded by the result size.
                                                     CREATE INDEX IF NOT EXISTS ix_runs_stale_heartbeat
                                                         ON surefire_runs (last_heartbeat_at) WHERE status = 1;
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
                                                         last_heartbeat_at TEXT,
                                                         running_count INTEGER NOT NULL DEFAULT 0
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
                                                     """, tx);
        await ddlCmd.ExecuteNonQueryAsync(cancellationToken);

        await tx.CommitAsync(cancellationToken);
    }

    public async Task UpsertJobsAsync(IReadOnlyList<JobDefinition> jobs,
        CancellationToken cancellationToken = default)
    {
        if (jobs.Count == 0)
        {
            return;
        }

        // One JSON payload, one json_each, one INSERT...ON CONFLICT, one statement. tags and
        // retry_policy stay as JSON text. is_enabled is omitted from DO UPDATE SET so existing
        // toggles stick.
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  INSERT INTO surefire_jobs (
                                                      name, description, tags, cron_expression, time_zone_id,
                                                      timeout_ticks, max_concurrency, priority, retry_policy,
                                                      is_continuous, queue, rate_limit_name, is_enabled,
                                                      misfire_policy, fire_all_limit, arguments_schema, last_heartbeat_at
                                                  )
                                                  SELECT
                                                      json_extract(e.value, '$.name'),
                                                      json_extract(e.value, '$.description'),
                                                      json_extract(e.value, '$.tags'),
                                                      json_extract(e.value, '$.cronExpression'),
                                                      json_extract(e.value, '$.timeZoneId'),
                                                      json_extract(e.value, '$.timeout'),
                                                      json_extract(e.value, '$.maxConcurrency'),
                                                      json_extract(e.value, '$.priority'),
                                                      json_extract(e.value, '$.retryPolicy'),
                                                      json_extract(e.value, '$.isContinuous'),
                                                      json_extract(e.value, '$.queue'),
                                                      json_extract(e.value, '$.rateLimitName'),
                                                      json_extract(e.value, '$.isEnabled'),
                                                      json_extract(e.value, '$.misfirePolicy'),
                                                      json_extract(e.value, '$.fireAllLimit'),
                                                      json_extract(e.value, '$.argumentsSchema'),
                                                      @now
                                                  FROM json_each(@payload) AS e
                                                  WHERE json_type(e.value) = 'object'
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
                                                      last_heartbeat_at = @now
                                                  """);
        cmd.Parameters.AddWithValue("@payload", UpsertPayloadFactory.SerializeJobs(jobs));
        cmd.Parameters.AddWithValue("@now", FormatTimestamp(timeProvider.GetUtcNow()));
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
        => CreateRunsAsyncCore(runs, initialEvents, cancellationToken);

    public Task<bool> TryCreateRunAsync(
        JobRun run, int? maxActiveForJob = null,
        DateTimeOffset? lastCronFireAt = null,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
        => TryCreateRunAsyncCore(run, maxActiveForJob, lastCronFireAt, initialEvents, cancellationToken);

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

    public async Task<IReadOnlyList<JobRun>> GetRunsByIdsAsync(IReadOnlyList<string> ids,
        CancellationToken cancellationToken = default)
    {
        if (ids.Count == 0)
        {
            return [];
        }

        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = new SqliteCommand { Connection = conn };
        var placeholders = new string[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            var p = "@id" + i;
            placeholders[i] = p;
            cmd.Parameters.AddWithValue(p, ids[i]);
        }

        cmd.CommandText = $"SELECT * FROM surefire_runs WHERE id IN ({string.Join(",", placeholders)})";

        // Preserve input order: fetch into a dictionary, then project.
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
                    AND (created_at < @cts OR (created_at = @cts AND id < @cid))
                  ORDER BY created_at DESC, id DESC
                  LIMIT @take
                  """;
        }
        else if (after is { })
        {
            sql = """
                  SELECT * FROM surefire_runs
                  WHERE parent_run_id = @parent
                    AND (created_at > @cts OR (created_at = @cts AND id > @cid))
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

        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, sql);
        cmd.Parameters.AddWithValue("@parent", parentRunId);
        // take+1 lookahead: NextCursor is non-null iff a row exists beyond the page boundary.
        cmd.Parameters.AddWithValue("@take", take + 1);
        if ((after ?? before) is { } c)
        {
            cmd.Parameters.AddWithValue("@cts", FormatTimestamp(c.CreatedAt));
            cmd.Parameters.AddWithValue("@cid", c.Id);
        }

        var items = new List<JobRun>(take + 1);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(ReadRun(reader));
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
        // Parent IDs are immutable; recursion terminates when parent_run_id is null.
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
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
                                                  """);
        cmd.Parameters.AddWithValue("@id", runId);
        var chain = new List<JobRun>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            chain.Add(ReadRun(reader));
        }

        return chain;
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
            where += " AND job_name = @jn";
            cmd.Parameters.AddWithValue("@jn", filter.JobName);
        }

        if (filter.JobNameContains is { })
        {
            where += " AND job_name LIKE @jnc ESCAPE '\\'";
            cmd.Parameters.AddWithValue("@jnc", $"%{EscapeLike(filter.JobNameContains)}%");
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
            where += " AND completed_at > @coa";
            cmd.Parameters.AddWithValue("@coa", FormatTimestamp(filter.CompletedAfter.Value));
        }

        if (filter.LastHeartbeatBefore is { })
        {
            where += " AND last_heartbeat_at < @lhb";
            cmd.Parameters.AddWithValue("@lhb", FormatTimestamp(filter.LastHeartbeatBefore.Value));
        }

        if (filter.BatchId is { })
        {
            where += " AND batch_id = @bid";
            cmd.Parameters.AddWithValue("@bid", filter.BatchId);
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
                                                  SET progress = @p, result = @r, reason = @e,
                                                      trace_id = @ti, span_id = @si, last_heartbeat_at = @lh
                                                  WHERE id = @id AND node_name IS @nn AND status NOT IN (2, 4, 5)
                                                  """);
        cmd.Parameters.AddWithValue("@id", run.Id);
        cmd.Parameters.AddWithValue("@p", run.Progress);
        cmd.Parameters.AddWithValue("@r", (object?)run.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@e", (object?)run.Reason ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ti", (object?)run.TraceId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@si", (object?)run.SpanId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@lh", FormatNullableTimestamp(run.LastHeartbeatAt));
        cmd.Parameters.AddWithValue("@nn", (object?)run.NodeName ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<RunTransitionResult> TryTransitionRunAsync(
        RunStatusTransition transition, CancellationToken cancellationToken = default)
    {
        if (!RunTransitionRules.IsAllowed(transition.ExpectedStatus, transition.NewStatus)
            || !transition.HasRequiredFields())
        {
            return RunTransitionResult.NotApplied;
        }

        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);
        // RETURNING captures the affected job_name for the follow-up counter UPDATEs.
        await using var cmd = CreateCommand(conn, """
                                                  UPDATE surefire_runs
                                                  SET status = @s, reason = @e, result = @r, progress = @p,
                                                      completed_at = COALESCE(@coa, completed_at),
                                                      canceled_at = COALESCE(@caa, canceled_at),
                                                      started_at = COALESCE(@sa, started_at),
                                                      node_name = @nn,
                                                      last_heartbeat_at = COALESCE(@lh, last_heartbeat_at),
                                                      not_before = @nb
                                                  WHERE id = @id AND status = @es AND attempt = @ea
                                                      AND status NOT IN (2, 4, 5)
                                                  RETURNING job_name
                                                  """, tx);
        cmd.Parameters.AddWithValue("@id", transition.RunId);
        cmd.Parameters.AddWithValue("@s", (int)transition.NewStatus);
        cmd.Parameters.AddWithValue("@e", (object?)transition.Reason ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@r", (object?)transition.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@p", transition.Progress);
        cmd.Parameters.AddWithValue("@coa", FormatNullableTimestamp(transition.CompletedAt));
        cmd.Parameters.AddWithValue("@caa", FormatNullableTimestamp(transition.CanceledAt));
        cmd.Parameters.AddWithValue("@sa", FormatNullableTimestamp(transition.StartedAt));
        cmd.Parameters.AddWithValue("@nn", (object?)transition.NodeName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@lh", FormatNullableTimestamp(transition.LastHeartbeatAt));
        cmd.Parameters.AddWithValue("@nb", FormatTimestamp(transition.NotBefore));
        cmd.Parameters.AddWithValue("@es", (int)transition.ExpectedStatus);
        cmd.Parameters.AddWithValue("@ea", transition.ExpectedAttempt);

        string? affectedJobName = null;
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                affectedJobName = reader.GetString(0);
            }
        }

        var updated = affectedJobName is { };
        if (updated && transition.ExpectedStatus == JobStatus.Running)
        {
            await DecrementRunningCountAsync(conn, tx, affectedJobName!, cancellationToken);
        }

        // Any transition INTO a terminal status decrements non_terminal_count.
        if (updated && transition.NewStatus is JobStatus.Succeeded or JobStatus.Failed
                or JobStatus.Canceled)
        {
            await DecrementNonTerminalCountAsync(conn, tx, affectedJobName!, cancellationToken);
        }

        if (updated)
        {
            var transitionEvents = new List<RunEvent>();
            transitionEvents.Add(RunStatusEvents.Create(transition.RunId, transition.ExpectedAttempt,
                transition.NewStatus, timeProvider.GetUtcNow()));
            if (transition.Events is { Count: > 0 })
            {
                transitionEvents.AddRange(transition.Events);
            }

            await InsertEventsAsync(
                conn,
                transitionEvents,
                cancellationToken,
                tx);
        }

        BatchCompletionInfo? batchCompletion = null;
        var newStatus = transition.NewStatus;
        if (updated && (newStatus == JobStatus.Succeeded || newStatus == JobStatus.Canceled ||
                        newStatus == JobStatus.Failed))
        {
            await using var batchIdCmd = CreateCommand(conn, """
                                                             SELECT batch_id FROM surefire_runs WHERE id = @id
                                                             """, tx);
            batchIdCmd.Parameters.AddWithValue("@id", transition.RunId);
            var batchIdObj = await batchIdCmd.ExecuteScalarAsync(cancellationToken);
            var batchId = batchIdObj is string s ? s : null;

            if (batchId is { })
            {
                await using var incrCmd = CreateCommand(conn, """
                                                              UPDATE surefire_batches
                                                              SET succeeded = succeeded + CASE WHEN @s = 2 THEN 1 ELSE 0 END,
                                                                  failed    = failed    + CASE WHEN @s = 5 THEN 1 ELSE 0 END,
                                                                  Canceled = Canceled + CASE WHEN @s = 4 THEN 1 ELSE 0 END
                                                              WHERE id = @id AND status NOT IN (2, 4, 5)
                                                              RETURNING total, succeeded, failed, canceled
                                                              """, tx);
                incrCmd.Parameters.AddWithValue("@id", batchId);
                incrCmd.Parameters.AddWithValue("@s", (int)newStatus);
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

                        await using var completeCmd = CreateCommand(conn, """
                                                                          UPDATE surefire_batches
                                                                          SET status = @st, completed_at = @coa
                                                                          WHERE id = @id AND status NOT IN (2, 4, 5)
                                                                          """, tx);
                        completeCmd.Parameters.AddWithValue("@id", batchId);
                        completeCmd.Parameters.AddWithValue("@st", (int)batchStatus);
                        completeCmd.Parameters.AddWithValue("@coa", FormatTimestamp(completedAt));
                        await completeCmd.ExecuteNonQueryAsync(cancellationToken);

                        batchCompletion = new(batchId, batchStatus, completedAt);
                    }
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return new(updated, batchCompletion);
    }

    public async Task<RunTransitionResult> TryCancelRunAsync(
        string runId,
        int? expectedAttempt = null,
        string? reason = null,
        IReadOnlyList<RunEvent>? events = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);
        var now = FormatTimestamp(timeProvider.GetUtcNow());
        // SQLite's RETURNING reflects post-update values, so read prior status separately.
        await using var priorCmd = CreateCommand(conn, """
                                                       SELECT status, attempt, job_name, batch_id
                                                       FROM surefire_runs
                                                       WHERE id = @id AND status NOT IN (2, 4, 5)
                                                         AND (@expected_attempt IS NULL OR attempt = @expected_attempt)
                                                       """, tx);
        priorCmd.Parameters.AddWithValue("@id", runId);
        priorCmd.Parameters.AddWithValue("@expected_attempt",
            expectedAttempt.HasValue ? expectedAttempt.Value : DBNull.Value);

        int? priorStatus = null;
        int? attempt = null;
        string? jobName = null;
        string? batchId = null;
        await using (var pr = await priorCmd.ExecuteReaderAsync(cancellationToken))
        {
            if (await pr.ReadAsync(cancellationToken))
            {
                priorStatus = pr.GetInt32(0);
                attempt = pr.GetInt32(1);
                jobName = pr.GetString(2);
                batchId = pr.IsDBNull(3) ? null : pr.GetString(3);
            }
        }

        if (attempt is null)
        {
            await tx.CommitAsync(cancellationToken);
            return RunTransitionResult.NotApplied;
        }

        await using var cmd = CreateCommand(conn, """
                                                  UPDATE surefire_runs
                                                  SET status = 4, canceled_at = @now, completed_at = @now,
                                                      reason = COALESCE(@reason, reason)
                                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                                      AND (@expected_attempt IS NULL OR attempt = @expected_attempt)
                                                  """, tx);
        cmd.Parameters.AddWithValue("@id", runId);
        cmd.Parameters.AddWithValue("@now", now);
        cmd.Parameters.AddWithValue("@reason", (object?)reason ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@expected_attempt",
            expectedAttempt.HasValue ? expectedAttempt.Value : DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken);

        if (priorStatus == 1 && jobName is { })
        {
            await DecrementRunningCountAsync(conn, tx, jobName, cancellationToken);
        }

        // Every cancel decrements non_terminal_count.
        if (jobName is { })
        {
            await DecrementNonTerminalCountAsync(conn, tx, jobName, cancellationToken);
        }

        var allEvents = new List<RunEvent>();
        allEvents.Add(RunStatusEvents.Create(runId, attempt.Value, JobStatus.Canceled, timeProvider.GetUtcNow()));
        if (events is { Count: > 0 })
        {
            allEvents.AddRange(events);
        }

        await InsertEventsAsync(conn, allEvents, cancellationToken, tx);

        BatchCompletionInfo? batchCompletion = null;
        if (batchId is { })
        {
            await using var incrCmd = CreateCommand(conn, """
                                                          UPDATE surefire_batches
                                                          SET canceled = canceled + 1
                                                          WHERE id = @id AND status NOT IN (2, 4, 5)
                                                          RETURNING total, succeeded, failed, canceled
                                                          """, tx);
            incrCmd.Parameters.AddWithValue("@id", batchId);
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

                    await using var completeCmd = CreateCommand(conn, """
                                                                      UPDATE surefire_batches
                                                                      SET status = @st, completed_at = @coa
                                                                      WHERE id = @id AND status NOT IN (2, 4, 5)
                                                                      """, tx);
                    completeCmd.Parameters.AddWithValue("@id", batchId);
                    completeCmd.Parameters.AddWithValue("@st", (int)batchStatus);
                    completeCmd.Parameters.AddWithValue("@coa", FormatTimestamp(completedAt));
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);

                    batchCompletion = new(batchId, batchStatus, completedAt);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return new(true, batchCompletion);
    }

    public Task<IReadOnlyList<JobRun>> ClaimRunsAsync(
        string nodeName,
        IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames,
        int maxCount,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCount, 1);

        if (jobNames.Count == 0 || queueNames.Count == 0)
        {
            return Task.FromResult<IReadOnlyList<JobRun>>(Array.Empty<JobRun>());
        }

        return ClaimRunsAsyncCore(nodeName, jobNames, queueNames, maxCount, cancellationToken);
    }

    public async Task CreateBatchAsync(
        JobBatch batch,
        IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);

        await using (var cmd = CreateCommand(conn, """
                                                    INSERT INTO surefire_batches (id, status, total, succeeded, failed, canceled, created_at, completed_at)
                                                    VALUES (@id, @st, @tot, @suc, @fai, @can, @ca, @coa)
                                                   """, tx))
        {
            cmd.Parameters.AddWithValue("@id", batch.Id);
            cmd.Parameters.AddWithValue("@st", (int)batch.Status);
            cmd.Parameters.AddWithValue("@tot", batch.Total);
            cmd.Parameters.AddWithValue("@suc", batch.Succeeded);
            cmd.Parameters.AddWithValue("@fai", batch.Failed);
            cmd.Parameters.AddWithValue("@can", batch.Canceled);
            cmd.Parameters.AddWithValue("@ca", FormatTimestamp(batch.CreatedAt));
            cmd.Parameters.AddWithValue("@coa", FormatNullableTimestamp(batch.CompletedAt));
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        var increments = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var run in runs)
        {
            await InsertRunAsync(conn, run, cancellationToken, tx);
            if (!run.Status.IsTerminal)
            {
                increments[run.JobName] = increments.GetValueOrDefault(run.JobName) + 1;
            }
        }

        await IncrementNonTerminalCountsAsync(conn, tx, increments, cancellationToken);
        await InsertEventsAsync(conn, initialEvents, cancellationToken, tx);

        await tx.CommitAsync(cancellationToken);
    }

    public async Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  SELECT id, status, total, succeeded, failed, canceled, created_at, completed_at
                                                  FROM surefire_batches WHERE id = @id
                                                  """);
        cmd.Parameters.AddWithValue("@id", batchId);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new()
        {
            Id = reader.GetString(0),
            Status = (JobStatus)reader.GetInt32(1),
            Total = reader.GetInt32(2),
            Succeeded = reader.GetInt32(3),
            Failed = reader.GetInt32(4),
            Canceled = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
            CreatedAt = ParseTimestamp(reader.GetString(6)),
            CompletedAt = reader.IsDBNull(7) ? null : ParseTimestamp(reader.GetString(7))
        };
    }

    public async Task<bool> TryCompleteBatchAsync(
        string batchId, JobStatus status, DateTimeOffset completedAt, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  UPDATE surefire_batches
                                                  SET status = @st, completed_at = @coa
                                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                                  RETURNING id
                                                  """);
        cmd.Parameters.AddWithValue("@id", batchId);
        cmd.Parameters.AddWithValue("@st", (int)status);
        cmd.Parameters.AddWithValue("@coa", FormatTimestamp(completedAt));
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> CancelBatchRunsAsync(
        string batchId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);
        var now = FormatTimestamp(timeProvider.GetUtcNow());

        // Snapshot prior status before the cancel UPDATE; SQLite RETURNING is post-update only,
        // and we need both the Running and non-terminal populations to decrement counters.
        var priorRunning = new Dictionary<string, int>(StringComparer.Ordinal);
        var priorNonTerminal = new Dictionary<string, int>(StringComparer.Ordinal);
        await using (var snapshotCmd = CreateCommand(conn, """
                                                           SELECT job_name, status FROM surefire_runs
                                                           WHERE batch_id = @batch_id AND status IN (0, 1)
                                                           """, tx))
        {
            snapshotCmd.Parameters.AddWithValue("@batch_id", batchId);
            await using var snapReader = await snapshotCmd.ExecuteReaderAsync(cancellationToken);
            while (await snapReader.ReadAsync(cancellationToken))
            {
                var jn = snapReader.GetString(0);
                var st = snapReader.GetInt32(1);
                priorNonTerminal[jn] = priorNonTerminal.GetValueOrDefault(jn) + 1;
                if (st == 1)
                {
                    priorRunning[jn] = priorRunning.GetValueOrDefault(jn) + 1;
                }
            }
        }

        var CanceledIds = new List<string>();
        var statusEvents = new List<RunEvent>();
        await using (var cmd = CreateCommand(conn, """
                                                   UPDATE surefire_runs
                                                   SET status = 4,
                                                       canceled_at = @now,
                                                       completed_at = @now,
                                                       reason = COALESCE(@reason, reason)
                                                   WHERE batch_id = @batch_id AND status IN (0, 1)
                                                   RETURNING id, attempt
                                                   """, tx))
        {
            cmd.Parameters.AddWithValue("@batch_id", batchId);
            cmd.Parameters.AddWithValue("@now", now);
            cmd.Parameters.AddWithValue("@reason", (object?)reason ?? DBNull.Value);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
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

        await InsertEventsAsync(conn, statusEvents, cancellationToken, tx);
        await DecrementRunningCountsAsync(conn, tx, priorRunning, cancellationToken);
        await DecrementNonTerminalCountsAsync(conn, tx, priorNonTerminal, cancellationToken);

        if (CanceledIds.Count > 0)
        {
            await using var incrCmd = CreateCommand(conn, """
                                                          UPDATE surefire_batches
                                                          SET canceled = canceled + @cnt
                                                          WHERE id = @id AND status NOT IN (2, 4, 5)
                                                          RETURNING total, succeeded, failed, canceled
                                                          """, tx);
            incrCmd.Parameters.AddWithValue("@id", batchId);
            incrCmd.Parameters.AddWithValue("@cnt", CanceledIds.Count);
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

                    await using var completeCmd = CreateCommand(conn, """
                                                                      UPDATE surefire_batches
                                                                      SET status = @st, completed_at = @coa
                                                                      WHERE id = @id AND status NOT IN (2, 4, 5)
                                                                      """, tx);
                    completeCmd.Parameters.AddWithValue("@id", batchId);
                    completeCmd.Parameters.AddWithValue("@st", (int)batchStatus);
                    completeCmd.Parameters.AddWithValue("@coa", FormatTimestamp(completedAt));
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return CanceledIds;
    }

    public async Task<IReadOnlyList<string>> CancelChildRunsAsync(
        string parentRunId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);
        var now = FormatTimestamp(timeProvider.GetUtcNow());

        // Snapshot prior status before the cancel UPDATE; mirrors CancelBatchRunsAsync.
        var priorRunning = new Dictionary<string, int>(StringComparer.Ordinal);
        var priorNonTerminal = new Dictionary<string, int>(StringComparer.Ordinal);
        await using (var snapshotCmd = CreateCommand(conn, """
                                                           SELECT job_name, status FROM surefire_runs
                                                           WHERE parent_run_id = @parent_run_id AND status IN (0, 1)
                                                           """, tx))
        {
            snapshotCmd.Parameters.AddWithValue("@parent_run_id", parentRunId);
            await using var snapReader = await snapshotCmd.ExecuteReaderAsync(cancellationToken);
            while (await snapReader.ReadAsync(cancellationToken))
            {
                var jn = snapReader.GetString(0);
                var st = snapReader.GetInt32(1);
                priorNonTerminal[jn] = priorNonTerminal.GetValueOrDefault(jn) + 1;
                if (st == 1)
                {
                    priorRunning[jn] = priorRunning.GetValueOrDefault(jn) + 1;
                }
            }
        }

        var CanceledIds = new List<string>();
        var CanceledBatchIds = new Dictionary<string, int>();
        var statusEvents = new List<RunEvent>();
        await using (var cmd = CreateCommand(conn, """
                                                   UPDATE surefire_runs
                                                   SET status = 4,
                                                       canceled_at = @now,
                                                       completed_at = @now,
                                                       reason = COALESCE(@reason, reason)
                                                   WHERE parent_run_id = @parent_run_id AND status IN (0, 1)
                                                   RETURNING id, attempt, batch_id
                                                   """, tx))
        {
            cmd.Parameters.AddWithValue("@parent_run_id", parentRunId);
            cmd.Parameters.AddWithValue("@now", now);
            cmd.Parameters.AddWithValue("@reason", (object?)reason ?? DBNull.Value);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
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

        await InsertEventsAsync(conn, statusEvents, cancellationToken, tx);
        await DecrementRunningCountsAsync(conn, tx, priorRunning, cancellationToken);
        await DecrementNonTerminalCountsAsync(conn, tx, priorNonTerminal, cancellationToken);

        foreach (var (batchId, cnt) in CanceledBatchIds)
        {
            await using var incrCmd = CreateCommand(conn, """
                                                          UPDATE surefire_batches
                                                          SET canceled = canceled + @cnt
                                                          WHERE id = @id AND status NOT IN (2, 4, 5)
                                                          RETURNING total, succeeded, failed, canceled
                                                          """, tx);
            incrCmd.Parameters.AddWithValue("@id", batchId);
            incrCmd.Parameters.AddWithValue("@cnt", cnt);
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

                    await using var completeCmd = CreateCommand(conn, """
                                                                      UPDATE surefire_batches
                                                                      SET status = @st, completed_at = @coa
                                                                      WHERE id = @id AND status NOT IN (2, 4, 5)
                                                                      """, tx);
                    completeCmd.Parameters.AddWithValue("@id", batchId);
                    completeCmd.Parameters.AddWithValue("@st", (int)batchStatus);
                    completeCmd.Parameters.AddWithValue("@coa", FormatTimestamp(completedAt));
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return CanceledIds;
    }

    public async Task<IReadOnlyList<string>> GetCompletableBatchIdsAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        var result = new List<string>();
        await using var cmd = CreateCommand(conn, """
                                                  SELECT b.id FROM surefire_batches b
                                                  WHERE b.status NOT IN (2, 4, 5)
                                                  AND NOT EXISTS (
                                                      SELECT 1 FROM surefire_runs r
                                                      WHERE r.batch_id = b.id AND r.status NOT IN (2, 4, 5)
                                                  )
                                                  """);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(reader.GetString(0));
        }

        return result;
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
        int? take = null,
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

        sql += " ORDER BY id";
        if (take is { } t)
        {
            sql += " LIMIT @take";
            cmd.Parameters.AddWithValue("@take", t);
        }

        cmd.CommandText = sql;
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

    public async Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchId, long sinceEventId = 0,
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
                                                  WHERE r.batch_id = @batch_id
                                                      AND e.event_type = @event_type
                                                      AND e.id > @since_id
                                                  ORDER BY e.id
                                                  LIMIT @take
                                                  """);
        cmd.Parameters.AddWithValue("@batch_id", batchId);
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

    public async Task<IReadOnlyList<RunEvent>> GetBatchEventsAsync(string batchId, long sinceEventId = 0,
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
                                                  WHERE r.batch_id = @batch_id
                                                      AND e.id > @since_id
                                                  ORDER BY e.id
                                                  LIMIT @take
                                                  """);
        cmd.Parameters.AddWithValue("@batch_id", batchId);
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
            nodeCmd.Parameters.AddWithValue("@rjn",
                JsonSerializer.Serialize(jobNames.ToArray(), SurefireJsonContext.Default.StringArray));
            nodeCmd.Parameters.AddWithValue("@rqn",
                JsonSerializer.Serialize(queueNames.ToArray(), SurefireJsonContext.Default.StringArray));
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

    public async Task<IReadOnlyList<string>> GetExternallyStoppedRunIdsAsync(
        IReadOnlyCollection<string> runIds, CancellationToken cancellationToken = default)
    {
        if (runIds.Count == 0)
        {
            return [];
        }

        await using var conn = await CreateConnectionAsync(cancellationToken);

        // Returns IDs that no longer have a Running row (deleted or transitioned).
        const int chunkSize = 900;
        var idList = runIds as IList<string> ?? runIds.ToList();
        var stopped = new List<string>();

        for (var offset = 0; offset < idList.Count; offset += chunkSize)
        {
            var count = Math.Min(chunkSize, idList.Count - offset);
            var sb = new StringBuilder("WITH input(id) AS (VALUES ");
            await using var cmd = new SqliteCommand { Connection = conn };

            for (var i = 0; i < count; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }

                var p = $"@rid_{i}";
                sb.Append('(').Append(p).Append(')');
                cmd.Parameters.AddWithValue(p, idList[offset + i]);
            }

            sb.Append(
                ") SELECT i.id FROM input i LEFT JOIN surefire_runs r ON r.id = i.id WHERE r.id IS NULL OR r.status <> 1");
            cmd.CommandText = sb.ToString();

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                stopped.Add(reader.GetString(0));
            }
        }

        return stopped;
    }

    public async Task<IReadOnlyList<string>> GetStaleRunningRunIdsAsync(DateTimeOffset staleBefore, int take,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(take, 1);

        await using var conn = await CreateConnectionAsync(cancellationToken);
        // Backed by ix_runs_stale_heartbeat. Oldest-first so the caller's loop makes monotonic
        // progress against a shrinking filter set.
        await using var cmd = CreateCommand(conn, """
                                                  SELECT id FROM surefire_runs
                                                  WHERE status = 1 AND last_heartbeat_at < @stale_before
                                                  ORDER BY last_heartbeat_at ASC, id ASC
                                                  LIMIT @take
                                                  """);
        cmd.Parameters.AddWithValue("@take", take);
        cmd.Parameters.AddWithValue("@stale_before", FormatTimestamp(staleBefore));

        var ids = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ids.Add(reader.GetString(0));
        }

        return ids;
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

    public async Task UpsertQueuesAsync(IReadOnlyList<QueueDefinition> queues,
        CancellationToken cancellationToken = default)
    {
        if (queues.Count == 0)
        {
            return;
        }

        // is_paused absent from the DO UPDATE SET list so dashboard pause survives re-upserts.
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  INSERT INTO surefire_queues (
                                                      name, priority, max_concurrency, is_paused,
                                                      rate_limit_name, last_heartbeat_at
                                                  )
                                                  SELECT
                                                      json_extract(e.value, '$.name'),
                                                      json_extract(e.value, '$.priority'),
                                                      json_extract(e.value, '$.maxConcurrency'),
                                                      json_extract(e.value, '$.isPaused'),
                                                      json_extract(e.value, '$.rateLimitName'),
                                                      @now
                                                  FROM json_each(@payload) AS e
                                                  WHERE json_type(e.value) = 'object'
                                                  ON CONFLICT (name) DO UPDATE SET
                                                      priority = excluded.priority,
                                                      max_concurrency = excluded.max_concurrency,
                                                      rate_limit_name = excluded.rate_limit_name,
                                                      last_heartbeat_at = @now
                                                  """);
        cmd.Parameters.AddWithValue("@payload", UpsertPayloadFactory.SerializeQueues(queues));
        cmd.Parameters.AddWithValue("@now", FormatTimestamp(timeProvider.GetUtcNow()));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
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

    public async Task<bool> SetQueuePausedAsync(
        string name, bool isPaused, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn,
            "UPDATE surefire_queues SET is_paused = @ip WHERE name = @n");
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@ip", isPaused ? 1 : 0);
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
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var cmd = CreateCommand(conn, """
                                                  INSERT INTO surefire_rate_limits (
                                                      name, type, max_permits, "window", last_heartbeat_at
                                                  )
                                                  SELECT
                                                      json_extract(e.value, '$.name'),
                                                      json_extract(e.value, '$.type'),
                                                      json_extract(e.value, '$.maxPermits'),
                                                      json_extract(e.value, '$.window'),
                                                      @now
                                                  FROM json_each(@payload) AS e
                                                  WHERE json_type(e.value) = 'object'
                                                  ON CONFLICT (name) DO UPDATE SET
                                                      type = excluded.type,
                                                      max_permits = excluded.max_permits,
                                                      "window" = excluded."window",
                                                      last_heartbeat_at = @now
                                                  """);
        cmd.Parameters.AddWithValue("@payload", UpsertPayloadFactory.SerializeRateLimits(rateLimits));
        cmd.Parameters.AddWithValue("@now", FormatTimestamp(timeProvider.GetUtcNow()));
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> CancelExpiredRunsWithIdsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        var now = FormatTimestamp(timeProvider.GetUtcNow());
        await using var tx = conn.BeginTransaction();

        var CanceledIds = new List<string>();
        var statusEvents = new List<RunEvent>();

        var CanceledBatchIds = new Dictionary<string, int>();
        var expiredByJob = new Dictionary<string, int>(StringComparer.Ordinal);
        await using (var updateCmd = CreateCommand(conn, """
                                                         UPDATE surefire_runs
                                                         SET status = 4,
                                                             canceled_at = @now,
                                                             completed_at = @now,
                                                             reason = @reason
                                                         WHERE status = 0
                                                             AND not_after IS NOT NULL
                                                             AND not_after < @now
                                                         RETURNING id, attempt, batch_id, job_name
                                                         """, tx))
        {
            updateCmd.Parameters.AddWithValue("@now", now);
            updateCmd.Parameters.AddWithValue("@reason", "Run expired past NotAfter deadline.");
            await using var reader = await updateCmd.ExecuteReaderAsync(cancellationToken);
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

                var jn = reader.GetString(3);
                expiredByJob[jn] = expiredByJob.GetValueOrDefault(jn) + 1;
            }
        }

        await InsertEventsAsync(conn, statusEvents, cancellationToken, tx);
        // Only pending rows touched; running_count unaffected, non_terminal_count decrements
        // once per Canceled row.
        await DecrementNonTerminalCountsAsync(conn, tx, expiredByJob, cancellationToken);

        foreach (var (batchId, cnt) in CanceledBatchIds)
        {
            await using var incrCmd = CreateCommand(conn, """
                                                          UPDATE surefire_batches
                                                          SET canceled = canceled + @cnt
                                                          WHERE id = @id AND status NOT IN (2, 4, 5)
                                                          RETURNING total, succeeded, failed, canceled
                                                          """, tx);
            incrCmd.Parameters.AddWithValue("@id", batchId);
            incrCmd.Parameters.AddWithValue("@cnt", cnt);
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

                    await using var completeCmd = CreateCommand(conn, """
                                                                      UPDATE surefire_batches
                                                                      SET status = @st, completed_at = @coa
                                                                      WHERE id = @id AND status NOT IN (2, 4, 5)
                                                                      """, tx);
                    completeCmd.Parameters.AddWithValue("@id", batchId);
                    completeCmd.Parameters.AddWithValue("@st", (int)batchStatus);
                    completeCmd.Parameters.AddWithValue("@coa", FormatTimestamp(completedAt));
                    await completeCmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        await tx.CommitAsync(cancellationToken);
        return CanceledIds;
    }

    public async Task PurgeAsync(
        DateTimeOffset threshold, CancellationToken cancellationToken = default)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        var thresholdStr = FormatTimestamp(threshold);

        while (true)
        {
            await using var tx = conn.BeginTransaction(false);
            // DELETE ... RETURNING captures status+job_name so non-terminal removals decrement
            // non_terminal_count.
            await using var cmd = CreateCommand(conn, """
                                                      DELETE FROM surefire_runs WHERE rowid IN (
                                                          SELECT rowid FROM surefire_runs
                                                          WHERE (status IN (2, 4, 5)
                                                              AND completed_at < @b
                                                              AND (batch_id IS NULL OR EXISTS (
                                                                  SELECT 1 FROM surefire_batches b
                                                                  WHERE b.id = surefire_runs.batch_id
                                                                      AND b.status IN (2, 4, 5)
                                                                      AND b.completed_at IS NOT NULL
                                                                      AND b.completed_at < @b
                                                              )))
                                                              OR (status = 0 AND not_before < @b)
                                                          LIMIT 1000
                                                      )
                                                      RETURNING status, job_name
                                                      """, tx);
            cmd.Parameters.AddWithValue("@b", thresholdStr);

            var nonTerminalByJob = new Dictionary<string, int>(StringComparer.Ordinal);
            var deleted = 0;
            await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    deleted++;
                    var status = reader.GetInt32(0);
                    if (status is 2 or 4 or 5)
                    {
                        continue;
                    }

                    var jn = reader.GetString(1);
                    nonTerminalByJob[jn] = nonTerminalByJob.GetValueOrDefault(jn) + 1;
                }
            }

            await DecrementNonTerminalCountsAsync(conn, tx, nonTerminalByJob, cancellationToken);
            await tx.CommitAsync(cancellationToken);
            if (deleted < 1000)
            {
                break;
            }
        }

        await using var metaTx = conn.BeginTransaction(false);

        await ExecuteWithThresholdAsync(conn, """
                                              DELETE FROM surefire_batches
                                              WHERE status IN (2, 4, 5) AND completed_at IS NOT NULL AND completed_at < @b
                                              """, thresholdStr, metaTx, cancellationToken);

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
                                                           COALESCE(SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END), 0),
                                                           COALESCE(SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END), 0)
                                                       FROM surefire_runs
                                                       WHERE created_at >= @since AND created_at <= @now
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
            var Canceled = Convert.ToInt32(reader.GetValue(6));
            var deadLetter = Convert.ToInt32(reader.GetValue(7));

            activeRuns = pending + running;
            var terminalCount = completed + Canceled + deadLetter;
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
        }

        var bucketSeconds = bucketMinutes * 60;
        var bucketSize = TimeSpan.FromMinutes(bucketMinutes);
        var bucketData =
            new Dictionary<int, (int Pending, int Running, int Succeeded, int Canceled, int Failed)>();

        await using (var timelineCmd = CreateCommand(conn, """
                                                           SELECT
                                                               CAST((julianday(created_at) - julianday(@since)) * 86400.0 / @bucket_seconds AS INTEGER) AS bucket_idx,
                                                               status,
                                                               COUNT(*) AS cnt
                                                           FROM surefire_runs
                                                           WHERE created_at >= @since AND created_at <= @now
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
                    entry = (0, 0, 0, 0, 0);
                }

                bucketData[idx] = status switch
                {
                    JobStatus.Pending => (entry.Pending + cnt, entry.Running, entry.Succeeded,
                        entry.Canceled, entry.Failed),
                    JobStatus.Running => (entry.Pending, entry.Running + cnt, entry.Succeeded,
                        entry.Canceled, entry.Failed),
                    JobStatus.Succeeded => (entry.Pending, entry.Running, entry.Succeeded + cnt,
                        entry.Canceled, entry.Failed),
                    JobStatus.Canceled => (entry.Pending, entry.Running, entry.Succeeded,
                        entry.Canceled + cnt, entry.Failed),
                    JobStatus.Failed => (entry.Pending, entry.Running, entry.Succeeded,
                        entry.Canceled, entry.Failed + cnt),
                    _ => entry
                };
            }
        }

        await tx.CommitAsync(cancellationToken);

        var buckets = new List<TimelineBucket>();
        var bucketStart = sinceTime;
        var bucketIdx = 0;
        while (bucketStart <= now)
        {
            bucketData.TryGetValue(bucketIdx, out var entry);
            buckets.Add(new()
            {
                Start = bucketStart,
                Pending = entry.Pending,
                Running = entry.Running,
                Succeeded = entry.Succeeded,
                Canceled = entry.Canceled,
                Failed = entry.Failed
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
        var CanceledCount = Convert.ToInt32(reader.GetValue(5));
        var terminalCount = succeeded + failed + CanceledCount;

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

    /// <summary>
    ///     SQLite transient errors. <c>SqliteErrorCode</c> is the primary result code
    ///     (Microsoft.Data.Sqlite masks off extended codes via <c>&amp; 0xFF</c>), so matching
    ///     <c>BUSY</c> (5) and <c>LOCKED</c> (6) also covers the extended variants
    ///     <c>BUSY_RECOVERY</c>, <c>BUSY_SNAPSHOT</c>, <c>BUSY_TIMEOUT</c>,
    ///     <c>LOCKED_SHAREDCACHE</c>, etc. Do not "fix" this by adding extended codes;
    ///     they are already captured by the primary code match.
    /// </summary>
    public bool IsTransientException(Exception ex) =>
        ex is SqliteException sqlite && sqlite.SqliteErrorCode is 5 or 6;

    private async Task<IReadOnlyList<JobRun>> ClaimRunsAsyncCore(
        string nodeName,
        IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames,
        int maxCount,
        CancellationToken cancellationToken)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);
        var now = FormatTimestamp(timeProvider.GetUtcNow());
        var jobNameParams = BuildInClause("@jn", jobNames, out var jobNameClause);
        var queueNameParams = BuildInClause("@qn", queueNames, out var queueNameClause);

        // ROW_NUMBER PARTITION BY caps per-bucket strictly. Capacity reads come from the
        // materialized running_count columns (no scan). q_rl_rn is skipped when the queue
        // shares the job's rate limiter. SQLite serializes writers, so no SKIP LOCKED needed.
        var claimSql = $"""
                        WITH rl_remaining AS (
                            SELECT rl.name,
                                MAX(0, rl.max_permits - (
                                    CASE
                                        WHEN rl.window_start IS NULL THEN 0
                                        WHEN (julianday(@now) - julianday(rl.window_start))
                                            * 864000000000.0 >= CAST(rl."window" AS REAL) * 2 THEN 0
                                        WHEN rl.type = 1 AND julianday(rl.window_start)
                                            + CAST(rl."window" AS REAL) / 864000000000.0 <= julianday(@now) THEN
                                            rl.current_count * MAX(0, 1.0 - ((julianday(@now) - julianday(rl.window_start))
                                                * 864000000000.0 - CAST(rl."window" AS REAL)) / CAST(rl."window" AS REAL))
                                        WHEN julianday(rl.window_start)
                                            + CAST(rl."window" AS REAL) / 864000000000.0 <= julianday(@now) THEN 0
                                        WHEN rl.type = 1 THEN
                                            COALESCE(rl.previous_count, 0) * MAX(0, 1.0 - (julianday(@now)
                                                - julianday(rl.window_start)) * 864000000000.0
                                                / CAST(rl."window" AS REAL)) + rl.current_count
                                        ELSE rl.current_count
                                    END
                                )) AS remaining
                            FROM surefire_rate_limits rl
                        ),
                        rl_state AS (
                            -- SQLite lacks scalar CEIL: trunc + (1 if fractional else 0).
                            -- Matches the "used < max_permits" rule, which allows one claim
                            -- even with fractional remaining (e.g. sliding-window decay).
                            SELECT name,
                                CAST(remaining AS INTEGER) + IIF(remaining > CAST(remaining AS INTEGER), 1, 0) AS available
                            FROM rl_remaining
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
                                AND r.not_before <= @now
                                AND (r.not_after IS NULL OR r.not_after > @now)
                                AND r.job_name IN ({jobNameClause})
                                AND COALESCE(j.queue, 'default') IN ({queueNameClause})
                                AND COALESCE(q.is_paused, 0) = 0
                        )
                        SELECT r.id, r.run_job_name, r.run_queue_name, r.j_rl, r.q_rl
                        FROM ranked r
                        LEFT JOIN rl_state jrl ON jrl.name = r.j_rl
                        LEFT JOIN rl_state qrl ON qrl.name = r.q_rl
                        WHERE (r.j_max IS NULL OR r.j_rn <= r.j_max - r.j_running)
                            AND (r.q_max IS NULL OR r.q_rn <= r.q_max - r.q_running)
                            AND (r.j_rl IS NULL OR jrl.available IS NULL OR r.j_rl_rn <= jrl.available)
                            AND (r.q_rl IS NULL OR r.q_rl = r.j_rl OR qrl.available IS NULL OR r.q_rl_rn <= qrl.available)
                        ORDER BY r.queue_priority DESC, r.priority DESC, r.not_before ASC, r.id ASC
                        LIMIT @max_count
                        """;

        // Phase 1: SELECT eligible candidates. SQLite forbids FROM columns in RETURNING, so we
        // capture metadata here and UPDATE-by-id in phase 2. BEGIN IMMEDIATE serializes writers,
        // so no other transaction can interleave between SELECT and UPDATE.
        var claimed = new List<(string Id, string JobName, string QueueName, string? JobRl, string? QueueRl)>();
        await using (var selectCmd = CreateCommand(conn, claimSql, tx))
        {
            selectCmd.Parameters.AddWithValue("@now", now);
            selectCmd.Parameters.AddWithValue("@max_count", maxCount);
            foreach (var p in jobNameParams)
            {
                selectCmd.Parameters.Add(p);
            }

            foreach (var p in queueNameParams)
            {
                selectCmd.Parameters.Add(p);
            }

            await using var reader = await selectCmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetString(0);
                var jobName = reader.GetString(1);
                var queueName = reader.GetString(2);
                var jobRl = reader.IsDBNull(3) ? null : reader.GetString(3);
                var queueRl = reader.IsDBNull(4) ? null : reader.GetString(4);
                claimed.Add((id, jobName, queueName, jobRl, queueRl));
            }
        }

        if (claimed.Count == 0)
        {
            await tx.CommitAsync(cancellationToken);
            return Array.Empty<JobRun>();
        }

        // Phase 2: claim by id with UPDATE...WHERE id IN (...) RETURNING * to bypass SQLite's
        // RETURNING-FROM restriction.
        var idParams = new List<string>(claimed.Count);
        var idCmd = CreateCommand(conn, "", tx);
        for (var i = 0; i < claimed.Count; i++)
        {
            var name = $"@id_{i}";
            idParams.Add(name);
            idCmd.Parameters.AddWithValue(name, claimed[i].Id);
        }

        idCmd.Parameters.AddWithValue("@now", now);
        idCmd.Parameters.AddWithValue("@nn", nodeName);
        idCmd.CommandText = $"""
                             UPDATE surefire_runs
                             SET status = 1, node_name = @nn, started_at = @now,
                                 last_heartbeat_at = @now, attempt = attempt + 1
                             WHERE id IN ({string.Join(",", idParams)})
                             RETURNING *
                             """;

        var claimedRuns = new List<JobRun>(claimed.Count);
        await using (var idReader = await idCmd.ExecuteReaderAsync(cancellationToken))
        {
            while (await idReader.ReadAsync(cancellationToken))
            {
                claimedRuns.Add(ReadRun(idReader));
            }
        }

        await idCmd.DisposeAsync();

        var jobIncrements = new Dictionary<string, int>(StringComparer.Ordinal);
        var queueIncrements = new Dictionary<string, int>(StringComparer.Ordinal);
        Dictionary<string, int>? rateLimitIncrements = null;
        var nowOffset = timeProvider.GetUtcNow();
        var statusEvents = new List<RunEvent>(claimedRuns.Count);

        // Index phase-1 metadata by id since UPDATE...RETURNING preserves no order.
        var metadataById = new Dictionary<string, (string QueueName, string? JobRl, string? QueueRl)>(
            claimed.Count, StringComparer.Ordinal);
        foreach (var (id, _, queueName, jobRl, queueRl) in claimed)
        {
            metadataById[id] = (queueName, jobRl, queueRl);
        }

        foreach (var run in claimedRuns)
        {
            statusEvents.Add(RunStatusEvents.Create(run.Id, run.Attempt, run.Status, nowOffset));
            jobIncrements[run.JobName] = jobIncrements.GetValueOrDefault(run.JobName) + 1;
            var meta = metadataById[run.Id];
            queueIncrements[meta.QueueName] = queueIncrements.GetValueOrDefault(meta.QueueName) + 1;

            if (meta.JobRl is { })
            {
                rateLimitIncrements ??= new(StringComparer.Ordinal);
                rateLimitIncrements[meta.JobRl] = rateLimitIncrements.GetValueOrDefault(meta.JobRl) + 1;
            }

            if (meta.QueueRl is { } && meta.QueueRl != meta.JobRl)
            {
                rateLimitIncrements ??= new(StringComparer.Ordinal);
                rateLimitIncrements[meta.QueueRl] = rateLimitIncrements.GetValueOrDefault(meta.QueueRl) + 1;
            }
        }

        await InsertEventsAsync(conn, statusEvents, cancellationToken, tx);

        // SQLite lacks UPDATE FROM (VALUES...), so issue one UPDATE per distinct job/queue.
        // Bounded by distinct names in the batch, not by run count.
        await using (var jobIncCmd = CreateCommand(conn,
                         "UPDATE surefire_jobs SET running_count = running_count + @cnt WHERE name = @name", tx))
        {
            var jobNameParam = jobIncCmd.Parameters.AddWithValue("@name", DBNull.Value);
            var jobCntParam = jobIncCmd.Parameters.AddWithValue("@cnt", 0);
            foreach (var (jn, cnt) in jobIncrements)
            {
                jobNameParam.Value = jn;
                jobCntParam.Value = cnt;
                await jobIncCmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        await using (var queueIncCmd = CreateCommand(conn,
                         "UPDATE surefire_queues SET running_count = running_count + @cnt WHERE name = @name", tx))
        {
            var queueNameParam = queueIncCmd.Parameters.AddWithValue("@name", DBNull.Value);
            var queueCntParam = queueIncCmd.Parameters.AddWithValue("@cnt", 0);
            foreach (var (qn, cnt) in queueIncrements)
            {
                queueNameParam.Value = qn;
                queueCntParam.Value = cnt;
                await queueIncCmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        if (rateLimitIncrements is { })
        {
            foreach (var (rateLimitName, count) in rateLimitIncrements)
            {
                await AcquireRateLimitByNameAsync(conn, rateLimitName, count, now, tx, cancellationToken);
            }
        }

        await tx.CommitAsync(cancellationToken);
        return claimedRuns;
    }

    private async Task CreateRunsAsyncCore(
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
        var increments = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var run in runs)
        {
            await InsertRunAsync(conn, run, cancellationToken, tx);
            if (!run.Status.IsTerminal)
            {
                increments[run.JobName] = increments.GetValueOrDefault(run.JobName) + 1;
            }
        }

        await IncrementNonTerminalCountsAsync(conn, tx, increments, cancellationToken);
        await InsertEventsAsync(conn, initialEvents, cancellationToken, tx);

        await tx.CommitAsync(cancellationToken);
    }

    private async Task<bool> TryCreateRunAsyncCore(
        JobRun run, int? maxActiveForJob, DateTimeOffset? lastCronFireAt,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        await using var conn = await CreateConnectionAsync(cancellationToken);
        await using var tx = conn.BeginTransaction(false);

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
            // Capacity check reads the maintained counter (no scan). Missing job row defaults
            // to "enabled, zero active" per the TryCreateRunAsync contract.
            await using var checkCmd = CreateCommand(conn, """
                                                           SELECT is_enabled, non_terminal_count
                                                           FROM surefire_jobs WHERE name = @n
                                                           """, tx);
            checkCmd.Parameters.AddWithValue("@n", run.JobName);
            await using (var reader = await checkCmd.ExecuteReaderAsync(cancellationToken))
            {
                if (await reader.ReadAsync(cancellationToken))
                {
                    var isEnabled = reader.GetInt64(0) != 0;
                    var nonTerminal = reader.GetInt32(1);
                    if (!isEnabled || nonTerminal >= maxActiveForJob.Value)
                    {
                        await tx.CommitAsync(cancellationToken);
                        return false;
                    }
                }
            }
        }

        try
        {
            await InsertRunAsync(conn, run, cancellationToken, tx);

            // Maintain non_terminal_count atomically with the insert.
            if (!run.Status.IsTerminal)
            {
                await IncrementNonTerminalCountAsync(conn, tx, run.JobName, cancellationToken);
            }

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
        pragmaCmd.CommandText = ConnectionPragmas;
        await pragmaCmd.ExecuteNonQueryAsync(cancellationToken);
        return conn;
    }

    private static SqliteCommand CreateCommand(SqliteConnection conn, string sql, SqliteTransaction? tx = null) =>
        new(sql, conn) { Transaction = tx };

    private static string BuildConnectionString(string connectionString, TimeSpan? commandTimeout)
    {
        var builder = new SqliteConnectionStringBuilder(connectionString);
        // Default to a generous timeout so concurrent writers don't surface SQLITE_BUSY.
        builder.DefaultTimeout =
            CommandTimeouts.ToSeconds(commandTimeout, nameof(commandTimeout))
            ?? 30;
        return builder.ToString();
    }

    private static async Task<bool> HasMigrationVersionAsync(SqliteConnection conn, int version,
        CancellationToken cancellationToken, SqliteTransaction? tx = null)
    {
        await using var cmd = CreateCommand(conn,
            "SELECT COUNT(*) FROM surefire_schema_migrations WHERE version = @version", tx);
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

    private static async Task InsertRunAsync(
        SqliteConnection conn,
        JobRun run,
        CancellationToken cancellationToken,
        SqliteTransaction? tx = null)
    {
        await using var cmd = CreateCommand(conn, """
                                                  INSERT INTO surefire_runs (
                                                      id, job_name, status, arguments, result, reason, progress,
                                                      created_at, started_at, completed_at, canceled_at, node_name,
                                                      attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                                                      rerun_of_run_id, not_before, not_after, priority,
                                                      deduplication_id, last_heartbeat_at, batch_id
                                                  ) VALUES (
                                                      @id, @jn, @st, @ar, @re, @er, @pr,
                                                      @ca, @sa, @coa, @caa, @nn,
                                                      @at, @ti, @si, @pti, @psi, @pri, @rri,
                                                      @ror, @nb, @na, @py,
                                                      @di, @lh, @bid
                                                  )
                                                  """, tx);
        cmd.Parameters.AddWithValue("@id", run.Id);
        cmd.Parameters.AddWithValue("@jn", run.JobName);
        cmd.Parameters.AddWithValue("@st", (int)run.Status);
        cmd.Parameters.AddWithValue("@ar", (object?)run.Arguments ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@re", (object?)run.Result ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@er", (object?)run.Reason ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@pr", run.Progress);
        cmd.Parameters.AddWithValue("@ca", FormatTimestamp(run.CreatedAt));
        cmd.Parameters.AddWithValue("@sa", FormatNullableTimestamp(run.StartedAt));
        cmd.Parameters.AddWithValue("@coa", FormatNullableTimestamp(run.CompletedAt));
        cmd.Parameters.AddWithValue("@caa", FormatNullableTimestamp(run.CanceledAt));
        cmd.Parameters.AddWithValue("@nn", (object?)run.NodeName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@at", run.Attempt);
        cmd.Parameters.AddWithValue("@ti", (object?)run.TraceId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@si", (object?)run.SpanId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@pti", (object?)run.ParentTraceId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@psi", (object?)run.ParentSpanId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@pri", (object?)run.ParentRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@rri", (object?)run.RootRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ror", (object?)run.RerunOfRunId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@nb", FormatTimestamp(run.NotBefore));
        cmd.Parameters.AddWithValue("@na", FormatNullableTimestamp(run.NotAfter));
        cmd.Parameters.AddWithValue("@py", run.Priority);
        cmd.Parameters.AddWithValue("@di", (object?)run.DeduplicationId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@lh", FormatNullableTimestamp(run.LastHeartbeatAt));
        cmd.Parameters.AddWithValue("@bid", (object?)run.BatchId ?? DBNull.Value);
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
            Tags = JsonSerializer.Deserialize(
                reader.GetString(reader.GetOrdinal("tags")),
                SurefireJsonContext.Default.StringArray) ?? [],
            CronExpression = GetNullableString(reader, "cron_expression"),
            TimeZoneId = GetNullableString(reader, "time_zone_id"),
            Timeout = timeout,
            MaxConcurrency = reader.IsDBNull(reader.GetOrdinal("max_concurrency"))
                ? null
                : reader.GetInt32(reader.GetOrdinal("max_concurrency")),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            RetryPolicy = reader.IsDBNull(reader.GetOrdinal("retry_policy"))
                ? new()
                : JsonSerializer.Deserialize(reader.GetString(reader.GetOrdinal("retry_policy")),
                      SurefireJsonContext.Default.RetryPolicy)
                  ?? throw new InvalidOperationException("Retry policy payload was null."),
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

    private static JobRun ReadRun(SqliteDataReader reader) =>
        new()
        {
            Id = reader.GetString(reader.GetOrdinal("id")),
            JobName = reader.GetString(reader.GetOrdinal("job_name")),
            Status = (JobStatus)reader.GetInt32(reader.GetOrdinal("status")),
            Arguments = GetNullableString(reader, "arguments"),
            Result = GetNullableString(reader, "result"),
            Reason = GetNullableString(reader, "reason"),
            Progress = reader.GetDouble(reader.GetOrdinal("progress")),
            CreatedAt = ParseTimestamp(reader.GetString(reader.GetOrdinal("created_at"))),
            StartedAt = GetNullableTimestamp(reader, "started_at"),
            CompletedAt = GetNullableTimestamp(reader, "completed_at"),
            CanceledAt = GetNullableTimestamp(reader, "canceled_at"),
            NodeName = GetNullableString(reader, "node_name"),
            Attempt = reader.GetInt32(reader.GetOrdinal("attempt")),
            TraceId = GetNullableString(reader, "trace_id"),
            SpanId = GetNullableString(reader, "span_id"),
            ParentTraceId = GetNullableString(reader, "parent_trace_id"),
            ParentSpanId = GetNullableString(reader, "parent_span_id"),
            ParentRunId = GetNullableString(reader, "parent_run_id"),
            RootRunId = GetNullableString(reader, "root_run_id"),
            RerunOfRunId = GetNullableString(reader, "rerun_of_run_id"),
            NotBefore = ParseTimestamp(reader.GetString(reader.GetOrdinal("not_before"))),
            NotAfter = GetNullableTimestamp(reader, "not_after"),
            Priority = reader.GetInt32(reader.GetOrdinal("priority")),
            DeduplicationId = GetNullableString(reader, "deduplication_id"),
            LastHeartbeatAt = GetNullableTimestamp(reader, "last_heartbeat_at"),
            BatchId = GetNullableString(reader, "batch_id")
        };

    private static NodeInfo ReadNode(SqliteDataReader reader) => new()
    {
        Name = reader.GetString(reader.GetOrdinal("name")),
        StartedAt = ParseTimestamp(reader.GetString(reader.GetOrdinal("started_at"))),
        LastHeartbeatAt = ParseTimestamp(reader.GetString(reader.GetOrdinal("last_heartbeat_at"))),
        RunningCount = reader.GetInt32(reader.GetOrdinal("running_count")),
        RegisteredJobNames = JsonSerializer.Deserialize(
            reader.GetString(reader.GetOrdinal("registered_job_names")),
            SurefireJsonContext.Default.StringArray) ?? [],
        RegisteredQueueNames = JsonSerializer.Deserialize(
            reader.GetString(reader.GetOrdinal("registered_queue_names")),
            SurefireJsonContext.Default.StringArray) ?? []
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

    private async Task DecrementRunningCountAsync(SqliteConnection conn, SqliteTransaction tx,
        string jobName, CancellationToken cancellationToken)
    {
        await using var jobCmd = CreateCommand(conn, """
                                                     UPDATE surefire_jobs
                                                     SET running_count = MAX(0, running_count - 1)
                                                     WHERE name = @name
                                                     """, tx);
        jobCmd.Parameters.AddWithValue("@name", jobName);
        await jobCmd.ExecuteNonQueryAsync(cancellationToken);

        await using var queueCmd = CreateCommand(conn, """
                                                       UPDATE surefire_queues
                                                       SET running_count = MAX(0, running_count - 1)
                                                       WHERE name = (
                                                           SELECT COALESCE(j.queue, 'default')
                                                           FROM surefire_jobs j WHERE j.name = @name
                                                       )
                                                       """, tx);
        queueCmd.Parameters.AddWithValue("@name", jobName);
        await queueCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task IncrementNonTerminalCountAsync(SqliteConnection conn, SqliteTransaction tx,
        string jobName, CancellationToken cancellationToken)
    {
        await using var cmd = CreateCommand(conn,
            "UPDATE surefire_jobs SET non_terminal_count = non_terminal_count + 1 WHERE name = @name", tx);
        cmd.Parameters.AddWithValue("@name", jobName);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task IncrementNonTerminalCountsAsync(SqliteConnection conn, SqliteTransaction tx,
        IReadOnlyDictionary<string, int> jobNameToCount, CancellationToken cancellationToken)
    {
        if (jobNameToCount.Count == 0)
        {
            return;
        }

        await using var cmd = CreateCommand(conn,
            "UPDATE surefire_jobs SET non_terminal_count = non_terminal_count + @cnt WHERE name = @name", tx);
        var nameParam = cmd.Parameters.AddWithValue("@name", DBNull.Value);
        var cntParam = cmd.Parameters.AddWithValue("@cnt", 0);

        foreach (var (jn, cnt) in jobNameToCount)
        {
            nameParam.Value = jn;
            cntParam.Value = cnt;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task DecrementNonTerminalCountAsync(SqliteConnection conn, SqliteTransaction tx,
        string jobName, CancellationToken cancellationToken)
    {
        await using var cmd = CreateCommand(conn,
            "UPDATE surefire_jobs SET non_terminal_count = MAX(0, non_terminal_count - 1) WHERE name = @name", tx);
        cmd.Parameters.AddWithValue("@name", jobName);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task DecrementNonTerminalCountsAsync(SqliteConnection conn, SqliteTransaction tx,
        IReadOnlyDictionary<string, int> jobNameToCount, CancellationToken cancellationToken)
    {
        if (jobNameToCount.Count == 0)
        {
            return;
        }

        await using var cmd = CreateCommand(conn,
            "UPDATE surefire_jobs SET non_terminal_count = MAX(0, non_terminal_count - @cnt) WHERE name = @name", tx);
        var nameParam = cmd.Parameters.AddWithValue("@name", DBNull.Value);
        var cntParam = cmd.Parameters.AddWithValue("@cnt", 0);

        foreach (var (jn, cnt) in jobNameToCount)
        {
            nameParam.Value = jn;
            cntParam.Value = cnt;
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task DecrementRunningCountsAsync(SqliteConnection conn, SqliteTransaction tx,
        IReadOnlyDictionary<string, int> jobNameToCount, CancellationToken cancellationToken)
    {
        if (jobNameToCount.Count == 0)
        {
            return;
        }

        await using var jobCmd = CreateCommand(conn,
            "UPDATE surefire_jobs SET running_count = MAX(0, running_count - @cnt) WHERE name = @name", tx);
        var jobNameParam = jobCmd.Parameters.AddWithValue("@name", DBNull.Value);
        var jobCntParam = jobCmd.Parameters.AddWithValue("@cnt", 0);
        await using var queueCmd = CreateCommand(conn,
            """
            UPDATE surefire_queues
            SET running_count = MAX(0, running_count - @cnt)
            WHERE name = (
                SELECT COALESCE(j.queue, 'default')
                FROM surefire_jobs j WHERE j.name = @name
            )
            """, tx);
        var queueNameParam = queueCmd.Parameters.AddWithValue("@name", DBNull.Value);
        var queueCntParam = queueCmd.Parameters.AddWithValue("@cnt", 0);

        foreach (var (jn, cnt) in jobNameToCount)
        {
            jobNameParam.Value = jn;
            jobCntParam.Value = cnt;
            await jobCmd.ExecuteNonQueryAsync(cancellationToken);

            queueNameParam.Value = jn;
            queueCntParam.Value = cnt;
            await queueCmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task AcquireRateLimitByNameAsync(
        SqliteConnection conn,
        string rateLimitName,
        int count,
        string now,
        SqliteTransaction tx,
        CancellationToken cancellationToken)
    {
        await using var cmd = CreateCommand(conn, """
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
                                                          WHEN window_start IS NULL THEN @count
                                                          WHEN julianday(window_start)
                                                              + CAST("window" AS REAL) / 864000000000.0
                                                              <= julianday(@now)
                                                          THEN @count
                                                          ELSE current_count + @count
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
        cmd.Parameters.AddWithValue("@name", rateLimitName);
        cmd.Parameters.AddWithValue("@now", now);
        cmd.Parameters.AddWithValue("@count", count);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
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
