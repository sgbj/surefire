using System.Collections;
using System.Data;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;

namespace Surefire.SqlServer;

/// <summary>
///     SQL Server implementation of <see cref="IJobStore" />. Requires
///     <c>READ_COMMITTED_SNAPSHOT</c> on the target database.
/// </summary>
internal sealed class SqlServerJobStore(
    string connectionString,
    TimeSpan? commandTimeout,
    TimeProvider timeProvider) : IJobStore
{
    private const string SchemaV1Sql = """
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
                                           source_code NVARCHAR(MAX),
                                           last_heartbeat_at DATETIMEOFFSET,
                                           last_cron_fire_at DATETIMEOFFSET,
                                           running_count INT NOT NULL DEFAULT 0,
                                           non_terminal_count INT NOT NULL DEFAULT 0
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
                                           canceled_at DATETIMEOFFSET,
                                           node_name NVARCHAR(450),
                                           attempt INT NOT NULL DEFAULT 1,
                                           lease_epoch BIGINT NOT NULL DEFAULT 0,
                                           failure_count INT NOT NULL DEFAULT 0,
                                           replay_count INT NOT NULL DEFAULT 0,
                                           trace_id NVARCHAR(450),
                                           span_id NVARCHAR(450),
                                           parent_trace_id NVARCHAR(450),
                                           parent_span_id NVARCHAR(450),
                                           parent_run_id NVARCHAR(450),
                                           root_run_id NVARCHAR(450),
                                            rerun_of_run_id NVARCHAR(450),
                                            not_before DATETIMEOFFSET NOT NULL,
                                            not_after DATETIMEOFFSET,
                                            expires_at DATETIMEOFFSET,
                                            priority INT NOT NULL DEFAULT 0,
                                           deduplication_id NVARCHAR(450),
                                           last_heartbeat_at DATETIMEOFFSET,
                                           batch_id NVARCHAR(450)
                                       );

                                       -- LOCK_ESCALATION = DISABLE prevents the engine from converting many row locks
                                       -- into a table lock when a single statement exceeds 5000 rows. The cancel-subtree
                                       -- and bulk-purge paths can touch large slices of surefire_runs; without this, an
                                       -- escalated table-X stalls every other writer.
                                       ALTER TABLE dbo.surefire_runs SET (LOCK_ESCALATION = DISABLE);

                                       IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'ix_surefire_runs_claim')
                                       CREATE INDEX ix_surefire_runs_claim
                                            ON dbo.surefire_runs (priority DESC, not_before, id)
                                            INCLUDE (job_name, not_after, expires_at)
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

                                       IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_surefire_runs_stale_heartbeat')
                                       CREATE INDEX ix_surefire_runs_stale_heartbeat
                                           ON dbo.surefire_runs (last_heartbeat_at)
                                           WHERE status = 1;

                                       IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_surefire_runs_expiring')
                                       CREATE INDEX ix_surefire_runs_expiring
                                            ON dbo.surefire_runs (not_after)
                                            WHERE status = 0 AND lease_epoch = 0 AND not_after IS NOT NULL;

                                        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_surefire_runs_expires_at')
                                        CREATE INDEX ix_surefire_runs_expires_at
                                            ON dbo.surefire_runs (expires_at)
                                            WHERE status <> 2 AND status <> 4 AND status <> 5 AND expires_at IS NOT NULL;

                                       IF OBJECT_ID('dbo.surefire_batches', 'U') IS NULL
                                       CREATE TABLE dbo.surefire_batches (
                                           id NVARCHAR(450) NOT NULL PRIMARY KEY,
                                           status SMALLINT NOT NULL DEFAULT 0,
                                           total INT NOT NULL DEFAULT 0,
                                           succeeded INT NOT NULL DEFAULT 0,
                                           failed INT NOT NULL DEFAULT 0,
                                           canceled INT NOT NULL DEFAULT 0,
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
                                           -- Auto-stamped per-database monotonic version. Event-tail readers clamp `id`
                                           -- paging to a commit-stable horizon by excluding rows at or above the lowest
                                           -- in-flight tx's first id (probed via MIN_ACTIVE_ROWVERSION). IDENTITY allocates
                                           -- `id` at INSERT time but the row is only visible at COMMIT, so concurrent
                                           -- writers commit out of id order; a naive `id > @since` cursor would skip
                                           -- in-flight low ids when they finally commit below the cursor.
                                           commit_seq ROWVERSION NOT NULL,
                                           CONSTRAINT fk_events_run_id FOREIGN KEY (run_id) REFERENCES dbo.surefire_runs(id) ON DELETE CASCADE
                                       );

                                       ALTER TABLE dbo.surefire_events SET (LOCK_ESCALATION = DISABLE);

                                       IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'ix_surefire_events_run')
                                       CREATE INDEX ix_surefire_events_run
                                           ON dbo.surefire_events (run_id, id);

                                       IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'ix_surefire_events_commit_seq')
                                       CREATE INDEX ix_surefire_events_commit_seq
                                           ON dbo.surefire_events (commit_seq, id);

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
                                           last_heartbeat_at DATETIMEOFFSET,
                                           running_count INT NOT NULL DEFAULT 0
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

                                       -- Table-valued parameter types for bulk run/event inserts. Reusing one execution
                                       -- plan across all callers regardless of input size, cheaper than VALUES-list packing
                                       -- and avoids the 2100-parameter cap.
                                       IF TYPE_ID(N'dbo.surefire_runs_input') IS NULL
                                       CREATE TYPE dbo.surefire_runs_input AS TABLE (
                                           id NVARCHAR(450) NOT NULL PRIMARY KEY,
                                           job_name NVARCHAR(450) NOT NULL,
                                           status INT NOT NULL,
                                           arguments NVARCHAR(MAX),
                                           result NVARCHAR(MAX),
                                           reason NVARCHAR(MAX),
                                           progress FLOAT NOT NULL,
                                           created_at DATETIMEOFFSET NOT NULL,
                                           started_at DATETIMEOFFSET,
                                           completed_at DATETIMEOFFSET,
                                           canceled_at DATETIMEOFFSET,
                                           node_name NVARCHAR(450),
                                           attempt INT NOT NULL,
                                           trace_id NVARCHAR(450),
                                           span_id NVARCHAR(450),
                                           parent_trace_id NVARCHAR(450),
                                           parent_span_id NVARCHAR(450),
                                           parent_run_id NVARCHAR(450),
                                           root_run_id NVARCHAR(450),
                                            rerun_of_run_id NVARCHAR(450),
                                            not_before DATETIMEOFFSET NOT NULL,
                                            not_after DATETIMEOFFSET,
                                            expires_at DATETIMEOFFSET,
                                            priority INT NOT NULL,
                                           deduplication_id NVARCHAR(450),
                                           last_heartbeat_at DATETIMEOFFSET,
                                           batch_id NVARCHAR(450)
                                       );

                                       IF TYPE_ID(N'dbo.surefire_events_input') IS NULL
                                       CREATE TYPE dbo.surefire_events_input AS TABLE (
                                           run_id NVARCHAR(450) NOT NULL,
                                           event_type SMALLINT NOT NULL,
                                           payload NVARCHAR(MAX) NOT NULL,
                                           created_at DATETIMEOFFSET NOT NULL,
                                           attempt INT NOT NULL
                                       );

                                       IF TYPE_ID(N'dbo.surefire_name_list') IS NULL
                                       CREATE TYPE dbo.surefire_name_list AS TABLE (
                                           name NVARCHAR(450) NOT NULL PRIMARY KEY
                                       );

                                       IF TYPE_ID(N'dbo.surefire_name_count') IS NULL
                                       CREATE TYPE dbo.surefire_name_count AS TABLE (
                                           name NVARCHAR(450) NOT NULL PRIMARY KEY,
                                           cnt INT NOT NULL
                                       );

                                       INSERT INTO dbo.surefire_schema_migrations (version)
                                       SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM dbo.surefire_schema_migrations WHERE version = 1);
                                       """;

    private const string SchemaV2Sql = """
                                       IF NOT EXISTS (
                                           SELECT 1 FROM sys.columns
                                           WHERE object_id = OBJECT_ID('dbo.surefire_runs')
                                             AND name = 'lease_epoch'
                                        )
                                           ALTER TABLE dbo.surefire_runs ADD lease_epoch BIGINT NOT NULL CONSTRAINT df_surefire_runs_lease_epoch DEFAULT 0;

                                       IF NOT EXISTS (
                                           SELECT 1 FROM sys.columns
                                           WHERE object_id = OBJECT_ID('dbo.surefire_runs')
                                             AND name = 'failure_count'
                                       )
                                           ALTER TABLE dbo.surefire_runs ADD failure_count INT NOT NULL CONSTRAINT df_surefire_runs_failure_count DEFAULT 0;

                                       IF NOT EXISTS (
                                           SELECT 1 FROM sys.columns
                                           WHERE object_id = OBJECT_ID('dbo.surefire_runs')
                                             AND name = 'replay_count'
                                       )
                                           ALTER TABLE dbo.surefire_runs ADD replay_count INT NOT NULL CONSTRAINT df_surefire_runs_replay_count DEFAULT 0;

                                       IF NOT EXISTS (
                                           SELECT 1 FROM sys.columns
                                           WHERE object_id = OBJECT_ID('dbo.surefire_runs')
                                             AND name = 'highest_recorded_step'
                                       )
                                           ALTER TABLE dbo.surefire_runs ADD highest_recorded_step INT NOT NULL CONSTRAINT df_surefire_runs_highest_recorded_step DEFAULT 0;

                                        IF NOT EXISTS (
                                            SELECT 1 FROM sys.columns
                                            WHERE object_id = OBJECT_ID('dbo.surefire_runs')
                                              AND name = 'is_durable'
                                        )
                                            ALTER TABLE dbo.surefire_runs ADD is_durable BIT NOT NULL CONSTRAINT df_surefire_runs_is_durable DEFAULT 0;

                                        IF NOT EXISTS (
                                            SELECT 1 FROM sys.columns
                                            WHERE object_id = OBJECT_ID('dbo.surefire_runs')
                                              AND name = 'expires_at'
                                        )
                                            ALTER TABLE dbo.surefire_runs ADD expires_at DATETIMEOFFSET NULL;

                                        IF NOT EXISTS (
                                            SELECT 1 FROM sys.columns
                                            WHERE object_id = OBJECT_ID('dbo.surefire_jobs')
                                              AND name = 'source_code'
                                        )
                                            ALTER TABLE dbo.surefire_jobs ADD source_code NVARCHAR(MAX) NULL;

                                       UPDATE dbo.surefire_runs
                                       SET lease_epoch = CASE WHEN lease_epoch > attempt THEN lease_epoch ELSE attempt END,
                                           failure_count = CASE
                                               WHEN failure_count > CASE WHEN status = 5 THEN attempt ELSE CASE WHEN attempt > 0 THEN attempt - 1 ELSE 0 END END
                                                   THEN failure_count
                                               ELSE CASE WHEN status = 5 THEN attempt ELSE CASE WHEN attempt > 0 THEN attempt - 1 ELSE 0 END END
                                           END,
                                           attempt = CASE WHEN attempt > 1 THEN attempt ELSE 1 END;

                                       -- The TVP type is immutable. Drop and recreate via name swap so referring procedures
                                       -- are not affected (the type is only used inline as a parameter declaration on
                                       -- inserts, no schema-bound dependency).
                                        IF TYPE_ID(N'dbo.surefire_runs_input') IS NOT NULL
                                            AND NOT EXISTS (
                                                SELECT 1 FROM sys.columns
                                                WHERE object_id = TYPE_ID(N'dbo.surefire_runs_input')
                                                  AND name = 'expires_at'
                                            )
                                        BEGIN
                                            DROP TYPE dbo.surefire_runs_input;
                                       END;

                                       IF TYPE_ID(N'dbo.surefire_runs_input') IS NULL
                                       CREATE TYPE dbo.surefire_runs_input AS TABLE (
                                           id NVARCHAR(450) NOT NULL PRIMARY KEY,
                                           job_name NVARCHAR(450) NOT NULL,
                                           status INT NOT NULL,
                                           arguments NVARCHAR(MAX),
                                           result NVARCHAR(MAX),
                                           reason NVARCHAR(MAX),
                                           progress FLOAT NOT NULL,
                                           created_at DATETIMEOFFSET NOT NULL,
                                           started_at DATETIMEOFFSET,
                                           completed_at DATETIMEOFFSET,
                                           canceled_at DATETIMEOFFSET,
                                           node_name NVARCHAR(450),
                                           attempt INT NOT NULL,
                                           trace_id NVARCHAR(450),
                                           span_id NVARCHAR(450),
                                           parent_trace_id NVARCHAR(450),
                                           parent_span_id NVARCHAR(450),
                                           parent_run_id NVARCHAR(450),
                                           root_run_id NVARCHAR(450),
                                            rerun_of_run_id NVARCHAR(450),
                                            not_before DATETIMEOFFSET NOT NULL,
                                            not_after DATETIMEOFFSET,
                                            expires_at DATETIMEOFFSET,
                                            priority INT NOT NULL,
                                           deduplication_id NVARCHAR(450),
                                           last_heartbeat_at DATETIMEOFFSET,
                                           batch_id NVARCHAR(450),
                                           lease_epoch BIGINT NOT NULL,
                                           failure_count INT NOT NULL,
                                           replay_count INT NOT NULL,
                                           highest_recorded_step INT NOT NULL,
                                           is_durable BIT NOT NULL
                                       );

                                       -- Batch -> orchestrator backlink for the executor's claim-time snapshot.
                                       IF NOT EXISTS (
                                           SELECT 1 FROM sys.columns
                                           WHERE object_id = OBJECT_ID('dbo.surefire_batches')
                                             AND name = 'parent_run_id'
                                       )
                                           EXEC sp_executesql N'ALTER TABLE dbo.surefire_batches ADD parent_run_id NVARCHAR(450) NULL';

                                       IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_batches_parent_run_id')
                                           EXEC sp_executesql N'CREATE INDEX ix_batches_parent_run_id
                                               ON dbo.surefire_batches (parent_run_id) WHERE parent_run_id IS NOT NULL';

                                       -- Wait table backing the wake-on-all suspend / resume protocol.
                                       -- Naming is symmetric: awaiter_run_id is the orchestrator that is
                                       -- waiting; awaited_run_id / awaited_batch_id name the entity it
                                       -- waits on. exactly_one_awaited keeps the union discriminated.
                                       IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'surefire_durable_waits')
                                       BEGIN
                                           CREATE TABLE dbo.surefire_durable_waits (
                                               awaiter_run_id   NVARCHAR(450) NOT NULL,
                                               awaited_run_id   NVARCHAR(450) NULL,
                                               awaited_batch_id NVARCHAR(450) NULL,
                                               suspended_at     DATETIMEOFFSET NOT NULL,
                                               CONSTRAINT fk_durable_waits_awaiter FOREIGN KEY (awaiter_run_id)
                                                   REFERENCES dbo.surefire_runs(id) ON DELETE CASCADE,
                                               CONSTRAINT exactly_one_awaited
                                                   CHECK ((awaited_run_id IS NOT NULL AND awaited_batch_id IS NULL)
                                                       OR (awaited_run_id IS NULL AND awaited_batch_id IS NOT NULL))
                                           );
                                       END;

                                       IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_durable_waits_run_uniq')
                                           EXEC sp_executesql N'CREATE UNIQUE INDEX ix_durable_waits_run_uniq
                                               ON dbo.surefire_durable_waits (awaiter_run_id, awaited_run_id)
                                               WHERE awaited_run_id IS NOT NULL';
                                       IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_durable_waits_batch_uniq')
                                           EXEC sp_executesql N'CREATE UNIQUE INDEX ix_durable_waits_batch_uniq
                                               ON dbo.surefire_durable_waits (awaiter_run_id, awaited_batch_id)
                                               WHERE awaited_batch_id IS NOT NULL';
                                       IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_durable_waits_by_awaited_run')
                                           EXEC sp_executesql N'CREATE INDEX ix_durable_waits_by_awaited_run
                                               ON dbo.surefire_durable_waits (awaited_run_id)
                                               WHERE awaited_run_id IS NOT NULL';
                                        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_durable_waits_by_awaited_batch')
                                            EXEC sp_executesql N'CREATE INDEX ix_durable_waits_by_awaited_batch
                                                ON dbo.surefire_durable_waits (awaited_batch_id)
                                                WHERE awaited_batch_id IS NOT NULL';
                                        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'surefire_durable_records')
                                        BEGIN
                                            CREATE TABLE dbo.surefire_durable_records (
                                                orchestrator_run_id NVARCHAR(450) NOT NULL,
                                                step                INT           NOT NULL,
                                                kind                NVARCHAR(100) NOT NULL,
                                                name                NVARCHAR(450) NULL,
                                                payload             NVARCHAR(MAX) NOT NULL,
                                                created_at          DATETIMEOFFSET NOT NULL,
                                                CONSTRAINT pk_surefire_durable_records PRIMARY KEY (orchestrator_run_id, step),
                                                CONSTRAINT fk_durable_records_orchestrator FOREIGN KEY (orchestrator_run_id)
                                                    REFERENCES dbo.surefire_runs(id) ON DELETE CASCADE
                                            );
                                        END;

                                        DROP INDEX IF EXISTS ix_surefire_runs_expiring ON dbo.surefire_runs;
                                        CREATE INDEX ix_surefire_runs_expiring
                                            ON dbo.surefire_runs (not_after)
                                            WHERE status = 0 AND lease_epoch = 0 AND not_after IS NOT NULL;
                                        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_surefire_runs_expires_at')
                                            EXEC sp_executesql N'CREATE INDEX ix_surefire_runs_expires_at
                                                ON dbo.surefire_runs (expires_at)
                                                WHERE status <> 2 AND status <> 4 AND status <> 5 AND expires_at IS NOT NULL';

                                       UPDATE j
                                       SET running_count = ISNULL(r.cnt, 0)
                                       FROM dbo.surefire_jobs j
                                       LEFT JOIN (
                                           SELECT job_name, COUNT(*) AS cnt
                                           FROM dbo.surefire_runs
                                           WHERE status = 1
                                           GROUP BY job_name
                                       ) r ON r.job_name = j.name;

                                       UPDATE q
                                       SET running_count = ISNULL(r.cnt, 0)
                                       FROM dbo.surefire_queues q
                                       LEFT JOIN (
                                           SELECT ISNULL(NULLIF(j.queue, ''), 'default') AS queue_name, COUNT(*) AS cnt
                                           FROM dbo.surefire_runs r
                                           INNER JOIN dbo.surefire_jobs j ON j.name = r.job_name
                                           WHERE r.status = 1
                                           GROUP BY ISNULL(NULLIF(j.queue, ''), 'default')
                                       ) r ON r.queue_name = q.name;

                                       INSERT INTO dbo.surefire_schema_migrations (version)
                                       SELECT 2 WHERE NOT EXISTS (SELECT 1 FROM dbo.surefire_schema_migrations WHERE version = 2);
                                       """;

    private static readonly SqlMetaData[] EventsTvpMeta =
    [
        new("run_id", SqlDbType.NVarChar, 450),
        new("event_type", SqlDbType.SmallInt),
        new("payload", SqlDbType.NVarChar, -1),
        new("created_at", SqlDbType.DateTimeOffset),
        new("attempt", SqlDbType.Int)
    ];

    private static readonly SqlMetaData[] RunsTvpMeta =
    [
        new("id", SqlDbType.NVarChar, 450),
        new("job_name", SqlDbType.NVarChar, 450),
        new("status", SqlDbType.Int),
        new("arguments", SqlDbType.NVarChar, -1),
        new("result", SqlDbType.NVarChar, -1),
        new("reason", SqlDbType.NVarChar, -1),
        new("progress", SqlDbType.Float),
        new("created_at", SqlDbType.DateTimeOffset),
        new("started_at", SqlDbType.DateTimeOffset),
        new("completed_at", SqlDbType.DateTimeOffset),
        new("canceled_at", SqlDbType.DateTimeOffset),
        new("node_name", SqlDbType.NVarChar, 450),
        new("attempt", SqlDbType.Int),
        new("trace_id", SqlDbType.NVarChar, 450),
        new("span_id", SqlDbType.NVarChar, 450),
        new("parent_trace_id", SqlDbType.NVarChar, 450),
        new("parent_span_id", SqlDbType.NVarChar, 450),
        new("parent_run_id", SqlDbType.NVarChar, 450),
        new("root_run_id", SqlDbType.NVarChar, 450),
        new("rerun_of_run_id", SqlDbType.NVarChar, 450),
        new("not_before", SqlDbType.DateTimeOffset),
        new("not_after", SqlDbType.DateTimeOffset),
        new("expires_at", SqlDbType.DateTimeOffset),
        new("priority", SqlDbType.Int),
        new("deduplication_id", SqlDbType.NVarChar, 450),
        new("last_heartbeat_at", SqlDbType.DateTimeOffset),
        new("batch_id", SqlDbType.NVarChar, 450),
        new("lease_epoch", SqlDbType.BigInt),
        new("failure_count", SqlDbType.Int),
        new("replay_count", SqlDbType.Int),
        new("highest_recorded_step", SqlDbType.Int),
        new("is_durable", SqlDbType.Bit)
    ];

    private static readonly SqlMetaData[] NameListTvpMeta =
    [
        new("name", SqlDbType.NVarChar, 450)
    ];

    private static readonly SqlMetaData[] NameCountTvpMeta =
    [
        new("name", SqlDbType.NVarChar, 450),
        new("cnt", SqlDbType.Int)
    ];

    private readonly string _connectionString = connectionString;

    internal int? CommandTimeoutSeconds { get; } =
        CommandTimeouts.ToSeconds(commandTimeout, nameof(commandTimeout));

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT 1";
        _ = await cmd.ExecuteScalarAsync(cancellationToken).WithSqlCancellation(cancellationToken);
    }

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);

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
        await lockCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);

        try
        {
            await using var migCmd = CreateCommand(conn);
            migCmd.CommandText = """
                                 IF OBJECT_ID('dbo.surefire_schema_migrations', 'U') IS NULL
                                 CREATE TABLE dbo.surefire_schema_migrations (version INT NOT NULL PRIMARY KEY);
                                 """;
            await migCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);

            await using var checkCmd = CreateCommand(conn);
            checkCmd.CommandText = "SELECT ISNULL(MAX(version), 0) FROM dbo.surefire_schema_migrations";
            var currentVersion =
                (int)(await checkCmd.ExecuteScalarAsync(cancellationToken).WithSqlCancellation(cancellationToken))!;

            if (currentVersion < 1)
            {
                await using var cmd = CreateCommand(conn);
                cmd.CommandText = SchemaV1Sql;
                await cmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            }

            if (currentVersion < 2)
            {
                await using var cmd = CreateCommand(conn);
                cmd.CommandText = SchemaV2Sql;
                await cmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
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

    public async Task UpsertJobsAsync(IReadOnlyList<JobDefinition> jobs,
        CancellationToken cancellationToken = default)
    {
        if (jobs.Count == 0)
        {
            return;
        }

        // is_enabled and last_cron_fire_at are intentionally not in the UPDATE list so dashboard
        // toggles and cron-fire bookkeeping survive re-upserts.
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = $$"""
                            SET NOCOUNT ON; SET XACT_ABORT ON;

                            DECLARE @input TABLE (
                                name NVARCHAR(450) NOT NULL PRIMARY KEY,
                                description NVARCHAR(MAX),
                                tags NVARCHAR(MAX),
                                cron_expression NVARCHAR(MAX),
                                time_zone_id NVARCHAR(450),
                                timeout BIGINT,
                                max_concurrency INT,
                                priority INT,
                                retry_policy NVARCHAR(MAX),
                                is_continuous BIT,
                                queue NVARCHAR(450),
                                rate_limit_name NVARCHAR(450),
                                is_enabled BIT,
                                misfire_policy INT,
                                fire_all_limit INT,
                                arguments_schema NVARCHAR(MAX),
                                source_code NVARCHAR(MAX)
                            );

                            INSERT INTO @input (
                                name, description, tags, cron_expression, time_zone_id, timeout,
                                max_concurrency, priority, retry_policy, is_continuous, queue,
                                rate_limit_name, is_enabled, misfire_policy, fire_all_limit, arguments_schema,
                                source_code
                            )
                            SELECT name, description, tags, cron_expression, time_zone_id, timeout,
                                max_concurrency, priority, retry_policy, is_continuous, queue,
                                rate_limit_name, is_enabled, misfire_policy, fire_all_limit, arguments_schema,
                                source_code
                            FROM OPENJSON(@payload)
                            WITH (
                                name NVARCHAR(450) '$.name',
                                description NVARCHAR(MAX) '$.description',
                                tags NVARCHAR(MAX) '$.tags' AS JSON,
                                cron_expression NVARCHAR(MAX) '$.cronExpression',
                                time_zone_id NVARCHAR(450) '$.timeZoneId',
                                timeout BIGINT '$.timeout',
                                max_concurrency INT '$.maxConcurrency',
                                priority INT '$.priority',
                                retry_policy NVARCHAR(MAX) '$.retryPolicy' AS JSON,
                                is_continuous BIT '$.isContinuous',
                                queue NVARCHAR(450) '$.queue',
                                rate_limit_name NVARCHAR(450) '$.rateLimitName',
                                is_enabled BIT '$.isEnabled',
                                misfire_policy INT '$.misfirePolicy',
                                fire_all_limit INT '$.fireAllLimit',
                                arguments_schema NVARCHAR(MAX) '$.argumentsSchema',
                                source_code NVARCHAR(MAX) '$.sourceCode'
                            );

                            {{BuildSortedApplockSql("@input", "surefire_job:", "uj")}}

                            DECLARE @lock_sink NVARCHAR(450);
                            SELECT @lock_sink = j.name FROM @input s
                            INNER JOIN dbo.surefire_jobs j WITH (UPDLOCK, ROWLOCK) ON j.name = s.name
                            ORDER BY s.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);

                            UPDATE j SET
                                description = s.description,
                                tags = s.tags,
                                cron_expression = s.cron_expression,
                                time_zone_id = s.time_zone_id,
                                timeout = s.timeout,
                                max_concurrency = s.max_concurrency,
                                priority = s.priority,
                                retry_policy = s.retry_policy,
                                is_continuous = s.is_continuous,
                                queue = s.queue,
                                rate_limit_name = s.rate_limit_name,
                                misfire_policy = s.misfire_policy,
                                fire_all_limit = s.fire_all_limit,
                                arguments_schema = s.arguments_schema,
                                source_code = s.source_code,
                                last_heartbeat_at = SYSUTCDATETIME()
                            FROM dbo.surefire_jobs j
                            INNER JOIN @input s ON s.name = j.name;

                            INSERT INTO dbo.surefire_jobs (
                                name, description, tags, cron_expression, time_zone_id, timeout,
                                max_concurrency, priority, retry_policy, is_continuous, queue,
                                rate_limit_name, is_enabled, misfire_policy, fire_all_limit, arguments_schema,
                                source_code, last_heartbeat_at
                            )
                            SELECT s.name, s.description, s.tags, s.cron_expression, s.time_zone_id, s.timeout,
                                s.max_concurrency, s.priority, s.retry_policy, s.is_continuous, s.queue,
                                s.rate_limit_name, s.is_enabled, s.misfire_policy, s.fire_all_limit,
                                s.arguments_schema, s.source_code, SYSUTCDATETIME()
                            FROM @input s
                            WHERE NOT EXISTS (SELECT 1 FROM dbo.surefire_jobs j WHERE j.name = s.name)
                            ORDER BY s.name;
                            """;
        cmd.Parameters.Add(CreateParameter("@payload", UpsertPayloadFactory.SerializeJobs(jobs)));

        try
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        }
        catch (SqlException ex) when (ex.Number is 2601 or 2627)
        {
            // A concurrent upsert won the insert race; the row exists, which is what we want.
        }
    }

    public async Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM dbo.surefire_jobs WHERE name = @name";
        cmd.Parameters.Add(CreateParameter("@name", name));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            return null;
        }

        return ReadJob(reader);
    }

    public async Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
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
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            results.Add(ReadJob(reader));
        }

        return results;
    }

    public async Task SetJobEnabledAsync(string name, bool enabled, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "UPDATE dbo.surefire_jobs SET is_enabled = @enabled WHERE name = @name";
        cmd.Parameters.Add(CreateParameter("@name", name));
        cmd.Parameters.Add(CreateParameter("@enabled", enabled));
        await cmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
    }

    public async Task UpdateLastCronFireAtAsync(string jobName, DateTimeOffset fireAt,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "UPDATE dbo.surefire_jobs SET last_cron_fire_at = @fire_at WHERE name = @name";
        cmd.Parameters.Add(CreateParameter("@name", jobName));
        cmd.Parameters.Add(CreateParameter("@fire_at", fireAt));
        await cmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
    }

    public async Task UpsertQueuesAsync(IReadOnlyList<QueueDefinition> queues,
        CancellationToken cancellationToken = default)
    {
        if (queues.Count == 0)
        {
            return;
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = $$"""
                            SET NOCOUNT ON; SET XACT_ABORT ON;

                            DECLARE @input TABLE (
                                name NVARCHAR(450) NOT NULL PRIMARY KEY,
                                priority INT,
                                max_concurrency INT,
                                is_paused BIT,
                                rate_limit_name NVARCHAR(450)
                            );

                            INSERT INTO @input (name, priority, max_concurrency, is_paused, rate_limit_name)
                            SELECT name, priority, max_concurrency, is_paused, rate_limit_name
                            FROM OPENJSON(@payload)
                            WITH (
                                name NVARCHAR(450) '$.name',
                                priority INT '$.priority',
                                max_concurrency INT '$.maxConcurrency',
                                is_paused BIT '$.isPaused',
                                rate_limit_name NVARCHAR(450) '$.rateLimitName'
                            );

                            {{BuildSortedApplockSql("@input", "surefire_queue:", "uq")}}

                            DECLARE @lock_sink NVARCHAR(450);
                            SELECT @lock_sink = q.name FROM @input s
                            INNER JOIN dbo.surefire_queues q WITH (UPDLOCK, ROWLOCK) ON q.name = s.name
                            ORDER BY s.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);

                            UPDATE q SET
                                priority = s.priority,
                                max_concurrency = s.max_concurrency,
                                rate_limit_name = s.rate_limit_name,
                                last_heartbeat_at = SYSUTCDATETIME()
                            FROM dbo.surefire_queues q
                            INNER JOIN @input s ON s.name = q.name;

                            INSERT INTO dbo.surefire_queues (name, priority, max_concurrency, is_paused, rate_limit_name, last_heartbeat_at)
                            SELECT s.name, s.priority, s.max_concurrency, s.is_paused, s.rate_limit_name, SYSUTCDATETIME()
                            FROM @input s
                            WHERE NOT EXISTS (SELECT 1 FROM dbo.surefire_queues q WHERE q.name = s.name)
                            ORDER BY s.name;
                            """;
        cmd.Parameters.Add(CreateParameter("@payload", UpsertPayloadFactory.SerializeQueues(queues)));

        try
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        }
        catch (SqlException ex) when (ex.Number is 2601 or 2627)
        {
        }
    }

    public async Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM dbo.surefire_queues";

        var results = new List<QueueDefinition>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            results.Add(ReadQueue(reader));
        }

        return results;
    }

    public async Task<bool> SetQueuePausedAsync(string name, bool isPaused,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "UPDATE dbo.surefire_queues SET is_paused = @is_paused WHERE name = @name";
        cmd.Parameters.Add(CreateParameter("@name", name));
        cmd.Parameters.Add(CreateParameter("@is_paused", isPaused));
        return await cmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken) > 0;
    }

    public async Task UpsertRateLimitsAsync(IReadOnlyList<RateLimitDefinition> rateLimits,
        CancellationToken cancellationToken = default)
    {
        if (rateLimits.Count == 0)
        {
            return;
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = $$"""
                            SET NOCOUNT ON; SET XACT_ABORT ON;

                            DECLARE @input TABLE (
                                name NVARCHAR(450) NOT NULL PRIMARY KEY,
                                type INT,
                                max_permits INT,
                                [window] BIGINT
                            );

                            INSERT INTO @input (name, type, max_permits, [window])
                            SELECT name, type, max_permits, [window]
                            FROM OPENJSON(@payload)
                            WITH (
                                name NVARCHAR(450) '$.name',
                                type INT '$.type',
                                max_permits INT '$.maxPermits',
                                [window] BIGINT '$.window'
                            );

                            {{BuildSortedApplockSql("@input", "surefire_rl:", "url")}}

                            DECLARE @lock_sink NVARCHAR(450);
                            SELECT @lock_sink = rl.name FROM @input s
                            INNER JOIN dbo.surefire_rate_limits rl WITH (UPDLOCK, ROWLOCK) ON rl.name = s.name
                            ORDER BY s.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);

                            UPDATE rl SET
                                type = s.type,
                                max_permits = s.max_permits,
                                [window] = s.[window],
                                last_heartbeat_at = SYSUTCDATETIME()
                            FROM dbo.surefire_rate_limits rl
                            INNER JOIN @input s ON s.name = rl.name;

                            INSERT INTO dbo.surefire_rate_limits (name, type, max_permits, [window], last_heartbeat_at)
                            SELECT s.name, s.type, s.max_permits, s.[window], SYSUTCDATETIME()
                            FROM @input s
                            WHERE NOT EXISTS (SELECT 1 FROM dbo.surefire_rate_limits rl WHERE rl.name = s.name)
                            ORDER BY s.name;
                            """;
        cmd.Parameters.Add(CreateParameter("@payload", UpsertPayloadFactory.SerializeRateLimits(rateLimits)));

        try
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        }
        catch (SqlException ex) when (ex.Number is 2601 or 2627)
        {
        }
    }

    public async Task<JobRun?> GetRunAsync(string id, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM dbo.surefire_runs WHERE id = @id";
        cmd.Parameters.Add(CreateParameter("@id", id));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
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
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SELECT r.* FROM dbo.surefire_runs r
                          INNER JOIN @ids i ON i.name = r.id
                          """;
        cmd.Parameters.Add(CreateTvpParameter("@ids", "dbo.surefire_name_list", NamesToTvp(ids)));

        var byId = new Dictionary<string, JobRun>(ids.Count, StringComparer.Ordinal);
        await using (var reader =
                     await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                var run = ReadRun(reader);
                byId[run.Id] = run;
            }
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
            throw new ArgumentException("afterCursor and beforeCursor are mutually exclusive.", nameof(afterCursor));
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
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = sql;
        cmd.Parameters.Add(CreateParameter("@parent", parentRunId));
        cmd.Parameters.Add(CreateParameter("@take", take + 1));
        if ((after ?? before) is { } c)
        {
            cmd.Parameters.Add(CreateParameter("@cts", c.CreatedAt));
            cmd.Parameters.Add(CreateParameter("@cid", c.Id));
        }

        var items = new List<JobRun>(take + 1);
        await using (var reader =
                     await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
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
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
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
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
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
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);

        var whereParts = new List<string>();
        await using var countCmd = CreateCommand(conn);
        BuildRunFilterWhere(filter, whereParts, countCmd);
        var whereClause = whereParts.Count > 0 ? "WHERE " + string.Join(" AND ", whereParts) : "";

        countCmd.CommandText = $"SELECT COUNT(*) FROM dbo.surefire_runs {whereClause}";
        var totalCount =
            (int)(await countCmd.ExecuteScalarAsync(cancellationToken).WithSqlCancellation(cancellationToken))!;

        if (totalCount == 0 || skip >= totalCount)
        {
            return new() { Items = [], TotalCount = totalCount };
        }

        var dir = filter.Direction == RunOrderDirection.Ascending ? "ASC" : "DESC";
        // Nulls always last: T-SQL has no NULLS LAST keyword, so prepend an "IS NULL" CASE to
        // bucket non-null rows first regardless of direction. Cheap and uses the index.
        var orderBy = filter.OrderBy switch
        {
            RunOrderBy.StartedAt =>
                $"CASE WHEN started_at IS NULL THEN 1 ELSE 0 END, started_at {dir}, id {dir}",
            RunOrderBy.CompletedAt =>
                $"CASE WHEN completed_at IS NULL THEN 1 ELSE 0 END, completed_at {dir}, id {dir}",
            _ => $"created_at {dir}, id {dir}"
        };

        await using var cmd = CreateCommand(conn);
        BuildRunFilterWhere(filter, [], cmd);
        cmd.CommandText =
            $"SELECT * FROM dbo.surefire_runs {whereClause} ORDER BY {orderBy} OFFSET @skip ROWS FETCH NEXT @take ROWS ONLY";
        cmd.Parameters.Add(CreateParameter("@take", take));
        cmd.Parameters.Add(CreateParameter("@skip", skip));

        var items = new List<JobRun>();
        await using (var reader =
                     await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                items.Add(ReadRun(reader));
            }
        }

        return new() { Items = items, TotalCount = totalCount };
    }

    public async Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
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

        await cmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
    }

    public async Task<DurableSuspendOutcome> TrySuspendRunAsync(string runId, long expectedLeaseEpoch,
        IReadOnlyCollection<string> awaitedRunIds,
        IReadOnlyCollection<string> awaitedBatchIds,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        // The orchestrator and every awaited entity are locked in sorted-id order under UPDLOCK
        // ROWLOCK so a concurrent terminal transition on an awaited entity serializes against this
        // suspend - the "child terminated between EXISTS-check and wait-row insert" race is
        // impossible by construction. Per-run applock and job/queue applocks serialize this run's
        // counter mutation against claim/transition writers on the same job/queue.
        cmd.CommandText = """
                          SET NOCOUNT ON; SET XACT_ABORT ON;

                          DECLARE @run_lock_r INT;
                          EXEC @run_lock_r = sp_getapplock @Resource = @run_lock_key,
                              @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;
                          IF @run_lock_r < 0 THROW 50004, 'Failed to acquire run lock', 1;

                          DECLARE @run_job_name NVARCHAR(450);
                          SELECT @run_job_name = job_name FROM dbo.surefire_runs WHERE id = @id;
                          DECLARE @run_queue_name NVARCHAR(450) = N'default';
                          SELECT @run_queue_name = COALESCE(queue, N'default')
                              FROM dbo.surefire_jobs WHERE name = @run_job_name;

                          DECLARE @meta_lock_r INT;
                          IF @run_job_name IS NOT NULL
                          BEGIN
                              DECLARE @meta_job_key NVARCHAR(900) = N'surefire_job:' + @run_job_name;
                              EXEC @meta_lock_r = sp_getapplock @Resource = @meta_job_key,
                                  @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;
                              IF @meta_lock_r < 0 THROW 50005, 'Failed to acquire job meta lock', 1;
                              DECLARE @meta_queue_key NVARCHAR(900) = N'surefire_queue:' + @run_queue_name;
                              EXEC @meta_lock_r = sp_getapplock @Resource = @meta_queue_key,
                                  @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;
                              IF @meta_lock_r < 0 THROW 50005, 'Failed to acquire queue meta lock', 1;
                          END

                          DECLARE @lock_sink NVARCHAR(450);
                          SELECT @lock_sink = name FROM dbo.surefire_jobs WITH (UPDLOCK, ROWLOCK)
                              WHERE name = @run_job_name;
                          SELECT @lock_sink = name FROM dbo.surefire_queues WITH (UPDLOCK, ROWLOCK)
                              WHERE name = @run_queue_name;

                          -- Materialize awaited ids into a single sorted set, including the
                          -- orchestrator itself, so the SELECT below acquires UPDLOCK/ROWLOCK in
                          -- sorted-id order. Both run-id and batch-id sets are sorted independently
                          -- (different tables) but share the deterministic per-table acquisition
                          -- order that prevents deadlocks across concurrent suspends.
                          DECLARE @lock_run_ids TABLE (id NVARCHAR(450) NOT NULL PRIMARY KEY);
                          INSERT INTO @lock_run_ids (id) VALUES (@id);
                          INSERT INTO @lock_run_ids (id)
                              SELECT name FROM @awaited_runs WHERE name <> @id;
                          DECLARE @lock_batch_ids TABLE (id NVARCHAR(450) NOT NULL PRIMARY KEY);
                          INSERT INTO @lock_batch_ids (id) SELECT name FROM @awaited_batches;

                          SELECT @lock_sink = r.id FROM @lock_run_ids lri
                              INNER JOIN dbo.surefire_runs r WITH (UPDLOCK, ROWLOCK) ON r.id = lri.id
                              ORDER BY lri.id OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);
                          SELECT @lock_sink = b.id FROM @lock_batch_ids lbi
                              INNER JOIN dbo.surefire_batches b WITH (UPDLOCK, ROWLOCK) ON b.id = lbi.id
                              ORDER BY lbi.id OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);

                          -- Decide destination by checking whether any awaited entity is still
                          -- non-terminal under the just-acquired row locks. If everything is
                          -- already terminal, the orchestrator goes straight to Pending so the
                          -- next claim sweep replays immediately.
                          DECLARE @has_nonterminal_await BIT =
                              CASE WHEN EXISTS (
                                  SELECT 1 FROM @awaited_runs a
                                  INNER JOIN dbo.surefire_runs r ON r.id = a.name
                                  WHERE r.status NOT IN (2, 4, 5)
                              ) OR EXISTS (
                                  SELECT 1 FROM @awaited_batches a
                                  INNER JOIN dbo.surefire_batches b ON b.id = a.name
                                  WHERE b.status NOT IN (2, 4, 5)
                              ) THEN 1 ELSE 0 END;

                          DECLARE @new_status INT = CASE WHEN @has_nonterminal_await = 1 THEN 3 ELSE 0 END;
                          DECLARE @upd TABLE (attempt INT NOT NULL);

                          UPDATE dbo.surefire_runs SET
                              status = @new_status,
                              node_name = NULL,
                              not_before = CASE WHEN @has_nonterminal_await = 1 THEN not_before ELSE @now END,
                              last_heartbeat_at = @now,
                              replay_count = replay_count + CASE WHEN @has_nonterminal_await = 1 THEN 0 ELSE 1 END
                           OUTPUT INSERTED.attempt INTO @upd (attempt)
                           WHERE id = @id AND status = 1 AND lease_epoch = @expected_lease_epoch;

                          DECLARE @transitioned INT = (SELECT COUNT(*) FROM @upd);

                           IF @transitioned > 0
                           BEGIN
                               -- Running -> Suspended/Pending releases the slot. The later claim reacquires it.
                               UPDATE dbo.surefire_jobs SET running_count =
                                   CASE WHEN running_count > 0 THEN running_count - 1 ELSE 0 END
                              WHERE name = @run_job_name;
                              UPDATE dbo.surefire_queues SET running_count =
                                  CASE WHEN running_count > 0 THEN running_count - 1 ELSE 0 END
                              WHERE name = @run_queue_name;
                          END

                          IF @transitioned > 0 AND @new_status = 3
                          BEGIN
                              -- Insert wait rows for every still-non-terminal awaited entity. Already
                              -- terminal entries are filtered out so the wake mechanism doesn't see
                              -- ghost rows. The UNIQUE indexes plus IGNORE_DUP_KEY-equivalent via
                              -- NOT EXISTS prevent duplicate pairs.
                              INSERT INTO dbo.surefire_durable_waits (awaiter_run_id, awaited_run_id, awaited_batch_id, suspended_at)
                              SELECT @id, r.id, NULL, @now FROM @awaited_runs a
                              INNER JOIN dbo.surefire_runs r ON r.id = a.name
                              WHERE r.status NOT IN (2, 4, 5)
                                AND NOT EXISTS (
                                    SELECT 1 FROM dbo.surefire_durable_waits w
                                    WHERE w.awaiter_run_id = @id AND w.awaited_run_id = r.id
                                );

                              INSERT INTO dbo.surefire_durable_waits (awaiter_run_id, awaited_run_id, awaited_batch_id, suspended_at)
                              SELECT @id, NULL, b.id, @now FROM @awaited_batches a
                              INNER JOIN dbo.surefire_batches b ON b.id = a.name
                              WHERE b.status NOT IN (2, 4, 5)
                                AND NOT EXISTS (
                                    SELECT 1 FROM dbo.surefire_durable_waits w
                                    WHERE w.awaiter_run_id = @id AND w.awaited_batch_id = b.id
                                );
                          END

                          IF @transitioned > 0
                          BEGIN
                              INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                              SELECT @id, 0, CONVERT(NVARCHAR(10), @new_status), @now, u.attempt FROM @upd u;
                          END

                          SELECT @transitioned AS transitioned, @new_status AS new_status;
                          """;

        cmd.Parameters.Add(CreateParameter("@id", runId));
        cmd.Parameters.Add(CreateParameter("@run_lock_key", "surefire_run:" + runId));
        cmd.Parameters.Add(CreateParameter("@expected_lease_epoch", expectedLeaseEpoch));
        cmd.Parameters.Add(CreateParameter("@now", now));
        cmd.Parameters.Add(CreateTvpParameter("@awaited_runs", "dbo.surefire_name_list",
            NamesToTvp(awaitedRunIds)));
        cmd.Parameters.Add(CreateTvpParameter("@awaited_batches", "dbo.surefire_name_list",
            NamesToTvp(awaitedBatchIds)));

        var outcome = DurableSuspendOutcome.NotTransitioned;
        await using (var reader =
                     await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                if (reader.GetInt32(0) > 0)
                {
                    outcome = (JobStatus)reader.GetInt32(1) == JobStatus.Suspended
                        ? DurableSuspendOutcome.Suspended
                        : DurableSuspendOutcome.ImmediatePending;
                }
            }
        }

        await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        return outcome;
    }

    public async Task<DurableExecutionSnapshot> LoadExecutionSnapshotAsync(string orchestratorRunId,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);

        // One round trip with three SELECTs under SNAPSHOT isolation gives a coherent view
        // of the orchestrator and its children. Without snapshot the three reads could see
        // a torn state if a child terminal commits between reads, surfacing a child as
        // non-terminal in `children` whose batch has already counted it.
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.Snapshot, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        int highestRecordedStep = 0;
        var children = new Dictionary<string, JobRun>(StringComparer.Ordinal);
        var childBatches = new Dictionary<string, JobBatch>(StringComparer.Ordinal);
        var records = new Dictionary<int, DurableRecord>();

        await using (var cmd = CreateCommand(conn))
        {
            cmd.Transaction = tx;
            cmd.CommandText = """
                              SELECT highest_recorded_step
                              FROM dbo.surefire_runs WHERE id = @id;

                              SELECT * FROM dbo.surefire_runs WHERE parent_run_id = @id;

                              SELECT * FROM dbo.surefire_batches WHERE parent_run_id = @id;

                              SELECT * FROM dbo.surefire_durable_records WHERE orchestrator_run_id = @id;
                              """;
            cmd.Parameters.Add(CreateParameter("@id", orchestratorRunId));
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                .WithSqlCancellation(cancellationToken);
            if (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                highestRecordedStep = reader.GetInt32(0);
            }

            await reader.NextResultAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                var run = ReadRun(reader);
                children[run.Id] = run;
            }

            await reader.NextResultAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                var batch = ReadBatch(reader);
                childBatches[batch.Id] = batch;
            }

            await reader.NextResultAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                var record = ReadDurableRecord(reader);
                records[record.Step] = record;
            }
        }

        await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        return new DurableExecutionSnapshot(children, childBatches, records, highestRecordedStep);
    }

    public async Task<DurableRecord> CreateDurableRecordAsync(DurableRecord record,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          SET NOCOUNT ON; SET XACT_ABORT ON;

                          DECLARE @inserted INT = 0;

                          IF NOT EXISTS (
                              SELECT 1 FROM dbo.surefire_durable_records WITH (UPDLOCK, HOLDLOCK)
                              WHERE orchestrator_run_id = @orchestrator_run_id AND step = @step
                          )
                          BEGIN
                              INSERT INTO dbo.surefire_durable_records (
                                  orchestrator_run_id, step, kind, name, payload, created_at
                              )
                              VALUES (
                                  @orchestrator_run_id, @step, @kind, @name, @payload, @created_at
                              );

                              SET @inserted = 1;

                              UPDATE dbo.surefire_runs
                              SET highest_recorded_step = CASE
                                      WHEN highest_recorded_step > @step THEN highest_recorded_step
                                      ELSE @step
                                  END
                              WHERE id = @orchestrator_run_id;
                          END

                          SELECT @inserted;
                          """;
        AddDurableRecordParameters(cmd, record);
        var inserted =
            Convert.ToInt32(await cmd.ExecuteScalarAsync(cancellationToken).WithSqlCancellation(cancellationToken)) > 0;
        if (inserted)
        {
            await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            return record;
        }

        DurableRecord? existing = null;
        await using (var select = CreateCommand(conn))
        {
            select.Transaction = tx;
            select.CommandText = """
                                 SELECT * FROM dbo.surefire_durable_records
                                 WHERE orchestrator_run_id = @orchestrator_run_id AND step = @step;
                                 """;
            select.Parameters.Add(CreateParameter("@orchestrator_run_id", record.OrchestratorRunId));
            select.Parameters.Add(CreateParameter("@step", record.Step));
            await using var reader = await select.ExecuteReaderAsync(cancellationToken)
                .WithSqlCancellation(cancellationToken);
            if (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                existing = ReadDurableRecord(reader);
            }
        }

        if (existing is { } found && DurableRecordsEqual(found, record))
        {
            await using var update = CreateCommand(conn);
            update.Transaction = tx;
            update.CommandText = """
                                 UPDATE dbo.surefire_runs
                                 SET highest_recorded_step = CASE
                                         WHEN highest_recorded_step > @step THEN highest_recorded_step
                                         ELSE @step
                                     END
                                 WHERE id = @orchestrator_run_id;
                                 """;
            update.Parameters.Add(CreateParameter("@orchestrator_run_id", record.OrchestratorRunId));
            update.Parameters.Add(CreateParameter("@step", record.Step));
            await update.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            return found;
        }

        await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        throw new DurableReplayMismatchException(record.OrchestratorRunId, record.Step,
            $"Expected {DescribeRecord(record)}; saw {(existing is null ? "no recorded operation" : DescribeRecord(existing))}.");
    }

    /// <summary>
    ///     The unified 3-step wake performed inside every terminal transition transaction. Driven
    ///     by the persistent <c>surefire_durable_waits</c> table so wake is wake-on-all (every
    ///     awaited entity must terminate) rather than wake-on-any.
    ///     <list type="number">
    ///         <item>Delete the just-terminated run's outgoing waits (cleanup for Suspended -> Canceled).</item>
    ///         <item>Capture affected orchestrator ids and delete incoming waits referencing the terminated entity.</item>
    ///         <item>For each captured orchestrator whose wait set is now empty, transition Suspended -> Pending.</item>
    ///     </list>
    /// </summary>
    // Unified wake for a set of terminated runs + batches. Collects the combined affected
    // orchestrator set ONCE, applocks every orchestrator in sorted-id order, then performs
    // the per-cascade cleanup. This breaks the cross-helper lock-order inversion that can
    // arise when two transactions each call WakeForTerminatedRunAsync + WakeForTerminatedBatchAsync
    // in sequence on overlapping orchestrator sets - per-helper sorts are not enough because
    // the two helpers' affected sets differ. Locking the union under one canonical order is
    // the only construction that's deadlock-free for all interleavings.
    private async Task WakeForTerminatedEntitiesAsync(SqlConnection conn, SqlTransaction tx,
        IReadOnlyCollection<string> runIds,
        IReadOnlyCollection<string> batchIds,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        if (runIds.Count == 0 && batchIds.Count == 0)
        {
            return;
        }

        // One round trip with two SELECTs collects affected orchestrators across runs + batches.
        // Fast-skip: if the wait table has no rows referencing any of the terminated ids, the
        // helper returns immediately without the per-orch applock prelude or the second pass.
        var affected = new SortedSet<string>(StringComparer.Ordinal);
        await using (var collectCmd = CreateCommand(conn))
        {
            collectCmd.Transaction = tx;
            var sb = new StringBuilder("SET NOCOUNT ON;\n");
            if (runIds.Count > 0)
            {
                sb.Append("""
                          SELECT DISTINCT awaiter_run_id FROM dbo.surefire_durable_waits
                          WHERE awaited_run_id IN (SELECT name FROM @rids);

                          """);
                collectCmd.Parameters.Add(CreateTvpParameter("@rids", "dbo.surefire_name_list", NamesToTvp(runIds)));
            }
            if (batchIds.Count > 0)
            {
                sb.Append("""
                          SELECT DISTINCT awaiter_run_id FROM dbo.surefire_durable_waits
                          WHERE awaited_batch_id IN (SELECT name FROM @bids);

                          """);
                collectCmd.Parameters.Add(CreateTvpParameter("@bids", "dbo.surefire_name_list", NamesToTvp(batchIds)));
            }

            collectCmd.CommandText = sb.ToString();
            await using var reader = await collectCmd.ExecuteReaderAsync(cancellationToken)
                .WithSqlCancellation(cancellationToken);
            do
            {
                while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
                {
                    affected.Add(reader.GetString(0));
                }
            } while (await reader.NextResultAsync(cancellationToken).WithSqlCancellation(cancellationToken));
        }

        // Prelock every affected orchestrator in canonical sorted order. WakeIfWaitSetEmptyAsync's
        // internal per-orch applock is reentrant, so its no-op repeat doesn't matter; what matters
        // is that this ordered acquisition happens before any cascade-driven write.
        if (affected.Count > 0)
        {
            await using var prelockCmd = CreateCommand(conn);
            prelockCmd.Transaction = tx;
            var sb = new StringBuilder("SET NOCOUNT ON; SET XACT_ABORT ON;\nDECLARE @lr INT;\n");
            var i = 0;
            foreach (var orch in affected)
            {
                var paramName = $"@__orch_{i}";
                sb.Append("EXEC @lr = sp_getapplock @Resource = ").Append(paramName)
                    .Append(", @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;\n")
                    .Append("IF @lr < 0 THROW 50006, 'Failed to prelock orchestrator for wake', 1;\n");
                prelockCmd.Parameters.Add(CreateParameter(paramName, "surefire_run:" + orch));
                i++;
            }

            prelockCmd.CommandText = sb.ToString();
            await prelockCmd.ExecuteNonQueryAsync(cancellationToken)
                .WithSqlCancellation(cancellationToken);
        }

        foreach (var runId in runIds)
        {
            await WakeForTerminatedRunAsync(conn, tx, runId, now, cancellationToken);
        }

        foreach (var batchId in batchIds)
        {
            await WakeForTerminatedBatchAsync(conn, tx, batchId, now, cancellationToken);
        }
    }

    private async Task WakeForTerminatedRunAsync(SqlConnection conn, SqlTransaction tx,
        string terminatedRunId, DateTimeOffset now, CancellationToken cancellationToken)
    {
        // Step 1: outgoing cleanup (no-op when the terminated run wasn't Suspended).
        await using (var deleteOutgoing = CreateCommand(conn))
        {
            deleteOutgoing.Transaction = tx;
            deleteOutgoing.CommandText = "DELETE FROM dbo.surefire_durable_waits WHERE awaiter_run_id = @id;";
            deleteOutgoing.Parameters.Add(CreateParameter("@id", terminatedRunId));
            await deleteOutgoing.ExecuteNonQueryAsync(cancellationToken)
                .WithSqlCancellation(cancellationToken);
        }

        // Step 2: collect affected orchestrators in sorted-id order, then delete the incoming
        // wait rows. Sorted ordering matches the row-lock acquisition order in step 3.
        var affected = new List<string>();
        await using (var collectCmd = CreateCommand(conn))
        {
            collectCmd.Transaction = tx;
            collectCmd.CommandText = """
                                     SELECT DISTINCT awaiter_run_id FROM dbo.surefire_durable_waits
                                     WHERE awaited_run_id = @id ORDER BY awaiter_run_id;
                                     """;
            collectCmd.Parameters.Add(CreateParameter("@id", terminatedRunId));
            await using var reader = await collectCmd.ExecuteReaderAsync(cancellationToken)
                .WithSqlCancellation(cancellationToken);
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                affected.Add(reader.GetString(0));
            }
        }

        if (affected.Count == 0)
        {
            return;
        }

        await using (var deleteIncoming = CreateCommand(conn))
        {
            deleteIncoming.Transaction = tx;
            deleteIncoming.CommandText = "DELETE FROM dbo.surefire_durable_waits WHERE awaited_run_id = @id;";
            deleteIncoming.Parameters.Add(CreateParameter("@id", terminatedRunId));
            await deleteIncoming.ExecuteNonQueryAsync(cancellationToken)
                .WithSqlCancellation(cancellationToken);
        }

        await WakeIfWaitSetEmptyAsync(conn, tx, affected, now, cancellationToken);
    }

    private async Task WakeForTerminatedBatchAsync(SqlConnection conn, SqlTransaction tx,
        string terminatedBatchId, DateTimeOffset now, CancellationToken cancellationToken)
    {
        var affected = new List<string>();
        await using (var collectCmd = CreateCommand(conn))
        {
            collectCmd.Transaction = tx;
            collectCmd.CommandText = """
                                     SELECT DISTINCT awaiter_run_id FROM dbo.surefire_durable_waits
                                     WHERE awaited_batch_id = @id ORDER BY awaiter_run_id;
                                     """;
            collectCmd.Parameters.Add(CreateParameter("@id", terminatedBatchId));
            await using var reader = await collectCmd.ExecuteReaderAsync(cancellationToken)
                .WithSqlCancellation(cancellationToken);
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                affected.Add(reader.GetString(0));
            }
        }

        if (affected.Count == 0)
        {
            return;
        }

        await using (var deleteIncoming = CreateCommand(conn))
        {
            deleteIncoming.Transaction = tx;
            deleteIncoming.CommandText = "DELETE FROM dbo.surefire_durable_waits WHERE awaited_batch_id = @id;";
            deleteIncoming.Parameters.Add(CreateParameter("@id", terminatedBatchId));
            await deleteIncoming.ExecuteNonQueryAsync(cancellationToken)
                .WithSqlCancellation(cancellationToken);
        }

        await WakeIfWaitSetEmptyAsync(conn, tx, affected, now, cancellationToken);
    }

    /// <summary>
    ///     Wakes each orchestrator whose wait set is now empty, in sorted-id order so concurrent
    ///     wakes acquire row locks deterministically. Each wake transitions Suspended -> Pending
    ///     and appends the Pending status event. The CAS on <c>status = 3</c> plus the <c>NOT EXISTS</c> on residual
    ///     waits keep wakes idempotent under sibling-race conditions.
    /// </summary>
    private async Task WakeIfWaitSetEmptyAsync(SqlConnection conn, SqlTransaction tx,
        IReadOnlyList<string> orchestratorIds, DateTimeOffset now, CancellationToken cancellationToken)
    {
        if (orchestratorIds.Count == 0)
        {
            return;
        }

        // Bulk wake under a single TVP-driven UPDATE. Per-orch applocks were already taken by
        // WakeForTerminatedEntitiesAsync's prelock in canonical sorted order, so the wake's own
        // applock chain is redundant. Job / queue UPDLOCK hints in sorted order keep this safe
        // against concurrent claim / transition writers. The wake row update is CAS-fenced on
        // status = 3 and emptiness of the wait set; both filters keep it idempotent.
        await using var wakeCmd = CreateCommand(conn);
        wakeCmd.Transaction = tx;
        wakeCmd.CommandText = """
                              SET NOCOUNT ON; SET XACT_ABORT ON;

                              DECLARE @woken TABLE (
                                  id NVARCHAR(450) NOT NULL PRIMARY KEY,
                                  attempt INT NOT NULL,
                                  job_name NVARCHAR(450) NOT NULL
                              );

                               UPDATE dbo.surefire_runs SET
                                   status = 0,
                                   not_before = @now,
                                   last_heartbeat_at = @now,
                                   replay_count = replay_count + 1
                              OUTPUT INSERTED.id, INSERTED.attempt, INSERTED.job_name
                                  INTO @woken (id, attempt, job_name)
                              WHERE id IN (SELECT name FROM @ids) AND status = 3
                                AND NOT EXISTS (
                                    SELECT 1 FROM dbo.surefire_durable_waits w WHERE w.awaiter_run_id = surefire_runs.id
                                );

                              INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                              SELECT w.id, 0, N'0', @now, w.attempt FROM @woken w;
                              """;
        wakeCmd.Parameters.Add(CreateTvpParameter("@ids", "dbo.surefire_name_list",
            NamesToTvp(orchestratorIds)));
        wakeCmd.Parameters.Add(CreateParameter("@now", now));
        await wakeCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
    }

    public async Task<RunTransitionResult> TryTransitionRunAsync(RunStatusTransition transition,
        CancellationToken cancellationToken = default)
    {
        if (!RunTransitionRules.IsAllowed(transition.ExpectedStatus, transition.NewStatus)
            || !transition.HasRequiredFields())
        {
            return RunTransitionResult.NotApplied;
        }

        var allEvents = new List<RunEvent>(transition.Events?.Count ?? 0);
        if (transition.Events is { Count: > 0 })
        {
            allEvents.AddRange(transition.Events);
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;

        cmd.CommandText = $$"""
                          SET NOCOUNT ON; SET XACT_ABORT ON;

                          -- Per-run applock serializes concurrent ops on the same run; SQL Server's
                          -- U/X conversion otherwise produces deadlocks under single-row contention.
                          DECLARE @run_lock_r INT;
                          EXEC @run_lock_r = sp_getapplock @Resource = @run_lock_key,
                              @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;
                          IF @run_lock_r < 0 THROW 50004, 'Failed to acquire run lock', 1;

                          -- Resolve the canonical lock keys via snapshot reads (RCSI). The terminal
                          -- transition decrements this run's job/queue running_count, so those rows
                          -- must be locked in the same alphabetical order as any other writer that
                          -- touches them. Wake of any Suspended orchestrator awaiting this run is
                          -- handled by WakeForTerminatedRunAsync after this statement commits its
                          -- read-set, in a sequence of per-orchestrator transactions that take
                          -- their own job/queue applocks.
                          DECLARE @run_job_name NVARCHAR(450);
                          SELECT @run_job_name = job_name FROM dbo.surefire_runs WHERE id = @id;
                          DECLARE @run_queue_name NVARCHAR(450) = N'default';
                          SELECT @run_queue_name = COALESCE(queue, N'default')
                              FROM dbo.surefire_jobs WHERE name = @run_job_name;

                          DECLARE @lock_jobs TABLE (name NVARCHAR(450) NOT NULL PRIMARY KEY);
                          DECLARE @lock_queues TABLE (name NVARCHAR(450) NOT NULL PRIMARY KEY);
                          IF @run_job_name IS NOT NULL
                          BEGIN
                              INSERT INTO @lock_jobs (name) VALUES (@run_job_name);
                              INSERT INTO @lock_queues (name) VALUES (@run_queue_name);
                          END

                          {{BuildSortedApplockSql("@lock_jobs", "surefire_job:", "ttj")}}
                          {{BuildSortedApplockSql("@lock_queues", "surefire_queue:", "ttq")}}

                          DECLARE @lock_sink NVARCHAR(450);
                          SELECT @lock_sink = j.name FROM @lock_jobs lj
                              INNER JOIN dbo.surefire_jobs j WITH (UPDLOCK, ROWLOCK) ON j.name = lj.name
                              ORDER BY lj.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);
                          SELECT @lock_sink = q.name FROM @lock_queues lq
                              INNER JOIN dbo.surefire_queues q WITH (UPDLOCK, ROWLOCK) ON q.name = lq.name
                              ORDER BY lq.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);

                           DECLARE @upd TABLE (job_name NVARCHAR(450) NOT NULL, batch_id NVARCHAR(450), attempt INT NOT NULL);

                          UPDATE dbo.surefire_runs SET
                              status = @new_status,
                              node_name = @node_name,
                              started_at = COALESCE(@started_at, started_at),
                              completed_at = COALESCE(@completed_at, completed_at),
                              canceled_at = COALESCE(@canceled_at, canceled_at),
                              reason = @reason,
                              result = @result,
                              progress = @progress,
                              not_before = @not_before,
                              last_heartbeat_at = COALESCE(@last_heartbeat_at, last_heartbeat_at),
                              lease_epoch = lease_epoch + @lease_epoch_increment,
                              attempt = attempt + @attempt_increment,
                              failure_count = failure_count + @failure_count_increment
                           OUTPUT INSERTED.job_name, INSERTED.batch_id, INSERTED.attempt
                               INTO @upd (job_name, batch_id, attempt)
                           WHERE id = @id
                               AND status = @expected_status
                               AND lease_epoch = @expected_lease_epoch
                              AND status NOT IN (2, 4, 5);

                          DECLARE @updated INT = (SELECT COUNT(*) FROM @upd);
                          DECLARE @batch_id NVARCHAR(450) = NULL;
                          DECLARE @batch_status SMALLINT = NULL;
                          DECLARE @batch_completed_at DATETIMEOFFSET(7) = NULL;

                          IF @updated > 0
                          BEGIN
                              -- Running is the only active slot holder. Decrement only when
                              -- transitioning from Running to a non-active status.
                              UPDATE dbo.surefire_jobs SET
                                  running_count = CASE WHEN @expected_status = 1 AND @new_status <> 1
                                      THEN CASE WHEN running_count > 0 THEN running_count - 1 ELSE 0 END
                                      ELSE running_count END,
                                  non_terminal_count = CASE WHEN @new_status IN (2, 4, 5)
                                      THEN CASE WHEN non_terminal_count > 0 THEN non_terminal_count - 1 ELSE 0 END
                                      ELSE non_terminal_count END
                              WHERE name = @run_job_name;

                              IF @expected_status = 1 AND @new_status <> 1
                                  UPDATE dbo.surefire_queues SET running_count =
                                      CASE WHEN running_count > 0 THEN running_count - 1 ELSE 0 END
                                  WHERE name = @run_queue_name;

                               INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                               SELECT @id, 0, CONVERT(NVARCHAR(MAX), @new_status), @now, attempt FROM @upd;

                               INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                               SELECT run_id, event_type, payload, created_at, attempt FROM @events;

                              IF @new_status IN (2, 4, 5)
                              BEGIN
                                  SET @batch_id = (SELECT TOP (1) batch_id FROM @upd WHERE batch_id IS NOT NULL);
                                  IF @batch_id IS NOT NULL
                                  BEGIN
                                      DECLARE @bc TABLE (total INT, succeeded INT, failed INT, canceled INT);
                                      UPDATE dbo.surefire_batches
                                      SET succeeded = succeeded + CASE WHEN @new_status = 2 THEN 1 ELSE 0 END,
                                          failed    = failed    + CASE WHEN @new_status = 5 THEN 1 ELSE 0 END,
                                          canceled  = canceled  + CASE WHEN @new_status = 4 THEN 1 ELSE 0 END
                                      OUTPUT INSERTED.total, INSERTED.succeeded, INSERTED.failed, INSERTED.canceled
                                          INTO @bc (total, succeeded, failed, canceled)
                                      WHERE id = @batch_id AND status NOT IN (2, 4, 5);

                                      IF EXISTS (SELECT 1 FROM @bc WHERE succeeded + failed + canceled >= total)
                                      BEGIN
                                          SET @batch_completed_at = SYSUTCDATETIME();
                                          SELECT @batch_status =
                                              CASE WHEN failed > 0 THEN 5
                                                   WHEN canceled > 0 THEN 4
                                                   ELSE 2 END
                                          FROM @bc;
                                          UPDATE dbo.surefire_batches
                                          SET status = @batch_status, completed_at = @batch_completed_at
                                          WHERE id = @batch_id AND status NOT IN (2, 4, 5);
                                      END
                                      ELSE
                                          SET @batch_id = NULL;
                                  END
                              END
                          END

                          SELECT @updated AS updated, @batch_id AS batch_id,
                                 @batch_status AS batch_status, @batch_completed_at AS completed_at;
                          """;

        cmd.Parameters.Add(CreateParameter("@id", transition.RunId));
        cmd.Parameters.Add(CreateParameter("@now", timeProvider.GetUtcNow()));
        cmd.Parameters.Add(CreateParameter("@run_lock_key", "surefire_run:" + transition.RunId));
        cmd.Parameters.Add(CreateParameter("@new_status", (int)transition.NewStatus));
        cmd.Parameters.Add(CreateParameter("@node_name", (object?)transition.NodeName ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@started_at",
            transition.StartedAt.HasValue ? transition.StartedAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@completed_at",
            transition.CompletedAt.HasValue ? transition.CompletedAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@canceled_at",
            transition.CanceledAt.HasValue ? transition.CanceledAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@reason", (object?)transition.Reason ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@result", (object?)transition.Result ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@progress", transition.Progress));
        cmd.Parameters.Add(CreateParameter("@not_before", transition.NotBefore));
        cmd.Parameters.Add(CreateParameter("@last_heartbeat_at",
            transition.LastHeartbeatAt.HasValue ? transition.LastHeartbeatAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@expected_status", (int)transition.ExpectedStatus));
        cmd.Parameters.Add(CreateParameter("@expected_lease_epoch", transition.ExpectedLeaseEpoch));
        cmd.Parameters.Add(CreateParameter("@lease_epoch_increment", transition.IncrementLeaseEpoch ? 1 : 0));
        cmd.Parameters.Add(CreateParameter("@attempt_increment", transition.IncrementAttempt ? 1 : 0));
        cmd.Parameters.Add(CreateParameter("@failure_count_increment", transition.IncrementFailureCount ? 1 : 0));
        cmd.Parameters.Add(CreateTvpParameter("@events", "dbo.surefire_events_input", EventsToTvp(allEvents)));

        BatchCompletionInfo? batchCompletion = null;
        var transitioned = false;
        await using (var reader =
                     await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                transitioned = reader.GetInt32(0) > 0;
                if (!reader.IsDBNull(1))
                {
                    var bId = reader.GetString(1);
                    var bStatus = (JobStatus)reader.GetInt16(2);
                    var bAt = reader.GetDateTimeOffset(3);
                    batchCompletion = new(bId, bStatus, bAt);
                }
            }
        }

        // Unified wake: clear outgoing waits, delete incoming waits, wake any orchestrator
        // whose wait set is now empty. Locks all affected orchestrators in one canonical
        // sorted order so the run and batch cascades cannot deadlock on overlapping sets.
        if (transitioned && transition.NewStatus.IsTerminal)
        {
            var batchIds = batchCompletion is { } bc ? new[] { bc.BatchId } : Array.Empty<string>();
            await WakeForTerminatedEntitiesAsync(conn, tx, [transition.RunId], batchIds,
                timeProvider.GetUtcNow(), cancellationToken);
        }

        await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        return new(transitioned, batchCompletion);
    }

    public async Task<RunTransitionResult> TryCancelRunAsync(string runId,
        long? expectedLeaseEpoch = null,
        string? reason = null,
        IReadOnlyList<RunEvent>? events = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        // The auto-emitted status event needs the pre-cancel attempt, which we capture from the
        // OUTPUT clause of the CAS UPDATE.
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = $$"""
                          SET NOCOUNT ON; SET XACT_ABORT ON;

                          -- See TryTransitionRunAsync for the per-run applock + sorted-applock
                          -- pattern. Wake of any Suspended orchestrator awaiting this run is
                          -- performed by WakeForTerminatedRunAsync after this statement, in a
                          -- sequence of per-orchestrator transactions that take their own
                          -- job/queue applocks.
                          DECLARE @run_lock_r INT;
                          EXEC @run_lock_r = sp_getapplock @Resource = @run_lock_key,
                              @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;
                          IF @run_lock_r < 0 THROW 50004, 'Failed to acquire run lock', 1;

                          DECLARE @run_job_name NVARCHAR(450);
                          SELECT @run_job_name = job_name FROM dbo.surefire_runs WHERE id = @id;
                          DECLARE @run_queue_name NVARCHAR(450) = N'default';
                          SELECT @run_queue_name = COALESCE(queue, N'default')
                              FROM dbo.surefire_jobs WHERE name = @run_job_name;

                          DECLARE @lock_jobs TABLE (name NVARCHAR(450) NOT NULL PRIMARY KEY);
                          DECLARE @lock_queues TABLE (name NVARCHAR(450) NOT NULL PRIMARY KEY);
                          IF @run_job_name IS NOT NULL
                          BEGIN
                              INSERT INTO @lock_jobs (name) VALUES (@run_job_name);
                              INSERT INTO @lock_queues (name) VALUES (@run_queue_name);
                          END

                          {{BuildSortedApplockSql("@lock_jobs", "surefire_job:", "ctj")}}
                          {{BuildSortedApplockSql("@lock_queues", "surefire_queue:", "ctq")}}

                          DECLARE @lock_sink NVARCHAR(450);
                          SELECT @lock_sink = j.name FROM @lock_jobs lj
                              INNER JOIN dbo.surefire_jobs j WITH (UPDLOCK, ROWLOCK) ON j.name = lj.name
                              ORDER BY lj.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);
                          SELECT @lock_sink = q.name FROM @lock_queues lq
                              INNER JOIN dbo.surefire_queues q WITH (UPDLOCK, ROWLOCK) ON q.name = lq.name
                              ORDER BY lq.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);

                          DECLARE @now DATETIMEOFFSET(7) = SYSUTCDATETIME();
                          DECLARE @upd TABLE (
                              id NVARCHAR(450) NOT NULL,
                              attempt INT NOT NULL,
                              batch_id NVARCHAR(450),
                              prior_status INT NOT NULL,
                              job_name NVARCHAR(450) NOT NULL
                          );

                          UPDATE dbo.surefire_runs SET
                              status = 4,
                              canceled_at = @now,
                              completed_at = @now,
                              reason = COALESCE(@reason, reason)
                          OUTPUT INSERTED.id, INSERTED.attempt, INSERTED.batch_id, DELETED.status,
                                 INSERTED.job_name
                              INTO @upd (id, attempt, batch_id, prior_status, job_name)
                          WHERE id = @id
                              AND status NOT IN (2, 4, 5)
                              AND (@expected_lease_epoch IS NULL OR lease_epoch = @expected_lease_epoch);

                          DECLARE @transitioned INT = (SELECT COUNT(*) FROM @upd);
                          DECLARE @attempt INT = (SELECT TOP (1) attempt FROM @upd);
                          DECLARE @prior_status INT = (SELECT TOP (1) prior_status FROM @upd);
                          DECLARE @batch_id NVARCHAR(450) = NULL;
                          DECLARE @batch_status SMALLINT = NULL;
                          DECLARE @batch_completed_at DATETIMEOFFSET(7) = NULL;

                          IF @transitioned > 0
                          BEGIN
                              -- Running (1) is the only active slot holder.
                              UPDATE dbo.surefire_jobs SET
                                  running_count = CASE WHEN @prior_status = 1
                                      THEN CASE WHEN running_count > 0 THEN running_count - 1 ELSE 0 END
                                      ELSE running_count END,
                                  non_terminal_count =
                                      CASE WHEN non_terminal_count > 0 THEN non_terminal_count - 1 ELSE 0 END
                              WHERE name = @run_job_name;

                              IF @prior_status = 1
                                  UPDATE dbo.surefire_queues SET running_count =
                                      CASE WHEN running_count > 0 THEN running_count - 1 ELSE 0 END
                                  WHERE name = @run_queue_name;

                              -- Auto-emitted status event derived from @upd (carries the actual attempt).
                              INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                              SELECT u.id, 0, CONVERT(NVARCHAR(10), 4), @now, u.attempt FROM @upd u;

                              -- Caller-provided extras (may be empty).
                              INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                              SELECT run_id, event_type, payload, created_at, attempt FROM @events;

                              SET @batch_id = (SELECT TOP (1) batch_id FROM @upd WHERE batch_id IS NOT NULL);
                              IF @batch_id IS NOT NULL
                              BEGIN
                                  DECLARE @bc TABLE (total INT, succeeded INT, failed INT, canceled INT);
                                  UPDATE dbo.surefire_batches
                                  SET canceled = canceled + 1
                                  OUTPUT INSERTED.total, INSERTED.succeeded, INSERTED.failed, INSERTED.canceled
                                      INTO @bc (total, succeeded, failed, canceled)
                                  WHERE id = @batch_id AND status NOT IN (2, 4, 5);

                                  IF EXISTS (SELECT 1 FROM @bc WHERE succeeded + failed + canceled >= total)
                                  BEGIN
                                      SET @batch_completed_at = SYSUTCDATETIME();
                                      SELECT @batch_status =
                                          CASE WHEN failed > 0 THEN 5
                                               WHEN canceled > 0 THEN 4
                                               ELSE 2 END
                                      FROM @bc;
                                      UPDATE dbo.surefire_batches
                                      SET status = @batch_status, completed_at = @batch_completed_at
                                      WHERE id = @batch_id AND status NOT IN (2, 4, 5);
                                  END
                                  ELSE
                                      SET @batch_id = NULL;
                              END
                          END

                          SELECT @transitioned AS transitioned, @attempt AS attempt,
                                 @batch_id AS batch_id, @batch_status AS batch_status,
                                 @batch_completed_at AS completed_at;
                          """;

        cmd.Parameters.Add(CreateParameter("@id", runId));
        cmd.Parameters.Add(CreateParameter("@run_lock_key", "surefire_run:" + runId));
        cmd.Parameters.Add(CreateParameter("@reason", (object?)reason ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@expected_lease_epoch",
            expectedLeaseEpoch.HasValue ? expectedLeaseEpoch.Value : DBNull.Value));
        cmd.Parameters.Add(CreateTvpParameter("@events", "dbo.surefire_events_input",
            EventsToTvp(events ?? Array.Empty<RunEvent>())));

        var transitioned = false;
        BatchCompletionInfo? batchCompletion = null;
        await using (var reader =
                     await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                transitioned = reader.GetInt32(0) > 0;
                if (!reader.IsDBNull(2))
                {
                    var bId = reader.GetString(2);
                    var bStatus = (JobStatus)reader.GetInt16(3);
                    var bAt = reader.GetDateTimeOffset(4);
                    batchCompletion = new(bId, bStatus, bAt);
                }
            }
        }

        // Unified wake: clear outgoing waits (matters if the canceled run was Suspended), delete
        // incoming waits, wake any orchestrator whose wait set is now empty. Both cascades share
        // one canonical sorted applock acquisition pass.
        if (transitioned)
        {
            var batchIds = batchCompletion is { } bc ? new[] { bc.BatchId } : Array.Empty<string>();
            await WakeForTerminatedEntitiesAsync(conn, tx, [runId], batchIds,
                timeProvider.GetUtcNow(), cancellationToken);
        }

        await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);

        return transitioned ? new(true, batchCompletion) : RunTransitionResult.NotApplied;
    }

    public async Task<bool> TryCompleteBatchAsync(string batchId, JobStatus status, DateTimeOffset completedAt,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        bool transitioned;
        await using (var cmd = CreateCommand(conn))
        {
            cmd.Transaction = tx;
            // Take a batch-scoped applock before the UPDATE so concurrent batch terminals on
            // the same id serialize cleanly; without this, two writers racing on the same batch
            // can interleave with the cascading WakeForTerminatedEntities pass and deadlock on
            // overlapping orchestrator applock sets.
            cmd.CommandText = """
                              SET NOCOUNT ON; SET XACT_ABORT ON;

                              DECLARE @batch_lock_r INT;
                              EXEC @batch_lock_r = sp_getapplock @Resource = @batch_lock_key,
                                  @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;
                              IF @batch_lock_r < 0 THROW 50007, 'Failed to acquire batch lock', 1;

                              UPDATE dbo.surefire_batches
                              SET status = @status, completed_at = @completed_at
                              OUTPUT INSERTED.id
                              WHERE id = @id AND status NOT IN (2, 4, 5);
                              """;
            cmd.Parameters.Add(CreateParameter("@id", batchId));
            cmd.Parameters.Add(CreateParameter("@batch_lock_key", "surefire_batch:" + batchId));
            cmd.Parameters.Add(CreateParameter("@status", (short)status));
            cmd.Parameters.Add(CreateParameter("@completed_at", completedAt));

            await using var reader =
                await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            transitioned = await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        }

        if (transitioned)
        {
            // Unified wake propagates the batch terminal to any orchestrator awaiting it.
            await WakeForTerminatedEntitiesAsync(conn, tx, Array.Empty<string>(),
                [batchId], completedAt, cancellationToken);
        }

        await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        return transitioned;
    }

    public Task CreateRunsAsync(IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
        => CreateRunsAsyncCore(runs, initialEvents, cancellationToken);

    public Task<bool> TryCreateRunAsync(JobRun run, int? maxActiveForJob = null,
        DateTimeOffset? lastCronFireAt = null,
        IReadOnlyList<RunEvent>? initialEvents = null,
        DurableStepRecord? durableStepRecord = null,
        CancellationToken cancellationToken = default)
        => TryCreateRunAsyncCore(run, maxActiveForJob, lastCronFireAt, initialEvents, durableStepRecord,
            cancellationToken);

    public async Task CreateBatchAsync(JobBatch batch, IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        DurableStepRecord? durableStepRecord = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        try
        {
            await using var batchCmd = CreateCommand(conn);
            batchCmd.Transaction = tx;
            batchCmd.CommandText = """
                                   SET NOCOUNT ON; SET XACT_ABORT ON;
                                   INSERT INTO dbo.surefire_batches (id, status, total, succeeded, failed, canceled, created_at, completed_at, parent_run_id)
                                   VALUES (@id, @status, @total, @succeeded, @failed, @canceled, @created_at, @completed_at, @parent_run_id);
                                   """;
            batchCmd.Parameters.Add(CreateParameter("@id", batch.Id));
            batchCmd.Parameters.Add(CreateParameter("@status", (short)batch.Status));
            batchCmd.Parameters.Add(CreateParameter("@total", batch.Total));
            batchCmd.Parameters.Add(CreateParameter("@succeeded", batch.Succeeded));
            batchCmd.Parameters.Add(CreateParameter("@failed", batch.Failed));
            batchCmd.Parameters.Add(CreateParameter("@canceled", batch.Canceled));
            batchCmd.Parameters.Add(CreateParameter("@created_at", batch.CreatedAt));
            batchCmd.Parameters.Add(CreateParameter("@completed_at",
                batch.CompletedAt.HasValue ? batch.CompletedAt.Value : DBNull.Value));
            batchCmd.Parameters.Add(CreateParameter("@parent_run_id", (object?)batch.ParentRunId ?? DBNull.Value));
            await batchCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);

            await CreateRunsCoreInTransactionAsync(conn, tx, runs, initialEvents, cancellationToken);

            if (durableStepRecord is { } step)
            {
                await using var stepCmd = CreateCommand(conn);
                stepCmd.Transaction = tx;
                stepCmd.CommandText = """
                                      UPDATE dbo.surefire_runs
                                      SET highest_recorded_step = CASE
                                              WHEN highest_recorded_step > @step THEN highest_recorded_step
                                              ELSE @step
                                          END
                                      WHERE id = @orch_id;
                                      """;
                stepCmd.Parameters.Add(CreateParameter("@step", step.Step));
                stepCmd.Parameters.Add(CreateParameter("@orch_id", step.OrchestratorRunId));
                await stepCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            }

            await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        }
        catch (SqlException ex) when (ex.Number is 2601 or 2627)
        {
            throw new RunConflictException(batch.Id,
                $"Batch '{batch.Id}' or one of its runs already exists.", ex);
        }
    }

    public async Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM dbo.surefire_batches WHERE id = @id";
        cmd.Parameters.Add(CreateParameter("@id", batchId));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            return null;
        }

        return ReadBatch(reader);
    }

    public async Task<IReadOnlyList<string>> GetCompletableBatchIdsAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
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
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            result.Add(reader.GetString(0));
        }

        return result;
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

    public Task<SubtreeCancellation> CancelRunSubtreeAsync(string rootRunId,
        string? reason = null,
        bool includeRoot = true,
        CancellationToken cancellationToken = default)
        => CancelSubtreeAsyncCore(SubtreeSeed.Run, rootRunId, reason, includeRoot, cancellationToken);

    public Task<SubtreeCancellation> CancelBatchSubtreeAsync(string batchId,
        string? reason = null,
        CancellationToken cancellationToken = default)
        => CancelSubtreeAsyncCore(SubtreeSeed.Batch, batchId, reason, true, cancellationToken);

    public async Task<SubtreeCancellation> CancelExpiredRunsWithIdsAsync(
        CancellationToken cancellationToken = default)
    {
        var canceledRuns = new List<CanceledRun>();
        var expiredRuns = new List<ExpiredCanceledRun>();
        var completedBatches = new List<BatchCompletionInfo>();
        var seeds = await GetRootmostExpiredRunIdsAsync(cancellationToken);
        foreach (var seed in seeds)
        {
            var result = await CancelSubtreeAsyncCore(
                SubtreeSeed.Run,
                seed,
                "Run expired past its deadline.",
                includeRoot: true,
                cancellationToken,
                expirationCancellation: true);
            canceledRuns.AddRange(result.Runs);
            expiredRuns.AddRange(result.ExpiredRuns);
            completedBatches.AddRange(result.CompletedBatches);
        }

        return canceledRuns.Count == 0 && completedBatches.Count == 0
            ? SubtreeCancellation.Empty
            : new SubtreeCancellation(canceledRuns, completedBatches) { ExpiredRuns = expiredRuns };
    }

    private async Task<IReadOnlyList<string>> GetRootmostExpiredRunIdsAsync(CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          ;WITH expired AS (
                              SELECT id
                              FROM dbo.surefire_runs
                              WHERE status NOT IN (2, 4, 5)
                                  AND ((status = 0 AND lease_epoch = 0 AND not_after IS NOT NULL AND not_after < SYSUTCDATETIME())
                                    OR (expires_at IS NOT NULL AND expires_at < SYSUTCDATETIME()))
                          ),
                          ancestors AS (
                              SELECT e.id AS candidate_id, r.parent_run_id AS parent_id
                              FROM expired e
                              INNER JOIN dbo.surefire_runs r ON r.id = e.id
                              UNION ALL
                              SELECT a.candidate_id, p.parent_run_id
                              FROM ancestors a
                              INNER JOIN dbo.surefire_runs p ON p.id = a.parent_id
                              WHERE a.parent_id IS NOT NULL
                          )
                          SELECT e.id
                          FROM expired e
                          WHERE NOT EXISTS (
                              SELECT 1
                              FROM ancestors a
                              INNER JOIN expired ancestor ON ancestor.id = a.parent_id
                              WHERE a.candidate_id = e.id
                          )
                          ORDER BY e.id
                          OPTION (MAXRECURSION 0)
                          """;

        var roots = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
            .WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            roots.Add(reader.GetString(0));
        }

        return roots;
    }

    public async Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default)
    {
        if (events.Count == 0)
        {
            return;
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SET NOCOUNT ON; SET XACT_ABORT ON;
                          INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                          SELECT run_id, event_type, payload, created_at, attempt FROM @events;
                          """;
        cmd.Parameters.Add(CreateTvpParameter("@events", "dbo.surefire_events_input", EventsToTvp(events)));
        await cmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
    }

    public async Task<IReadOnlySet<string>> AppendEventsIfRunNonTerminalAsync(
        IReadOnlyList<RunEvent> events,
        CancellationToken cancellationToken = default)
    {
        if (events.Count == 0)
        {
            return new HashSet<string>(StringComparer.Ordinal);
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = """
                          SET NOCOUNT ON; SET XACT_ABORT ON;
                          DECLARE @accepted TABLE (run_id nvarchar(200) PRIMARY KEY);

                          INSERT INTO @accepted (run_id)
                          SELECT DISTINCT e.run_id
                          FROM @events e
                          INNER JOIN dbo.surefire_runs r WITH (UPDLOCK, HOLDLOCK) ON r.id = e.run_id
                          WHERE r.status NOT IN (2, 4, 5);

                          INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                          SELECT e.run_id, e.event_type, e.payload, e.created_at, e.attempt
                          FROM @events e
                          INNER JOIN @accepted a ON a.run_id = e.run_id;

                          SELECT run_id FROM @accepted ORDER BY run_id;
                          """;
        cmd.Parameters.Add(CreateTvpParameter("@events", "dbo.surefire_events_input", EventsToTvp(events)));

        var accepted = new HashSet<string>(StringComparer.Ordinal);
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                accepted.Add(reader.GetString(0));
            }
        }

        await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        return accepted;
    }

    public async Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0,
        RunEventType[]? types = null, int? attempt = null, int? take = null,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);

        // The MIN_ACTIVE_ROWVERSION clamp prevents skipping in-flight low-id rows for non-terminal
        // runs. It must NOT apply to terminal runs: with no further writers, an unrelated long TX
        // holding the rowversion back would cause the reader to drain zero events and miss rows.
        var sb = new StringBuilder("""
                                   SELECT e.* FROM dbo.surefire_events e
                                   INNER JOIN dbo.surefire_runs r ON r.id = e.run_id
                                   WHERE e.run_id = @run_id
                                     AND e.id > @since_id
                                     AND (
                                         r.status IN (2, 4, 5)
                                         OR e.id < ISNULL(
                                             (SELECT MIN(id) FROM dbo.surefire_events
                                              WHERE commit_seq >= MIN_ACTIVE_ROWVERSION()),
                                             9223372036854775807)
                                     )
                                   """);
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
            sb.Append($" AND e.event_type IN ({string.Join(",", typeParams)})");
        }

        if (attempt is { })
        {
            sb.Append(" AND (e.attempt = @attempt OR e.attempt = 0)");
            cmd.Parameters.Add(CreateParameter("@attempt", attempt.Value));
        }

        sb.Append(" ORDER BY e.id");

        if (take is { })
        {
            sb.Append(" OFFSET 0 ROWS FETCH NEXT @take ROWS ONLY");
            cmd.Parameters.Add(CreateParameter("@take", take.Value));
        }

        cmd.CommandText = sb.ToString();

        var results = new List<RunEvent>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
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
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SELECT TOP (@take) e.*
                          FROM dbo.surefire_events e
                          INNER JOIN dbo.surefire_runs r ON r.id = e.run_id
                          INNER JOIN dbo.surefire_batches b ON b.id = r.batch_id
                          WHERE r.batch_id = @batch_id
                              AND e.id > @since_event_id
                              AND (
                                  b.status IN (2, 4, 5)
                                  OR e.id < ISNULL(
                                      (SELECT MIN(id) FROM dbo.surefire_events
                                       WHERE commit_seq >= MIN_ACTIVE_ROWVERSION()),
                                      9223372036854775807)
                              )
                          ORDER BY e.id ASC
                          """;
        cmd.Parameters.Add(CreateParameter("@batch_id", batchId));
        cmd.Parameters.Add(CreateParameter("@since_event_id", sinceEventId));
        cmd.Parameters.Add(CreateParameter("@take", take));

        var results = new List<RunEvent>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
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
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SELECT TOP (@take) e.*
                          FROM dbo.surefire_events e
                          INNER JOIN dbo.surefire_runs r ON r.id = e.run_id
                          INNER JOIN dbo.surefire_batches b ON b.id = r.batch_id
                          WHERE r.batch_id = @batch_id
                              AND e.event_type = @event_type
                              AND e.id > @since_event_id
                              AND (
                                  b.status IN (2, 4, 5)
                                  OR e.id < ISNULL(
                                      (SELECT MIN(id) FROM dbo.surefire_events
                                       WHERE commit_seq >= MIN_ACTIVE_ROWVERSION()),
                                      9223372036854775807)
                              )
                          ORDER BY e.id ASC
                          """;
        cmd.Parameters.Add(CreateParameter("@batch_id", batchId));
        cmd.Parameters.Add(CreateParameter("@event_type", (short)RunEventType.Output));
        cmd.Parameters.Add(CreateParameter("@since_event_id", sinceEventId));
        cmd.Parameters.Add(CreateParameter("@take", take));

        var results = new List<RunEvent>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
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
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        await using (var nodeCmd = CreateCommand(conn))
        {
            nodeCmd.Transaction = tx;
            nodeCmd.CommandText = """
                                  SET NOCOUNT ON; SET XACT_ABORT ON;
                                  UPDATE dbo.surefire_nodes
                                  SET last_heartbeat_at = SYSUTCDATETIME(),
                                      running_count = @running_count,
                                      registered_job_names = @job_names,
                                      registered_queue_names = @queue_names
                                  WHERE name = @name;
                                  IF @@ROWCOUNT = 0
                                      INSERT INTO dbo.surefire_nodes (
                                          name, started_at, last_heartbeat_at, running_count,
                                          registered_job_names, registered_queue_names)
                                      VALUES (@name, SYSUTCDATETIME(), SYSUTCDATETIME(), @running_count,
                                          @job_names, @queue_names);
                                  """;
            nodeCmd.Parameters.Add(CreateParameter("@name", nodeName));
            nodeCmd.Parameters.Add(CreateParameter("@running_count", activeRunIds.Count));
            nodeCmd.Parameters.Add(CreateParameter("@job_names",
                JsonSerializer.Serialize(jobNames.ToArray(), SurefireJsonContext.Default.StringArray)));
            nodeCmd.Parameters.Add(CreateParameter("@queue_names",
                JsonSerializer.Serialize(queueNames.ToArray(), SurefireJsonContext.Default.StringArray)));

            try
            {
                await nodeCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            }
            catch (SqlException ex) when (ex.Number is 2601 or 2627)
            {
                // A concurrent writer for the same node name won the insert race; the row exists.
            }
        }

        if (activeRunIds.Count > 0)
        {
            await using var runCmd = CreateCommand(conn);
            runCmd.Transaction = tx;
            runCmd.CommandText = """
                                 SET NOCOUNT ON; SET XACT_ABORT ON;
                                 UPDATE r SET last_heartbeat_at = SYSUTCDATETIME()
                                 FROM dbo.surefire_runs r
                                 INNER JOIN @ids i ON i.name = r.id
                                 WHERE r.node_name = @node AND r.status NOT IN (2, 4, 5);
                                 """;
            runCmd.Parameters.Add(CreateParameter("@node", nodeName));
            runCmd.Parameters.Add(CreateTvpParameter("@ids", "dbo.surefire_name_list", NamesToTvp(activeRunIds)));
            await runCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        }

        await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> GetExternallyStoppedRunIdsAsync(IReadOnlyCollection<string> runIds,
        CancellationToken cancellationToken = default)
    {
        if (runIds.Count == 0)
        {
            return [];
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SELECT i.name
                          FROM @ids i
                          LEFT JOIN dbo.surefire_runs r ON r.id = i.name
                          WHERE r.id IS NULL OR r.status <> 1
                          """;
        cmd.Parameters.Add(CreateTvpParameter("@ids", "dbo.surefire_name_list", NamesToTvp(runIds)));

        var results = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            results.Add(reader.GetString(0));
        }

        return results;
    }

    public async Task<IReadOnlyList<string>> GetStaleRunningRunIdsAsync(DateTimeOffset staleBefore, int take,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(take, 1);

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SELECT TOP (@take) id
                          FROM dbo.surefire_runs
                          WHERE status = 1 AND last_heartbeat_at < @stale_before
                          ORDER BY last_heartbeat_at ASC, id ASC
                          """;
        cmd.Parameters.Add(CreateParameter("@take", take));
        cmd.Parameters.Add(CreateParameter("@stale_before", staleBefore));

        var ids = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            ids.Add(reader.GetString(0));
        }

        return ids;
    }

    public async Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT * FROM dbo.surefire_nodes";

        var results = new List<NodeInfo>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            results.Add(ReadNode(reader));
        }

        return results;
    }

    public async Task<NodeInfo?> GetNodeAsync(string name, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = "SELECT TOP (1) * FROM dbo.surefire_nodes WHERE name = @name";
        cmd.Parameters.Add(CreateParameter("@name", name));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        if (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            return ReadNode(reader);
        }

        return null;
    }

    public async Task PurgeAsync(DateTimeOffset threshold, CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);

        while (true)
        {
            await using var tx = (SqlTransaction)await conn
                .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
                .WithSqlCancellation(cancellationToken);

            int deleted;
            await using (var cmd = CreateCommand(conn))
            {
                cmd.Transaction = tx;
                cmd.CommandText = """
                                    SET NOCOUNT ON; SET XACT_ABORT ON;

                                    DECLARE @candidate TABLE (
                                        id NVARCHAR(450) NOT NULL PRIMARY KEY
                                    );

                                    INSERT INTO @candidate (id)
                                    SELECT TOP (1000) r.id
                                    FROM dbo.surefire_runs r
                                    WHERE r.status IN (2, 4, 5)
                                        AND r.completed_at < @threshold
                                        AND NOT EXISTS (
                                            SELECT 1
                                            FROM dbo.surefire_runs open_run
                                            WHERE ISNULL(open_run.root_run_id, open_run.id) = ISNULL(r.root_run_id, r.id)
                                                AND open_run.status NOT IN (2, 4, 5)
                                        )
                                        AND (r.batch_id IS NULL OR EXISTS (
                                            SELECT 1 FROM dbo.surefire_batches b
                                            WHERE b.id = r.batch_id
                                                AND b.status IN (2, 4, 5)
                                                AND b.completed_at IS NOT NULL
                                                AND b.completed_at < @threshold
                                        ))
                                    ORDER BY r.id;

                                    IF NOT EXISTS (SELECT 1 FROM @candidate)
                                    BEGIN
                                        SELECT 0 AS deleted;
                                        RETURN;
                                    END

                                    DELETE r FROM dbo.surefire_runs r
                                    INNER JOIN @candidate c ON c.id = r.id
                                    WHERE r.status IN (2, 4, 5)
                                        AND r.completed_at < @threshold
                                        AND NOT EXISTS (
                                            SELECT 1
                                            FROM dbo.surefire_runs open_run
                                            WHERE ISNULL(open_run.root_run_id, open_run.id) = ISNULL(r.root_run_id, r.id)
                                                AND open_run.status NOT IN (2, 4, 5)
                                        )
                                        AND (r.batch_id IS NULL OR EXISTS (
                                            SELECT 1 FROM dbo.surefire_batches b
                                            WHERE b.id = r.batch_id
                                                AND b.status IN (2, 4, 5)
                                                AND b.completed_at IS NOT NULL
                                                AND b.completed_at < @threshold
                                        ));

                                    DECLARE @deleted INT = @@ROWCOUNT;
                                    SELECT @deleted AS deleted;
                                    """;
                cmd.Parameters.Add(CreateParameter("@threshold", threshold));

                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                    .WithSqlCancellation(cancellationToken);
                deleted = 0;
                if (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
                {
                    deleted = reader.GetInt32(0);
                }

            }

            await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);

            if (deleted == 0)
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
        await jobCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);

        await using var queueCmd = CreateCommand(conn);
        queueCmd.CommandText = "DELETE FROM dbo.surefire_queues WHERE last_heartbeat_at < @threshold";
        queueCmd.Parameters.Add(CreateParameter("@threshold", threshold));
        await queueCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);

        await using var rlCmd = CreateCommand(conn);
        rlCmd.CommandText = "DELETE FROM dbo.surefire_rate_limits WHERE last_heartbeat_at < @threshold";
        rlCmd.Parameters.Add(CreateParameter("@threshold", threshold));
        await rlCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);

        await using var batchCmd = CreateCommand(conn);
        batchCmd.CommandText = """
                               DELETE FROM dbo.surefire_batches
                               WHERE status IN (2, 4, 5) AND completed_at IS NOT NULL AND completed_at < @threshold
                                   AND NOT EXISTS (SELECT 1 FROM dbo.surefire_runs r WHERE r.batch_id = dbo.surefire_batches.id)
                               """;
        batchCmd.Parameters.Add(CreateParameter("@threshold", threshold));
        await batchCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);

        await using var nodeCmd = CreateCommand(conn);
        nodeCmd.CommandText = "DELETE FROM dbo.surefire_nodes WHERE last_heartbeat_at < @threshold";
        nodeCmd.Parameters.Add(CreateParameter("@threshold", threshold));
        await nodeCmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
    }

    public async Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60,
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);

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
                                   ISNULL(SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END), 0) AS suspended,
                                   ISNULL(SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END), 0) AS succeeded,
                                   ISNULL(SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END), 0) AS canceled,
                                   ISNULL(SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END), 0) AS failed
                               FROM dbo.surefire_runs
                               WHERE created_at >= @since AND created_at <= @now
                               """;
        statsCmd.Parameters.Add(CreateParameter("@now", now));
        statsCmd.Parameters.Add(CreateParameter("@since", sinceTime));

        int totalJobs = 0, totalRuns = 0, activeRuns = 0, nodeCount = 0;
        var runsByStatus = new Dictionary<string, int>();
        double successRate = 0;
        await using (var reader =
                     await statsCmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                totalJobs = reader.GetInt32(reader.GetOrdinal("total_jobs"));
                nodeCount = reader.GetInt32(reader.GetOrdinal("node_count"));
                totalRuns = reader.GetInt32(reader.GetOrdinal("total_runs"));

                var pending = reader.GetInt32(reader.GetOrdinal("pending"));
                var running = reader.GetInt32(reader.GetOrdinal("running"));
                var suspended = reader.GetInt32(reader.GetOrdinal("suspended"));
                var succeeded = reader.GetInt32(reader.GetOrdinal("succeeded"));
                var canceled = reader.GetInt32(reader.GetOrdinal("canceled"));
                var failed = reader.GetInt32(reader.GetOrdinal("failed"));

                // Active dashboard count tracks runnable/executing work. Suspended orchestrators
                // are durable waits and do not consume execution slots.
                activeRuns = pending + running;

                if (pending > 0)
                {
                    runsByStatus["Pending"] = pending;
                }

                if (running > 0)
                {
                    runsByStatus["Running"] = running;
                }

                if (suspended > 0)
                {
                    runsByStatus["Suspended"] = suspended;
                }

                if (succeeded > 0)
                {
                    runsByStatus["Succeeded"] = succeeded;
                }

                if (canceled > 0)
                {
                    runsByStatus["Canceled"] = canceled;
                }

                if (failed > 0)
                {
                    runsByStatus["Failed"] = failed;
                }

                var terminalCount = succeeded + canceled + failed;
                successRate = terminalCount > 0 ? succeeded / (double)terminalCount : 0.0;
            }
        }

        await using var bucketCmd = CreateCommand(conn);
        bucketCmd.CommandText = """
                                SELECT
                                    DATEADD(MINUTE, (DATEDIFF(MINUTE, @since, created_at) / @bucket_minutes) * @bucket_minutes, @since) AS bucket_start,
                                    SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS pending,
                                    SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS running,
                                    SUM(CASE WHEN status = 3 THEN 1 ELSE 0 END) AS suspended,
                                    SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS succeeded,
                                    SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END) AS canceled,
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
        await using (var reader =
                     await bucketCmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                var start = reader.GetDateTimeOffset(0);
                bucketMap[start] = new()
                {
                    Start = start,
                    Pending = reader.GetInt32(1),
                    Running = reader.GetInt32(2),
                    Suspended = reader.GetInt32(3),
                    Succeeded = reader.GetInt32(4),
                    Canceled = reader.GetInt32(5),
                    Failed = reader.GetInt32(6)
                };
            }
        }

        var buckets = new List<TimelineBucket>();
        var bucketStart = sinceTime;
        var bucketSpan = TimeSpan.FromMinutes(bucketMinutes);
        while (bucketStart <= now)
        {
            buckets.Add(bucketMap.TryGetValue(bucketStart, out var bucket)
                ? bucket
                : new() { Start = bucketStart });
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
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          SELECT
                              COUNT(*) AS total_runs,
                              ISNULL(SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END), 0) AS succeeded,
                              ISNULL(SUM(CASE WHEN status = 5 THEN 1 ELSE 0 END), 0) AS failed,
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

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken);

        return new()
        {
            TotalRuns = reader.GetInt32(0),
            SucceededRuns = reader.GetInt32(1),
            FailedRuns = reader.GetInt32(2),
            SuccessRate = reader.GetDouble(3),
            AvgDuration = !reader.IsDBNull(4) ? TimeSpan.FromSeconds(reader.GetDouble(4)) : null,
            LastRunAt = !reader.IsDBNull(5) ? reader.GetDateTimeOffset(5) : null
        };
    }

    public async Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(
        CancellationToken cancellationToken = default)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var cmd = CreateCommand(conn);
        cmd.CommandText = """
                          WITH queue_names AS (
                              SELECT name FROM dbo.surefire_queues
                              UNION
                              SELECT ISNULL(j.queue, N'default') AS name
                              FROM dbo.surefire_runs r
                              JOIN dbo.surefire_jobs j ON j.name = r.job_name
                              WHERE r.status = 0
                              UNION
                              SELECT ISNULL(j.queue, N'default') AS name
                              FROM dbo.surefire_runs r
                              JOIN dbo.surefire_jobs j ON j.name = r.job_name
                              WHERE r.status = 1
                          ),
                          pending AS (
                              SELECT ISNULL(j.queue, N'default') AS queue_name, COUNT(*) AS cnt
                              FROM dbo.surefire_runs r
                              JOIN dbo.surefire_jobs j ON j.name = r.job_name
                              WHERE r.status = 0
                              GROUP BY ISNULL(j.queue, N'default')
                          ),
                          running AS (
                              SELECT ISNULL(j.queue, N'default') AS queue_name, COUNT(*) AS cnt
                              FROM dbo.surefire_runs r
                              JOIN dbo.surefire_jobs j ON j.name = r.job_name
                              WHERE r.status = 1
                              GROUP BY ISNULL(j.queue, N'default')
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
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            results[reader.GetString(0)] = new()
            {
                PendingCount = reader.GetInt32(1),
                RunningCount = reader.GetInt32(2)
            };
        }

        return results;
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

    private static SqlParameter CreateTvpParameter(string name, string typeName, object? value)
    {
        // SqlClient encodes empty TVPs as a null Value; both empty enumerations and DBNull throw.
        var normalized = value is ICollection { Count: 0 } ? null : value;
        return new()
        {
            ParameterName = name,
            SqlDbType = SqlDbType.Structured,
            TypeName = typeName,
            Value = normalized
        };
    }

    private static string EscapeLike(string input) =>
        input.Replace("[", "[[]").Replace("%", "[%]").Replace("_", "[_]");

    private static List<SqlDataRecord> EventsToTvp(IEnumerable<RunEvent> events)
    {
        var list = new List<SqlDataRecord>();
        foreach (var e in events)
        {
            var rec = new SqlDataRecord(EventsTvpMeta);
            rec.SetString(0, e.RunId);
            rec.SetInt16(1, (short)e.EventType);
            rec.SetString(2, e.Payload);
            rec.SetDateTimeOffset(3, e.CreatedAt);
            rec.SetInt32(4, e.Attempt);
            list.Add(rec);
        }

        return list;
    }

    private static List<SqlDataRecord> RunsToTvp(IEnumerable<JobRun> runs)
    {
        var list = new List<SqlDataRecord>();
        foreach (var r in runs)
        {
            var rec = new SqlDataRecord(RunsTvpMeta);
            rec.SetString(0, r.Id);
            rec.SetString(1, r.JobName);
            rec.SetInt32(2, (int)r.Status);
            SetNullableString(rec, 3, r.Arguments);
            SetNullableString(rec, 4, r.Result);
            SetNullableString(rec, 5, r.Reason);
            rec.SetDouble(6, r.Progress);
            rec.SetDateTimeOffset(7, r.CreatedAt);
            SetNullableDto(rec, 8, r.StartedAt);
            SetNullableDto(rec, 9, r.CompletedAt);
            SetNullableDto(rec, 10, r.CanceledAt);
            SetNullableString(rec, 11, r.NodeName);
            rec.SetInt32(12, r.Attempt);
            SetNullableString(rec, 13, r.TraceId);
            SetNullableString(rec, 14, r.SpanId);
            SetNullableString(rec, 15, r.ParentTraceId);
            SetNullableString(rec, 16, r.ParentSpanId);
            SetNullableString(rec, 17, r.ParentRunId);
            SetNullableString(rec, 18, r.RootRunId);
            SetNullableString(rec, 19, r.RerunOfRunId);
            rec.SetDateTimeOffset(20, r.NotBefore);
            SetNullableDto(rec, 21, r.NotAfter);
            SetNullableDto(rec, 22, r.ExpiresAt);
            rec.SetInt32(23, r.Priority);
            SetNullableString(rec, 24, r.DeduplicationId);
            SetNullableDto(rec, 25, r.LastHeartbeatAt);
            SetNullableString(rec, 26, r.BatchId);
            rec.SetInt64(27, r.LeaseEpoch);
            rec.SetInt32(28, r.FailureCount);
            rec.SetInt32(29, r.ReplayCount);
            rec.SetInt32(30, r.HighestRecordedStep);
            rec.SetBoolean(31, r.IsDurable);
            list.Add(rec);
        }

        return list;
    }

    private static List<SqlDataRecord> NamesToTvp(IEnumerable<string> names)
    {
        var list = new List<SqlDataRecord>();
        foreach (var n in names)
        {
            var rec = new SqlDataRecord(NameListTvpMeta);
            rec.SetString(0, n);
            list.Add(rec);
        }

        return list;
    }

    private static List<SqlDataRecord> NameCountsToTvp(IEnumerable<KeyValuePair<string, int>> pairs)
    {
        var list = new List<SqlDataRecord>();
        foreach (var (n, c) in pairs)
        {
            var rec = new SqlDataRecord(NameCountTvpMeta);
            rec.SetString(0, n);
            rec.SetInt32(1, c);
            list.Add(rec);
        }

        return list;
    }

    private static void SetNullableString(SqlDataRecord rec, int ordinal, string? value)
    {
        if (value is null)
        {
            rec.SetDBNull(ordinal);
        }
        else
        {
            rec.SetString(ordinal, value);
        }
    }

    private static void SetNullableDto(SqlDataRecord rec, int ordinal, DateTimeOffset? value)
    {
        if (value.HasValue)
        {
            rec.SetDateTimeOffset(ordinal, value.Value);
        }
        else
        {
            rec.SetDBNull(ordinal);
        }
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
        CanceledAt = reader.IsDBNull(reader.GetOrdinal("canceled_at"))
            ? null
            : reader.GetDateTimeOffset(reader.GetOrdinal("canceled_at")),
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
        ExpiresAt = reader.IsDBNull(reader.GetOrdinal("expires_at"))
            ? null
            : reader.GetDateTimeOffset(reader.GetOrdinal("expires_at")),
        DeduplicationId = reader.IsDBNull(reader.GetOrdinal("deduplication_id"))
            ? null
            : reader.GetString(reader.GetOrdinal("deduplication_id")),
        LastHeartbeatAt = reader.IsDBNull(reader.GetOrdinal("last_heartbeat_at"))
            ? null
            : reader.GetDateTimeOffset(reader.GetOrdinal("last_heartbeat_at")),
        BatchId = reader.IsDBNull(reader.GetOrdinal("batch_id"))
            ? null
            : reader.GetString(reader.GetOrdinal("batch_id")),
        LeaseEpoch = reader.GetInt64(reader.GetOrdinal("lease_epoch")),
        FailureCount = reader.GetInt32(reader.GetOrdinal("failure_count")),
        ReplayCount = reader.GetInt32(reader.GetOrdinal("replay_count")),
        HighestRecordedStep = reader.GetInt32(reader.GetOrdinal("highest_recorded_step")),
        IsDurable = reader.GetBoolean(reader.GetOrdinal("is_durable"))
    };

    private static JobBatch ReadBatch(SqlDataReader reader)
    {
        var idCol = reader.GetOrdinal("id");
        var statusCol = reader.GetOrdinal("status");
        var totalCol = reader.GetOrdinal("total");
        var succeededCol = reader.GetOrdinal("succeeded");
        var failedCol = reader.GetOrdinal("failed");
        var canceledCol = reader.GetOrdinal("canceled");
        var createdAtCol = reader.GetOrdinal("created_at");
        var completedAtCol = reader.GetOrdinal("completed_at");
        var parentCol = reader.GetOrdinal("parent_run_id");

        return new()
        {
            Id = reader.GetString(idCol),
            Status = (JobStatus)reader.GetInt16(statusCol),
            Total = reader.GetInt32(totalCol),
            Succeeded = reader.GetInt32(succeededCol),
            Failed = reader.GetInt32(failedCol),
            Canceled = reader.GetInt32(canceledCol),
            CreatedAt = reader.GetDateTimeOffset(createdAtCol),
            CompletedAt = reader.IsDBNull(completedAtCol) ? null : reader.GetDateTimeOffset(completedAtCol),
            ParentRunId = reader.IsDBNull(parentCol) ? null : reader.GetString(parentCol)
        };
    }

    private static DurableRecord ReadDurableRecord(SqlDataReader reader) =>
        new(
            reader.GetString(reader.GetOrdinal("orchestrator_run_id")),
            reader.GetInt32(reader.GetOrdinal("step")),
            reader.GetString(reader.GetOrdinal("kind")),
            reader.IsDBNull(reader.GetOrdinal("name")) ? null : reader.GetString(reader.GetOrdinal("name")),
            reader.GetString(reader.GetOrdinal("payload")),
            reader.GetDateTimeOffset(reader.GetOrdinal("created_at")));

    private static void AddDurableRecordParameters(SqlCommand cmd, DurableRecord record)
    {
        cmd.Parameters.Add(CreateParameter("@orchestrator_run_id", record.OrchestratorRunId));
        cmd.Parameters.Add(CreateParameter("@step", record.Step));
        cmd.Parameters.Add(CreateParameter("@kind", record.Kind));
        cmd.Parameters.Add(CreateParameter("@name", (object?)record.Name ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@payload", record.Payload));
        cmd.Parameters.Add(CreateParameter("@created_at", record.CreatedAt));
    }

    private static bool DurableRecordsEqual(DurableRecord left, DurableRecord right) =>
        string.Equals(left.OrchestratorRunId, right.OrchestratorRunId, StringComparison.Ordinal)
        && left.Step == right.Step
        && string.Equals(left.Kind, right.Kind, StringComparison.Ordinal)
        && string.Equals(left.Name, right.Name, StringComparison.Ordinal)
        && string.Equals(left.Payload, right.Payload, StringComparison.Ordinal);

    private static string DescribeRecord(DurableRecord record) =>
        record.Name is { Length: > 0 }
            ? $"record '{record.Name}' ({record.Kind})"
            : $"record kind '{record.Kind}'";

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
        var sourceCol = reader.GetOrdinal("source_code");
        var heartbeatCol = reader.GetOrdinal("last_heartbeat_at");
        var cronFireCol = reader.GetOrdinal("last_cron_fire_at");

        return new()
        {
            Name = reader.GetString(reader.GetOrdinal("name")),
            Tags = JsonSerializer.Deserialize(reader.GetString(reader.GetOrdinal("tags")),
                SurefireJsonContext.Default.StringArray) ?? [],
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
            SourceCode = reader.IsDBNull(sourceCol) ? null : reader.GetString(sourceCol),
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
        RegisteredJobNames = JsonSerializer.Deserialize(
            reader.GetString(reader.GetOrdinal("registered_job_names")),
            SurefireJsonContext.Default.StringArray) ?? [],
        RegisteredQueueNames = JsonSerializer.Deserialize(
            reader.GetString(reader.GetOrdinal("registered_queue_names")),
            SurefireJsonContext.Default.StringArray) ?? []
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

    private static void BuildRunFilterWhere(RunFilter filter, List<string> parts, SqlCommand cmd)
    {
        if (filter.Status is { })
        {
            parts.Add("status = @filter_status");
            cmd.Parameters.Add(CreateParameter("@filter_status", (int)filter.Status.Value));
        }

        if (filter.JobName is { })
        {
            parts.Add("job_name = @filter_job_name");
            cmd.Parameters.Add(CreateParameter("@filter_job_name", filter.JobName));
        }

        if (filter.JobNameContains is { })
        {
            parts.Add("job_name LIKE '%' + @filter_job_name_contains + '%'");
            cmd.Parameters.Add(CreateParameter("@filter_job_name_contains", EscapeLike(filter.JobNameContains)));
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
            parts.Add(filter.IsTerminal.Value ? "status IN (2, 4, 5)" : "status NOT IN (2, 4, 5)");
        }
    }

    // Use sequential sp_getapplock calls to force a stable lock order for multi-row writers.
    // Row UPDLOCK queries with ORDER BY do not guarantee acquisition order, which can deadlock
    // claim, transition, and cancel paths when they touch the same config rows.
    // Take this before row UPDLOCK on any path that touches more than one row of the target table.
    private static string BuildSortedApplockSql(string tableExpr, string lockKeyPrefix, string suffix)
        => $$"""
             DECLARE @__cur_{{suffix}} CURSOR;
             SET @__cur_{{suffix}} = CURSOR LOCAL FAST_FORWARD READ_ONLY FOR
                 SELECT N'{{lockKeyPrefix}}' + name FROM {{tableExpr}} ORDER BY name;
             OPEN @__cur_{{suffix}};
             DECLARE @__name_{{suffix}} NVARCHAR(900);
             DECLARE @__r_{{suffix}} INT;
             FETCH NEXT FROM @__cur_{{suffix}} INTO @__name_{{suffix}};
             WHILE @@FETCH_STATUS = 0
             BEGIN
                 EXEC @__r_{{suffix}} = sp_getapplock @Resource = @__name_{{suffix}},
                     @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;
                 IF @__r_{{suffix}} < 0 THROW 50005, 'Failed to acquire ordered applock', 1;
                 FETCH NEXT FROM @__cur_{{suffix}} INTO @__name_{{suffix}};
             END
             CLOSE @__cur_{{suffix}};
             DEALLOCATE @__cur_{{suffix}};
             """;

    // Tree applocks must be acquired in a deterministic order so concurrent callers can't deadlock
    // on the advisory itself.
    private static string BuildTreeLockSql(IReadOnlyList<string> orderedKeys, SqlCommand cmd, ref int paramOffset)
    {
        if (orderedKeys.Count == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder("DECLARE @__tree_lock_r INT;\n");
        for (var i = 0; i < orderedKeys.Count; i++)
        {
            var p = "@__tree_key_" + paramOffset++;
            sb.Append("EXEC @__tree_lock_r = sp_getapplock @Resource = ").Append(p)
                .Append(", @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;\n")
                .Append("IF @__tree_lock_r < 0 THROW 50003, 'Failed to acquire tree lock', 1;\n");
            cmd.Parameters.Add(CreateParameter(p, "surefire_tree:" + orderedKeys[i]));
        }

        return sb.ToString();
    }

    private static IReadOnlyList<string> OrderTreeKeys(IEnumerable<string?> rootKeys) =>
        rootKeys.OfType<string>()
            .Distinct(StringComparer.Ordinal)
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

    private async Task CreateRunsAsyncCore(IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        if (runs.Count == 0 && (initialEvents is null || initialEvents.Count == 0))
        {
            return;
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        await CreateRunsCoreInTransactionAsync(conn, tx, runs, initialEvents, cancellationToken);

        await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
    }

    private async Task CreateRunsCoreInTransactionAsync(SqlConnection conn, SqlTransaction tx,
        IReadOnlyList<JobRun> runs, IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        if (runs.Count == 0 && (initialEvents is null || initialEvents.Count == 0))
        {
            return;
        }

        var jobNamesToLock = runs
            .Where(r => !r.Status.IsTerminal)
            .Select(r => r.JobName)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();
        var treeKeys = OrderTreeKeys(runs.Select(r => r.RootRunId));
        var hasEvents = initialEvents is { Count: > 0 };

        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;

        var sb = new StringBuilder("SET NOCOUNT ON; SET XACT_ABORT ON;\n");
        var paramOffset = 0;
        sb.Append(BuildTreeLockSql(treeKeys, cmd, ref paramOffset));

        if (jobNamesToLock.Count > 0)
        {
            // Lock affected jobs in name order so claim/transition/create can't form a deadlock
            // cycle. The applock cursor enforces a deterministic acquisition order on the meta
            // serialization channel (surefire_job:<name>); the row UPDLOCK below is then
            // uncontended within this transaction.
            sb.Append(BuildSortedApplockSql("@lock_jobs", "surefire_job:", "cr"));
            sb.Append('\n');
            sb.Append("""
                      DECLARE @lock_sink_jobs NVARCHAR(450);
                      SELECT @lock_sink_jobs = j.name FROM @lock_jobs ln
                      INNER JOIN dbo.surefire_jobs j WITH (UPDLOCK, ROWLOCK) ON j.name = ln.name
                      ORDER BY ln.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);
                      """);
            cmd.Parameters.Add(CreateTvpParameter("@lock_jobs", "dbo.surefire_name_list",
                NamesToTvp(jobNamesToLock)));
            sb.Append('\n');
        }

        if (runs.Count > 0)
        {
            sb.Append("""
                      INSERT INTO dbo.surefire_runs (
                          id, job_name, status, arguments, result, reason, progress,
                          created_at, started_at, completed_at, canceled_at, node_name,
                          attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                          rerun_of_run_id, not_before, not_after, expires_at, priority, deduplication_id,
                          last_heartbeat_at, batch_id, lease_epoch, failure_count, replay_count,
                          highest_recorded_step, is_durable
                      )
                      SELECT
                          id, job_name, status, arguments, result, reason, progress,
                          created_at, started_at, completed_at, canceled_at, node_name,
                          attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                          rerun_of_run_id, not_before, not_after, expires_at, priority, deduplication_id,
                          last_heartbeat_at, batch_id, lease_epoch, failure_count, replay_count,
                          highest_recorded_step, is_durable
                      FROM @runs;

                      """);
            cmd.Parameters.Add(CreateTvpParameter("@runs", "dbo.surefire_runs_input", RunsToTvp(runs)));

            var increments = runs
                .Where(r => !r.Status.IsTerminal)
                .GroupBy(r => r.JobName, StringComparer.Ordinal)
                .Select(g => new KeyValuePair<string, int>(g.Key, g.Count()))
                .ToList();
            if (increments.Count > 0)
            {
                sb.Append("""
                          UPDATE j SET non_terminal_count = j.non_terminal_count + v.cnt
                          FROM dbo.surefire_jobs j INNER JOIN @inc v ON v.name = j.name;
                          """);
                cmd.Parameters.Add(CreateTvpParameter("@inc", "dbo.surefire_name_count",
                    NameCountsToTvp(increments)));
                sb.Append('\n');
            }
        }

        if (hasEvents)
        {
            sb.Append("""
                      INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                      SELECT run_id, event_type, payload, created_at, attempt FROM @events;
                      """);
            cmd.Parameters.Add(CreateTvpParameter("@events", "dbo.surefire_events_input",
                EventsToTvp(initialEvents!)));
        }

        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(cancellationToken).WithSqlCancellation(cancellationToken);
    }

    private async Task<bool> TryCreateRunAsyncCore(JobRun run, int? maxActiveForJob,
        DateTimeOffset? lastCronFireAt,
        IReadOnlyList<RunEvent>? initialEvents,
        DurableStepRecord? durableStepRecord,
        CancellationToken cancellationToken)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;

        var hasEvents = initialEvents is { Count: > 0 };
        var treeKeys = OrderTreeKeys([run.RootRunId]);

        var sb = new StringBuilder("SET NOCOUNT ON; SET XACT_ABORT ON;\n");
        var paramOffset = 0;
        sb.Append(BuildTreeLockSql(treeKeys, cmd, ref paramOffset));

        // Lock the job row so the non_terminal_count check and decrement are consistent.
        // Take a job-applock first (alpha-singleton) so multi-row writers (claim / cancel) can't
        // contend on the row UPDLOCK in optimizer-dependent order.
        sb.Append("""
                  DECLARE @__cr_meta_r INT;
                  DECLARE @__cr_meta_key NVARCHAR(900) = N'surefire_job:' + @job_name;
                  EXEC @__cr_meta_r = sp_getapplock @Resource = @__cr_meta_key,
                      @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;
                  IF @__cr_meta_r < 0 THROW 50005, 'Failed to acquire job meta lock', 1;

                  DECLARE @lock_sink_job NVARCHAR(450);
                  SELECT @lock_sink_job = j.name FROM dbo.surefire_jobs j WITH (UPDLOCK, ROWLOCK)
                  WHERE j.name = @job_name;

                  """);

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
            // Disabled jobs are gated on the claim path, not creation, so a trigger fired
            // while a job is disabled produces a Pending run that sits idle until re-enabled.
            conditions.Add(
                "ISNULL((SELECT non_terminal_count FROM dbo.surefire_jobs WHERE name = @job_name), 0) < @max_active");
            cmd.Parameters.Add(CreateParameter("@max_active", maxActiveForJob.Value));
        }

        var insertWhere = conditions.Count == 0
            ? "WHERE NOT EXISTS (SELECT 1 FROM dbo.surefire_runs WHERE id = @id)"
            : "WHERE NOT EXISTS (SELECT 1 FROM dbo.surefire_runs WHERE id = @id) AND " +
              string.Join(" AND ", conditions);

        sb.Append($"""
                   INSERT INTO dbo.surefire_runs (
                       id, job_name, status, arguments, result, reason, progress,
                       created_at, started_at, completed_at, canceled_at, node_name,
                       attempt, trace_id, span_id, parent_trace_id, parent_span_id, parent_run_id, root_run_id,
                       rerun_of_run_id, not_before, not_after, expires_at, priority, deduplication_id,
                       last_heartbeat_at, batch_id, lease_epoch, failure_count, replay_count,
                       highest_recorded_step, is_durable
                   )
                   SELECT
                       @id, @job_name, @status, @arguments, @result, @reason, @progress,
                       @created_at, @started_at, @completed_at, @canceled_at, @node_name,
                       @attempt, @trace_id, @span_id, @parent_trace_id, @parent_span_id, @parent_run_id, @root_run_id,
                       @rerun_of_run_id, @not_before, @not_after, @expires_at, @priority, @deduplication_id,
                       @last_heartbeat_at, @batch_id, @lease_epoch, @failure_count, @replay_count,
                       @highest_recorded_step, @is_durable
                   {insertWhere};

                   DECLARE @inserted INT = @@ROWCOUNT;

                   IF @inserted > 0 AND @initial_status NOT IN (2, 4, 5)
                       UPDATE dbo.surefire_jobs SET non_terminal_count = non_terminal_count + 1
                       WHERE name = @job_name;

                   IF @inserted > 0 AND @last_cron_fire_at IS NOT NULL
                       UPDATE dbo.surefire_jobs SET last_cron_fire_at = @last_cron_fire_at
                       WHERE name = @job_name;

                   IF @inserted > 0 AND @orchestrator_run_id IS NOT NULL
                       UPDATE dbo.surefire_runs
                       SET highest_recorded_step = CASE
                               WHEN highest_recorded_step > @durable_step THEN highest_recorded_step
                               ELSE @durable_step
                           END
                       WHERE id = @orchestrator_run_id;

                   """);

        if (hasEvents)
        {
            sb.Append("""
                      IF @inserted > 0
                          INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                          SELECT run_id, event_type, payload, created_at, attempt FROM @events;

                      """);
            cmd.Parameters.Add(CreateTvpParameter("@events", "dbo.surefire_events_input",
                EventsToTvp(initialEvents!)));
        }

        sb.Append("SELECT @inserted;");

        cmd.CommandText = sb.ToString();

        AddRunInsertParams(cmd, run);
        cmd.Parameters.Add(CreateParameter("@initial_status", (int)run.Status));
        cmd.Parameters.Add(CreateParameter("@last_cron_fire_at",
            lastCronFireAt.HasValue ? lastCronFireAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@orchestrator_run_id",
            (object?)durableStepRecord?.OrchestratorRunId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@durable_step",
            durableStepRecord is { } drr ? drr.Step : 0));

        try
        {
            var result = await cmd.ExecuteScalarAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            var inserted = Convert.ToInt32(result) > 0;
            if (inserted)
            {
                await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
                return true;
            }

            // Inserted == 0: disambiguate id conflict from dedup hit vs capacity hit. Same
            // transaction so reads see the same snapshot the INSERT saw.
            await using (var idCheck = CreateCommand(conn))
            {
                idCheck.Transaction = tx;
                idCheck.CommandText = "SELECT TOP 1 1 FROM dbo.surefire_runs WHERE id = @id";
                idCheck.Parameters.Add(CreateParameter("@id", run.Id));
                if (await idCheck.ExecuteScalarAsync(cancellationToken).WithSqlCancellation(cancellationToken) is not null)
                {
                    await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
                    return false;
                }
            }

            if (run.DeduplicationId is { } dedup)
            {
                await using var dedupCheck = CreateCommand(conn);
                dedupCheck.Transaction = tx;
                dedupCheck.CommandText = """
                                         SELECT TOP 1 1 FROM dbo.surefire_runs
                                         WHERE job_name = @job_name AND deduplication_id = @dedup_id
                                             AND status NOT IN (2, 4, 5)
                                         """;
                dedupCheck.Parameters.Add(CreateParameter("@job_name", run.JobName));
                dedupCheck.Parameters.Add(CreateParameter("@dedup_id", dedup));
                if (await dedupCheck.ExecuteScalarAsync(cancellationToken).WithSqlCancellation(cancellationToken) is not null)
                {
                    await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
                    throw new RunConflictException(run.Id,
                        $"Run with deduplication id '{dedup}' is already active for job '{run.JobName}'.");
                }
            }

            await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            throw new RunConflictException(run.Id,
                $"Job '{run.JobName}' is at the maximum active run capacity ({maxActiveForJob ?? 0}).");
        }
        catch (SqlException ex) when (ex.Number is 2601 or 2627)
        {
            // A concurrent caller won the insert race. Disambiguate the dedup unique index from
            // the run id PK so the caller still observes the dedup contract.
            if (ex.Message.Contains("ix_surefire_runs_dedup", StringComparison.OrdinalIgnoreCase))
            {
                throw new RunConflictException(run.Id,
                    $"Run with deduplication id '{run.DeduplicationId}' is already active for job '{run.JobName}'.", ex);
            }

            return false;
        }
    }

    private static void AddRunInsertParams(SqlCommand cmd, JobRun run)
    {
        cmd.Parameters.Add(CreateParameter("@id", run.Id));
        cmd.Parameters.Add(CreateParameter("@job_name", run.JobName));
        cmd.Parameters.Add(CreateParameter("@status", (int)run.Status));
        cmd.Parameters.Add(CreateParameter("@arguments", (object?)run.Arguments ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@result", (object?)run.Result ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@reason", (object?)run.Reason ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@progress", run.Progress));
        cmd.Parameters.Add(CreateParameter("@created_at", run.CreatedAt));
        cmd.Parameters.Add(CreateParameter("@started_at",
            run.StartedAt.HasValue ? run.StartedAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@completed_at",
            run.CompletedAt.HasValue ? run.CompletedAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@canceled_at",
            run.CanceledAt.HasValue ? run.CanceledAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@node_name", (object?)run.NodeName ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@attempt", run.Attempt));
        cmd.Parameters.Add(CreateParameter("@trace_id", (object?)run.TraceId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@span_id", (object?)run.SpanId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@parent_trace_id", (object?)run.ParentTraceId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@parent_span_id", (object?)run.ParentSpanId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@parent_run_id", (object?)run.ParentRunId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@root_run_id", (object?)run.RootRunId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@rerun_of_run_id", (object?)run.RerunOfRunId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@not_before", run.NotBefore));
        cmd.Parameters.Add(CreateParameter("@not_after",
            run.NotAfter.HasValue ? run.NotAfter.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@expires_at",
            run.ExpiresAt.HasValue ? run.ExpiresAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@priority", run.Priority));
        cmd.Parameters.Add(CreateParameter("@deduplication_id", (object?)run.DeduplicationId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@last_heartbeat_at",
            run.LastHeartbeatAt.HasValue ? run.LastHeartbeatAt.Value : DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@batch_id", (object?)run.BatchId ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@lease_epoch", run.LeaseEpoch));
        cmd.Parameters.Add(CreateParameter("@failure_count", run.FailureCount));
        cmd.Parameters.Add(CreateParameter("@replay_count", run.ReplayCount));
        cmd.Parameters.Add(CreateParameter("@highest_recorded_step", run.HighestRecordedStep));
        cmd.Parameters.Add(CreateParameter("@is_durable", run.IsDurable));
    }

    private async Task<IReadOnlyList<JobRun>> ClaimRunsAsyncCore(string nodeName,
        IReadOnlyCollection<string> jobNames, IReadOnlyCollection<string> queueNames, int maxCount,
        CancellationToken cancellationToken)
    {
        var orderedJobNames = jobNames.Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal).ToList();
        var orderedQueueNames = queueNames.Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal).ToList();

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;

        // The candidate UPDATE uses READPAST so claimers with overlapping eligible sets silently
        // miss locked rows instead of blocking or deadlocking.
        //
        // Lock acquisition order on surefire_jobs/queues/rate_limits is enforced by
        // sp_getapplock per name, fetched from the source table in ORDER BY name. The row UPDLOCK
        // that follows is uncontended within the applock-holding transaction.
        cmd.CommandText = $$"""
                            SET NOCOUNT ON; SET XACT_ABORT ON;

                            {{BuildSortedApplockSql("@job_names", "surefire_job:", "cj")}}

                            DECLARE @lock_sink NVARCHAR(450);

                            SELECT @lock_sink = j.name FROM @job_names jn
                            INNER JOIN dbo.surefire_jobs j WITH (UPDLOCK, ROWLOCK) ON j.name = jn.name
                            ORDER BY jn.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);

                            DECLARE @target_queues TABLE (name NVARCHAR(450) NOT NULL PRIMARY KEY);
                            INSERT INTO @target_queues (name)
                            SELECT DISTINCT q.name FROM @queue_names qn
                            INNER JOIN dbo.surefire_jobs j ON j.name IN (SELECT name FROM @job_names)
                            INNER JOIN dbo.surefire_queues q ON q.name = qn.name
                            WHERE qn.name = COALESCE(j.queue, N'default');

                            {{BuildSortedApplockSql("@target_queues", "surefire_queue:", "cq")}}

                            SELECT @lock_sink = q.name FROM @target_queues tq
                            INNER JOIN dbo.surefire_queues q WITH (UPDLOCK, ROWLOCK) ON q.name = tq.name
                            ORDER BY tq.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);

                            DECLARE @target_rate_limits TABLE (name NVARCHAR(450) NOT NULL PRIMARY KEY);
                            INSERT INTO @target_rate_limits (name)
                            SELECT DISTINCT rl_name FROM (
                                SELECT j.rate_limit_name AS rl_name FROM dbo.surefire_jobs j
                                INNER JOIN @job_names jn ON jn.name = j.name
                                WHERE j.rate_limit_name IS NOT NULL
                                UNION
                                SELECT q.rate_limit_name FROM dbo.surefire_jobs j
                                INNER JOIN @job_names jn ON jn.name = j.name
                                INNER JOIN dbo.surefire_queues q ON q.name = COALESCE(j.queue, N'default')
                                WHERE q.rate_limit_name IS NOT NULL
                            ) src
                            WHERE EXISTS (SELECT 1 FROM dbo.surefire_rate_limits rl WHERE rl.name = src.rl_name);

                            {{BuildSortedApplockSql("@target_rate_limits", "surefire_rl:", "crl")}}

                            SELECT @lock_sink = rl.name FROM @target_rate_limits trl
                            INNER JOIN dbo.surefire_rate_limits rl WITH (UPDLOCK, ROWLOCK) ON rl.name = trl.name
                            ORDER BY trl.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);

                            DECLARE @now DATETIMEOFFSET(7) = SYSUTCDATETIME();
                            DECLARE @candidate TABLE (
                                id NVARCHAR(450) NOT NULL PRIMARY KEY,
                                run_queue_name NVARCHAR(450) NOT NULL,
                                j_rl NVARCHAR(450) NULL,
                                q_rl NVARCHAR(450) NULL
                            );

                            DECLARE @claimed TABLE (
                                id NVARCHAR(450) NOT NULL PRIMARY KEY,
                                job_name NVARCHAR(450) NOT NULL,
                                queue_name NVARCHAR(450) NOT NULL,
                                j_rl NVARCHAR(450) NULL,
                                q_rl NVARCHAR(450) NULL,
                                status INT NOT NULL,
                                attempt INT NOT NULL
                            );

                            ;WITH rl_state AS (
                                SELECT rl.name,
                                    CAST(CEILING(CASE WHEN rl.max_permits -
                                        CASE
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
                                        END > 0 THEN rl.max_permits -
                                        CASE
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
                                        END
                                    ELSE 0 END) AS INT) AS available
                                FROM dbo.surefire_rate_limits rl
                            ),
                            ranked AS (
                                SELECT r.id, ISNULL(q.priority, 0) AS queue_priority, r.priority, r.not_before,
                                    r.job_name AS run_job_name, ISNULL(j.queue, N'default') AS run_queue_name,
                                    j.max_concurrency AS j_max, q.max_concurrency AS q_max,
                                    j.running_count AS j_running, ISNULL(q.running_count, 0) AS q_running,
                                    j.rate_limit_name AS j_rl, q.rate_limit_name AS q_rl,
                                    ROW_NUMBER() OVER (PARTITION BY r.job_name
                                        ORDER BY ISNULL(q.priority, 0) DESC, r.priority DESC, r.not_before ASC, r.id ASC) AS j_rn,
                                    ROW_NUMBER() OVER (PARTITION BY ISNULL(j.queue, N'default')
                                        ORDER BY ISNULL(q.priority, 0) DESC, r.priority DESC, r.not_before ASC, r.id ASC) AS q_rn,
                                    ROW_NUMBER() OVER (PARTITION BY j.rate_limit_name
                                        ORDER BY ISNULL(q.priority, 0) DESC, r.priority DESC, r.not_before ASC, r.id ASC) AS j_rl_rn,
                                    ROW_NUMBER() OVER (PARTITION BY q.rate_limit_name
                                        ORDER BY ISNULL(q.priority, 0) DESC, r.priority DESC, r.not_before ASC, r.id ASC) AS q_rl_rn
                                FROM dbo.surefire_runs r
                                JOIN dbo.surefire_jobs j ON j.name = r.job_name
                                LEFT JOIN dbo.surefire_queues q ON q.name = ISNULL(j.queue, N'default')
                                WHERE r.status = 0
                                    AND r.not_before <= @now
                                    AND (r.not_after IS NULL OR r.lease_epoch > 0 OR r.not_after >= @now)
                                    AND (r.expires_at IS NULL OR r.expires_at >= @now)
                                    AND r.job_name IN (SELECT name FROM @job_names)
                                    AND ISNULL(j.queue, N'default') IN (SELECT name FROM @queue_names)
                                    AND ISNULL(q.is_paused, 0) = 0
                            ),
                            eligible AS (
                                SELECT TOP (@max_count) r.id, r.run_queue_name, r.j_rl, r.q_rl
                                FROM ranked r
                                LEFT JOIN rl_state jrl ON jrl.name = r.j_rl
                                LEFT JOIN rl_state qrl ON qrl.name = r.q_rl
                                WHERE (r.j_max IS NULL OR r.j_rn <= r.j_max - r.j_running)
                                    AND (r.q_max IS NULL OR r.q_rn <= r.q_max - r.q_running)
                                    AND (r.j_rl IS NULL OR jrl.available IS NULL OR r.j_rl_rn <= jrl.available)
                                    AND (r.q_rl IS NULL OR r.q_rl = r.j_rl OR qrl.available IS NULL OR r.q_rl_rn <= qrl.available)
                                ORDER BY r.queue_priority DESC, r.priority DESC, r.not_before ASC, r.id ASC
                            )
                            INSERT INTO @candidate (id, run_queue_name, j_rl, q_rl)
                            SELECT id, run_queue_name, j_rl, q_rl FROM eligible
                            OPTION (MAXDOP 1);

                             UPDATE r SET
                                 status = 1,
                                 node_name = @node_name,
                                 started_at = COALESCE(r.started_at, @now),
                                 last_heartbeat_at = @now,
                                 lease_epoch = r.lease_epoch + 1
                             OUTPUT INSERTED.id, INSERTED.job_name, c.run_queue_name, c.j_rl, c.q_rl,
                                   INSERTED.status, INSERTED.attempt
                                INTO @claimed (id, job_name, queue_name, j_rl, q_rl, status, attempt)
                            FROM dbo.surefire_runs r WITH (UPDLOCK, ROWLOCK, READPAST)
                            INNER JOIN @candidate c ON c.id = r.id
                            WHERE r.status = 0
                                AND r.not_before <= @now
                                AND (r.not_after IS NULL OR r.lease_epoch > 0 OR r.not_after >= @now)
                                AND (r.expires_at IS NULL OR r.expires_at >= @now);

                            UPDATE j SET running_count = j.running_count + ji.cnt
                            FROM dbo.surefire_jobs j
                            INNER JOIN (SELECT job_name, COUNT(*) AS cnt FROM @claimed GROUP BY job_name) ji
                                ON ji.job_name = j.name;

                            UPDATE q SET running_count = q.running_count + qi.cnt
                            FROM dbo.surefire_queues q
                            INNER JOIN (SELECT queue_name, COUNT(*) AS cnt FROM @claimed GROUP BY queue_name) qi
                                ON qi.queue_name = q.name;

                            ;WITH rl_pairs AS (
                                SELECT j_rl AS rl_name FROM @claimed WHERE j_rl IS NOT NULL
                                UNION ALL
                                SELECT q_rl FROM @claimed WHERE q_rl IS NOT NULL AND q_rl <> ISNULL(j_rl, N'')
                            ),
                            rl_increments AS (
                                SELECT rl_name, COUNT(*) AS cnt FROM rl_pairs GROUP BY rl_name
                            )
                            UPDATE rl SET
                                previous_count = CASE
                                    WHEN window_start IS NULL THEN 0
                                    WHEN DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10 >= [window] * 2 THEN 0
                                    WHEN DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10 >= [window] THEN current_count
                                    ELSE previous_count
                                END,
                                current_count = CASE
                                    WHEN window_start IS NULL THEN i.cnt
                                    WHEN DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10 >= [window] THEN i.cnt
                                    ELSE current_count + i.cnt
                                END,
                                window_start = CASE
                                    WHEN window_start IS NULL THEN @now
                                    WHEN DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10 >= [window] * 2 THEN
                                        DATEADD(MICROSECOND,
                                            CAST(((DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10) / [window]) * [window] / 10 AS INT),
                                            window_start)
                                    WHEN DATEDIFF_BIG(MICROSECOND, window_start, @now) * 10 >= [window] THEN
                                        DATEADD(MICROSECOND, CAST([window] / 10 AS INT), window_start)
                                    ELSE window_start
                                END
                            FROM dbo.surefire_rate_limits rl
                            INNER JOIN rl_increments i ON i.rl_name = rl.name;

                            INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                            SELECT c.id, 0, CONVERT(NVARCHAR(10), c.status), @now, c.attempt FROM @claimed c;

                            SELECT r.id, r.job_name, r.status, r.arguments, r.result, r.reason, r.progress,
                                   r.created_at, r.started_at, r.completed_at, r.canceled_at, r.node_name, r.attempt,
                                   r.trace_id, r.span_id, r.parent_trace_id, r.parent_span_id, r.parent_run_id,
                                   r.root_run_id, r.rerun_of_run_id, r.not_before, r.not_after, r.expires_at, r.priority,
                                    r.deduplication_id, r.last_heartbeat_at, r.batch_id,
                                    r.lease_epoch, r.failure_count, r.replay_count, r.highest_recorded_step, r.is_durable
                            FROM dbo.surefire_runs r INNER JOIN @claimed c ON c.id = r.id;
                            """;

        cmd.Parameters.Add(CreateTvpParameter("@job_names", "dbo.surefire_name_list", NamesToTvp(orderedJobNames)));
        cmd.Parameters.Add(CreateTvpParameter("@queue_names", "dbo.surefire_name_list", NamesToTvp(orderedQueueNames)));
        cmd.Parameters.Add(CreateParameter("@node_name", nodeName));
        cmd.Parameters.Add(CreateParameter("@max_count", maxCount));

        var claimed = new List<JobRun>();
        await using (var reader =
                     await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                claimed.Add(ReadRun(reader));
            }
        }

        await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        return claimed;
    }

    // The tree applock keyed on the resolved RootRunId serializes cancels and creates within the
    // same tree while leaving disjoint trees fully concurrent. JobClient guarantees every run in a
    // batch shares a single RootRunId (top-level batches: RootRunId = batchId; nested batches:
    // RootRunId = enclosing run's RootRunId), so TOP(1) is sufficient: any row from the batch
    // resolves the canonical tree key.
    private async Task<SubtreeCancellation> CancelSubtreeAsyncCore(SubtreeSeed seed, string seedId,
        string? reason, bool includeRoot, CancellationToken cancellationToken,
        bool expirationCancellation = false)
    {
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        await using var tx = (SqlTransaction)await conn
            .BeginTransactionAsync(IsolationLevel.ReadCommitted, cancellationToken)
            .WithSqlCancellation(cancellationToken);

        // Existence check up front so we can distinguish NotFound from Empty.
        await using (var existsCmd = CreateCommand(conn))
        {
            existsCmd.Transaction = tx;
            existsCmd.CommandText = seed switch
            {
                SubtreeSeed.Run => "SELECT 1 FROM dbo.surefire_runs WHERE id = @seed",
                SubtreeSeed.Batch => "SELECT 1 FROM dbo.surefire_batches WHERE id = @seed",
                _ => throw new ArgumentOutOfRangeException(nameof(seed))
            };
            existsCmd.Parameters.Add(CreateParameter("@seed", seedId));
            if (await existsCmd.ExecuteScalarAsync(cancellationToken).WithSqlCancellation(cancellationToken) is null)
            {
                await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
                return SubtreeCancellation.NotFound;
            }
        }

        var seedClause = seed switch
        {
            SubtreeSeed.Run => "SELECT id FROM dbo.surefire_runs WHERE id = @seed",
            SubtreeSeed.Batch => "SELECT id FROM dbo.surefire_runs WHERE batch_id = @seed",
            _ => throw new ArgumentOutOfRangeException(nameof(seed))
        };
        var includeRootPredicate = seed == SubtreeSeed.Run && !includeRoot
            ? " AND r.id <> @seed"
            : string.Empty;
        var treeKeySql = seed == SubtreeSeed.Run
            ? "(SELECT TOP (1) ISNULL(root_run_id, id) FROM dbo.surefire_runs WHERE id = @seed)"
            : "(SELECT TOP (1) ISNULL(root_run_id, id) FROM dbo.surefire_runs WHERE batch_id = @seed)";

        var nowOffset = timeProvider.GetUtcNow();
        await using var cmd = CreateCommand(conn);
        cmd.Transaction = tx;
        cmd.CommandText = $"""
                           SET NOCOUNT ON; SET XACT_ABORT ON;

                           DECLARE @tree_key NVARCHAR(450) =
                               N'surefire_tree:' + ISNULL({treeKeySql}, @seed);
                           DECLARE @tree_r INT;
                           EXEC @tree_r = sp_getapplock @Resource = @tree_key,
                               @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 30000;
                           IF @tree_r < 0 THROW 50003, 'Failed to acquire tree lock', 1;

                           DECLARE @subtree TABLE (id NVARCHAR(450) NOT NULL PRIMARY KEY);
                           ;WITH walk AS (
                               {seedClause}
                               UNION ALL
                               SELECT r.id FROM dbo.surefire_runs r
                                   INNER JOIN walk w ON r.parent_run_id = w.id
                           )
                           INSERT INTO @subtree (id) SELECT id FROM walk OPTION (MAXRECURSION 0);

                           -- Subtree jobs/queues only. Wake of any Suspended orchestrator awaiting
                           -- a canceled member is performed by WakeForTerminatedRunAsync after this
                           -- statement, in a sequence of per-orchestrator transactions that take
                           -- their own job/queue applocks.
                           DECLARE @target_jobs TABLE (name NVARCHAR(450) NOT NULL PRIMARY KEY);
                           INSERT INTO @target_jobs (name)
                           SELECT DISTINCT r.job_name FROM dbo.surefire_runs r
                           INNER JOIN @subtree s ON s.id = r.id
                           WHERE r.status IN (0, 1, 3){includeRootPredicate};

                           DECLARE @target_queues TABLE (name NVARCHAR(450) NOT NULL PRIMARY KEY);
                           INSERT INTO @target_queues (name)
                           SELECT DISTINCT COALESCE(j.queue, N'default')
                           FROM dbo.surefire_runs r
                           INNER JOIN @subtree s ON s.id = r.id
                           INNER JOIN dbo.surefire_jobs j ON j.name = r.job_name
                           WHERE r.status IN (0, 1, 3){includeRootPredicate};

                           {BuildSortedApplockSql("@target_jobs", "surefire_job:", "stj")}

                           {BuildSortedApplockSql("@target_queues", "surefire_queue:", "stq")}

                           DECLARE @lock_sink NVARCHAR(450);

                           SELECT @lock_sink = j.name FROM @target_jobs tj
                           INNER JOIN dbo.surefire_jobs j WITH (UPDLOCK, ROWLOCK) ON j.name = tj.name
                           ORDER BY tj.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);

                           SELECT @lock_sink = q.name FROM @target_queues tq
                           INNER JOIN dbo.surefire_queues q WITH (UPDLOCK, ROWLOCK) ON q.name = tq.name
                           ORDER BY tq.name OPTION (FORCE ORDER, LOOP JOIN, MAXDOP 1);

                           DECLARE @now DATETIMEOFFSET(7) = SYSUTCDATETIME();
                            DECLARE @canceled TABLE (
                                id NVARCHAR(450) NOT NULL PRIMARY KEY,
                                attempt INT NOT NULL,
                                batch_id NVARCHAR(450),
                                prior_status INT NOT NULL,
                                job_name NVARCHAR(450) NOT NULL,
                                reason NVARCHAR(MAX) NULL,
                                expiration_kind INT NULL
                            );

                           -- ROWLOCK keeps lock granularity at row level even for large
                           -- subtrees. Without it, a 1000+ row UPDATE can take page X-locks
                           -- whose IX intent collides with concurrent ClaimRunsAsyncCore /
                           -- TryTransitionRunAsync calls touching unrelated rows on the same
                           -- pages.
                           UPDATE r SET
                                status = 4,
                                canceled_at = @now,
                                completed_at = @now,
                                reason = CASE
                                    WHEN @expiration_cancellation = 1 THEN
                                        CASE WHEN r.id = @seed THEN @reason
                                             ELSE N'Canceled because parent run ''' + ISNULL(r.parent_run_id, @seed) + N''' expired.'
                                        END
                                    ELSE COALESCE(@reason, r.reason)
                                END
                            OUTPUT INSERTED.id, INSERTED.attempt, INSERTED.batch_id,
                                   DELETED.status, INSERTED.job_name, INSERTED.reason,
                                   CASE WHEN INSERTED.id = @seed THEN 0 ELSE 1 END
                                INTO @canceled (id, attempt, batch_id, prior_status, job_name, reason, expiration_kind)
                           FROM dbo.surefire_runs r WITH (ROWLOCK)
                               INNER JOIN @subtree s ON s.id = r.id
                           WHERE r.status IN (0, 1, 3){includeRootPredicate};

                           -- Running (1) is the only active slot holder.
                           UPDATE j SET
                               running_count = CASE WHEN j.running_count > ag.running_cnt
                                   THEN j.running_count - ag.running_cnt ELSE 0 END,
                               non_terminal_count = CASE WHEN j.non_terminal_count > ag.nt_cnt
                                   THEN j.non_terminal_count - ag.nt_cnt ELSE 0 END
                           FROM dbo.surefire_jobs j
                           INNER JOIN (
                               SELECT job_name,
                                   SUM(CASE WHEN prior_status = 1 THEN 1 ELSE 0 END) AS running_cnt,
                                   COUNT(*) AS nt_cnt
                               FROM @canceled GROUP BY job_name
                           ) ag ON ag.job_name = j.name;

                           UPDATE q SET running_count =
                               CASE WHEN q.running_count > rq.cnt THEN q.running_count - rq.cnt ELSE 0 END
                           FROM dbo.surefire_queues q
                           INNER JOIN (
                               SELECT ISNULL(j.queue, N'default') AS queue_name, COUNT(*) AS cnt
                               FROM @canceled c
                               INNER JOIN dbo.surefire_jobs j ON j.name = c.job_name
                               WHERE c.prior_status = 1
                               GROUP BY ISNULL(j.queue, N'default')
                           ) rq ON rq.queue_name = q.name;

                           INSERT INTO dbo.surefire_events (run_id, event_type, payload, created_at, attempt)
                           SELECT c.id, 0, CONVERT(NVARCHAR(10), 4), @now, c.attempt FROM @canceled c;

                           SELECT id, attempt, batch_id, reason, expiration_kind FROM @canceled ORDER BY id;
                           """;
        cmd.Parameters.Add(CreateParameter("@seed", seedId));
        cmd.Parameters.Add(CreateParameter("@reason", (object?)reason ?? DBNull.Value));
        cmd.Parameters.Add(CreateParameter("@expiration_cancellation", expirationCancellation));

        var canceledRuns = new List<CanceledRun>();
        var expiredRuns = new List<ExpiredCanceledRun>();
        var batchCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        await using (var reader =
                     await cmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                var runId = reader.GetString(0);
                var attempt = reader.GetInt32(1);
                var batchId = reader.IsDBNull(2) ? null : reader.GetString(2);
                canceledRuns.Add(new(runId, batchId));
                if (expirationCancellation)
                {
                    var runReason = reader.IsDBNull(3) ? "Run expired past its deadline." : reader.GetString(3);
                    var kind = reader.GetInt32(4) == 0
                        ? ExpiredCancellationKind.Expired
                        : ExpiredCancellationKind.AncestorExpired;
                    expiredRuns.Add(new(runId, batchId, attempt, runReason, kind));
                }
                if (batchId is { })
                {
                    batchCounts[batchId] = batchCounts.GetValueOrDefault(batchId) + 1;
                }
            }
        }

        if (canceledRuns.Count == 0)
        {
            await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
            return SubtreeCancellation.Empty;
        }

        var completedBatches = await ApplyBatchCancelCountersAsync(conn, tx, batchCounts, cancellationToken);

        // Unified wake for canceled runs AND newly-terminal batches; one canonical sorted
        // applock acquisition pass keeps overlapping subtree cancels deadlock-free.
        if (canceledRuns.Count > 0 || completedBatches.Count > 0)
        {
            var runIds = canceledRuns.Count == 0
                ? Array.Empty<string>()
                : canceledRuns.Select(c => c.RunId).ToArray();
            var batchIds = completedBatches.Count == 0
                ? Array.Empty<string>()
                : completedBatches.Select(b => b.BatchId).ToArray();
            await WakeForTerminatedEntitiesAsync(conn, tx, runIds, batchIds, nowOffset, cancellationToken);
        }

        await tx.CommitAsync(cancellationToken).WithSqlCancellation(cancellationToken);
        return expirationCancellation
            ? new SubtreeCancellation(canceledRuns, completedBatches) { ExpiredRuns = expiredRuns }
            : new(canceledRuns, completedBatches);
    }

    private async Task<List<BatchCompletionInfo>> ApplyBatchCancelCountersAsync(SqlConnection conn,
        SqlTransaction tx, Dictionary<string, int> batchCounts, CancellationToken cancellationToken)
    {
        var completedBatches = new List<BatchCompletionInfo>();
        // Sorted iteration: two concurrent cancel transactions touching overlapping batches must
        // acquire surefire_batches X-locks in the same order or they deadlock.
        foreach (var (batchId, cnt) in batchCounts.OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            await using var incrCmd = CreateCommand(conn);
            incrCmd.Transaction = tx;
            incrCmd.CommandText = """
                                  UPDATE dbo.surefire_batches
                                  SET canceled = canceled + @cnt
                                  OUTPUT INSERTED.total, INSERTED.succeeded, INSERTED.failed, INSERTED.canceled
                                  WHERE id = @id AND status NOT IN (2, 4, 5)
                                  """;
            incrCmd.Parameters.Add(CreateParameter("@id", batchId));
            incrCmd.Parameters.Add(CreateParameter("@cnt", cnt));
            await using var batchReader =
                await incrCmd.ExecuteReaderAsync(cancellationToken).WithSqlCancellation(cancellationToken);

            if (await batchReader.ReadAsync(cancellationToken).WithSqlCancellation(cancellationToken))
            {
                var total = batchReader.GetInt32(0);
                var succeeded = batchReader.GetInt32(1);
                var failed = batchReader.GetInt32(2);
                var canceled = batchReader.GetInt32(3);

                if (succeeded + failed + canceled >= total)
                {
                    var batchStatus = failed > 0 ? JobStatus.Failed
                        : canceled > 0 ? JobStatus.Canceled
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
                    if (await completeCmd.ExecuteNonQueryAsync(cancellationToken)
                            .WithSqlCancellation(cancellationToken) > 0)
                    {
                        completedBatches.Add(new(batchId, batchStatus, completedAt));
                    }
                }
            }
        }

        return completedBatches;
    }

    private enum SubtreeSeed
    {
        Run,
        Batch
    }
}
