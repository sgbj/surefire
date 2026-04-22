using System.Collections.Concurrent;
using System.Globalization;
using System.Net.Sockets;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Surefire.Redis;

/// <summary>
///     Redis implementation of <see cref="IJobStore" />.
///     All keys use the <c>{surefire}:</c> hash tag prefix for Redis Cluster slot affinity.
///     Every mutation is a single Lua script for atomicity.
/// </summary>
internal sealed partial class RedisJobStore(
    IConnectionMultiplexer connection,
    TimeProvider timeProvider,
    ILogger<RedisJobStore> logger) : IJobStore
{
    private const string P = "{surefire}:";
    private const int FilterScanChunkSize = 500;
    private const int ChildMemberTimestampWidth = 20;

    // Shared Lua prelude. Inlined into every script that writes to the pending queue so
    // the member format is defined once and stays consistent across scripts.
    private const string PendingMemberHelper = """
                                               local function pending_member(priority, not_before_ms, run_id)
                                                   return string.format('%010d%020d%s|%s',
                                                       999999999 - priority, not_before_ms, run_id, run_id)
                                               end

                                               """;

    private const string CreateRunsScript = PendingMemberHelper + """
                                                                  local runs = cjson.decode(ARGV[1])
                                                                  local initial_events = cjson.decode(ARGV[2])

                                                                  -- Optional ARGV[3]: batch JSON to write atomically before runs
                                                                  -- become claimable, ensuring batch counter updates
                                                                  -- never race against a missing batch record.
                                                                  local batch_arg = ARGV[3]
                                                                  if batch_arg and batch_arg ~= '' then
                                                                      local batch = cjson.decode(batch_arg)
                                                                      redis.call('SET', '{surefire}:batch:' .. batch.id, batch_arg)
                                                                      redis.call('SADD', '{surefire}:batches:active', batch.id)
                                                                  end

                                                                  local function append_events(events)
                                                                      if not events then
                                                                          return
                                                                      end
                                                                      local seq_key = '{surefire}:event_seq'
                                                                      local batch_ids = {}
                                                                      for _, evt in ipairs(events) do
                                                                          local id = redis.call('INCR', seq_key)
                                                                          evt.id = id
                                                                          local payload = cjson.encode(evt)
                                                                          local id_str = tostring(id)
                                                                          redis.call('SET', '{surefire}:event:' .. id_str, payload)
                                                                          redis.call('ZADD', '{surefire}:events:run:' .. evt.runId, id, id_str)
                                                                          redis.call('ZADD', '{surefire}:events:run_type:' .. evt.runId .. ':' .. tostring(evt.eventType), id, id_str)
                                                                          redis.call('ZADD', '{surefire}:events:run_attempt:' .. evt.runId .. ':' .. tostring(evt.attempt), id, id_str)
                                                                          redis.call('SADD', '{surefire}:events:run_types:' .. evt.runId, tostring(evt.eventType))
                                                                          redis.call('SADD', '{surefire}:events:run_attempts:' .. evt.runId, tostring(evt.attempt))

                                                                          local batch_id = batch_ids[evt.runId]
                                                                          if batch_id == nil then
                                                                              local run_payload = redis.call('GET', '{surefire}:run:' .. evt.runId)
                                                                              if run_payload then
                                                                                  local run = cjson.decode(run_payload)
                                                                                  batch_id = run.batchId
                                                                              else
                                                                                  batch_id = false
                                                                              end
                                                                              batch_ids[evt.runId] = batch_id
                                                                          end

                                                                          if batch_id and batch_id ~= cjson.null and batch_id ~= '' then
                                                                              redis.call('ZADD', '{surefire}:events:batch:' .. batch_id, id, id_str)
                                                                              if evt.eventType == 3 then
                                                                                  redis.call('ZADD', '{surefire}:events:batch_output:' .. batch_id, id, id_str)
                                                                              end
                                                                          end
                                                                      end
                                                                  end

                                                                  local function resolve_queue(job_name)
                                                                      local q = redis.call('HGET', '{surefire}:job:' .. job_name, 'queue')
                                                                      if q and q ~= '' then return q end
                                                                      return 'default'
                                                                  end

                                                                  local function add_to_pending(run, run_id, qname)
                                                                      local member = pending_member(run.priority, run.notBefore, run_id)
                                                                      redis.call('ZADD', '{surefire}:pending_q:' .. qname, 0, member)
                                                                      redis.call('SET', '{surefire}:pending_member:' .. run_id, member)
                                                                  end

                                                                  local function child_member(created_at_ms, run_id)
                                                                      return string.format('%020d%s', tonumber(created_at_ms), run_id)
                                                                  end

                                                                  local function is_terminal(status)
                                                                      return status == 2 or status == 4 or status == 5
                                                                  end

                                                                  local function timeline_field(status)
                                                                      if status == 2 then return 'completed' end
                                                                      if status == 4 then return 'cancelled' end
                                                                      if status == 5 then return 'failed' end
                                                                      return nil
                                                                  end

                                                                  local function increment_job_stats_for_new_run(run)
                                                                      local stats_key = '{surefire}:job_stats:' .. run.jobName
                                                                      redis.call('HINCRBY', stats_key, 'total_runs', 1)

                                                                      if run.startedAt and run.startedAt ~= cjson.null then
                                                                          redis.call('ZADD', '{surefire}:job_started:' .. run.jobName, tonumber(run.startedAt), run.id)
                                                                      end

                                                                      if is_terminal(run.status) then
                                                                          redis.call('HINCRBY', stats_key, 'terminal_runs', 1)
                                                                          if run.status == 2 then
                                                                              redis.call('HINCRBY', stats_key, 'succeeded_runs', 1)
                                                                              if run.startedAt and run.startedAt ~= cjson.null and run.completedAt and run.completedAt ~= cjson.null then
                                                                                  redis.call('HINCRBYFLOAT', stats_key, 'duration_sum_ms', tonumber(run.completedAt) - tonumber(run.startedAt))
                                                                                  redis.call('HINCRBY', stats_key, 'duration_count', 1)
                                                                              end
                                                                          elseif run.status == 5 then
                                                                              redis.call('HINCRBY', stats_key, 'failed_runs', 1)
                                                                          end
                                                                      end
                                                                  end

                                                                  for i, run in ipairs(runs) do
                                                                      if redis.call('EXISTS', '{surefire}:run:' .. run.id) == 1 then
                                                                          return redis.error_reply('Run already exists: ' .. run.id)
                                                                      end
                                                                  end

                                                                  for i, run in ipairs(runs) do
                                                                      local run_id = run.id
                                                                      local run_key = '{surefire}:run:' .. run_id
                                                                      local qname = resolve_queue(run.jobName)
                                                                      run.priority = tonumber(run.priority) or 0
                                                                      local run_json = cjson.encode(run)
                                                                      redis.call('SET', run_key, run_json)
                                                                      increment_job_stats_for_new_run(run)

                                                                      redis.call('SADD', '{surefire}:nonterminal:' .. run.jobName, run_id)

                                                                      if run.deduplicationId and run.deduplicationId ~= cjson.null then
                                                                          redis.call('SET', '{surefire}:dedup:' .. run.jobName .. ':' .. run.deduplicationId, run_id)
                                                                      end

                                                                      if run.notAfter and run.notAfter ~= cjson.null then
                                                                          redis.call('ZADD', '{surefire}:expiring', tonumber(run.notAfter), run_id)
                                                                      end

                                                                      redis.call('ZADD', '{surefire}:runs:created', run.createdAt, run_id)
                                                                      redis.call('ZADD', '{surefire}:job_runs:' .. run.jobName, run.createdAt, run_id)
                                                                      redis.call('ZADD', '{surefire}:status:' .. tostring(run.status), run.createdAt, run_id)
                                                                      redis.call('HINCRBY', '{surefire}:status_counts', tostring(run.status), 1)
                                                                      if is_terminal(run.status) then
                                                                          redis.call('ZADD', '{surefire}:runs:terminal', run.createdAt, run_id)
                                                                      else
                                                                          redis.call('ZADD', '{surefire}:runs:nonterminal', run.createdAt, run_id)
                                                                      end
                                                                      if run.parentRunId and run.parentRunId ~= cjson.null and run.parentRunId ~= '' then
                                                                          redis.call('ZADD', '{surefire}:children:' .. run.parentRunId, 0, child_member(run.createdAt, run_id))
                                                                      end
                                                                      if run.rootRunId and run.rootRunId ~= cjson.null and run.rootRunId ~= '' then
                                                                          redis.call('ZADD', '{surefire}:tree:' .. run.rootRunId, run.createdAt, run_id)
                                                                      end
                                                                      if run.batchId and run.batchId ~= cjson.null and run.batchId ~= '' then
                                                                          redis.call('ZADD', '{surefire}:batch_runs:' .. run.batchId, run.createdAt, run_id)
                                                                      end

                                                                      if run.status == 0 then
                                                                          add_to_pending(run, run_id, qname)
                                                                      elseif run.status == 1 then
                                                                          redis.call('SADD', '{surefire}:running:' .. run.jobName, run_id)
                                                                          redis.call('SADD', '{surefire}:running_q:' .. qname, run_id)
                                                                          local hb = (run.lastHeartbeatAt and run.lastHeartbeatAt ~= cjson.null) and tonumber(run.lastHeartbeatAt) or tonumber(run.createdAt)
                                                                          redis.call('ZADD', '{surefire}:heartbeat:running', hb, run_id)
                                                                          if run.nodeName and run.nodeName ~= cjson.null and run.nodeName ~= '' then
                                                                              redis.call('SADD', '{surefire}:node_runs:' .. run.nodeName, run_id)
                                                                          end
                                                                      end
                                                                      if run.status == 2 or run.status == 4 or run.status == 5 then
                                                                          redis.call('SREM', '{surefire}:nonterminal:' .. run.jobName, run_id)
                                                                          local ca = run.completedAt
                                                                          if ca and ca ~= cjson.null then
                                                                              redis.call('ZADD', '{surefire}:runs:completed', tonumber(ca), run_id)
                                                                              local minute = math.floor(tonumber(ca) / 60000)
                                                                              local field = timeline_field(run.status)
                                                                              if field then
                                                                                  redis.call('HINCRBY', '{surefire}:timeline:' .. tostring(minute), field, 1)
                                                                                  redis.call('ZADD', '{surefire}:timeline:index', minute, tostring(minute))
                                                                              end
                                                                          end
                                                                      end
                                                                  end
                                                                  append_events(initial_events)
                                                                  return 1
                                                                  """;

    // Bulk upsert. One Lua call regardless of jobs.Count. ARGV[1] is the JSON array produced by
    // UpsertPayloadFactory.SerializeJobs; ARGV[2] is the shared "now" (unix ms, as a string).
    // `tags` arrives as a JSON array and `retry_policy` as a JSON object — both round-trip back
    // to JSON text via cjson.encode so the on-hash representation matches the single-object
    // storage format used on read. is_enabled and last_cron_fire_at are read off the existing
    // hash and re-set to preserve the dashboard toggle and the last cron fire timestamp; on
    // first insert they take their input / empty-string defaults.
    private const string UpsertJobsScript = """
                                            local payload = cjson.decode(ARGV[1])
                                            local now = ARGV[2]

                                            local function s(v) if v == nil or v == cjson.null then return '' else return tostring(v) end end
                                            local function bit(v) if v then return '1' else return '0' end end
                                            local function tags_json(t)
                                                if type(t) ~= 'table' or #t == 0 then return '[]' end
                                                return cjson.encode(t)
                                            end

                                            for i = 1, #payload do
                                                local j = payload[i]
                                                local key = '{surefire}:job:' .. j.name
                                                local exists = redis.call('EXISTS', key)
                                                local tags = tags_json(j.tags)
                                                local retry = cjson.encode(j.retryPolicy)

                                                if exists == 1 then
                                                    local is_enabled = redis.call('HGET', key, 'is_enabled')
                                                    local last_cron_fire_at = redis.call('HGET', key, 'last_cron_fire_at')
                                                    redis.call('HSET', key,
                                                        'name', j.name,
                                                        'description', s(j.description),
                                                        'tags', tags,
                                                        'cron_expression', s(j.cronExpression),
                                                        'time_zone_id', s(j.timeZoneId),
                                                        'timeout', s(j.timeout),
                                                        'max_concurrency', s(j.maxConcurrency),
                                                        'priority', s(j.priority),
                                                        'retry_policy', retry,
                                                        'is_continuous', bit(j.isContinuous),
                                                        'queue', s(j.queue),
                                                        'rate_limit_name', s(j.rateLimitName),
                                                        'misfire_policy', s(j.misfirePolicy),
                                                        'fire_all_limit', s(j.fireAllLimit),
                                                        'arguments_schema', s(j.argumentsSchema),
                                                        'last_heartbeat_at', now,
                                                        'is_enabled', is_enabled,
                                                        'last_cron_fire_at', last_cron_fire_at or '')
                                                else
                                                    redis.call('HSET', key,
                                                        'name', j.name,
                                                        'description', s(j.description),
                                                        'tags', tags,
                                                        'cron_expression', s(j.cronExpression),
                                                        'time_zone_id', s(j.timeZoneId),
                                                        'timeout', s(j.timeout),
                                                        'max_concurrency', s(j.maxConcurrency),
                                                        'priority', s(j.priority),
                                                        'retry_policy', retry,
                                                        'is_continuous', bit(j.isContinuous),
                                                        'queue', s(j.queue),
                                                        'rate_limit_name', s(j.rateLimitName),
                                                        'is_enabled', bit(j.isEnabled),
                                                        'misfire_policy', s(j.misfirePolicy),
                                                        'fire_all_limit', s(j.fireAllLimit),
                                                        'arguments_schema', s(j.argumentsSchema),
                                                        'last_heartbeat_at', now,
                                                        'last_cron_fire_at', '')
                                                    redis.call('SADD', '{surefire}:jobs', j.name)
                                                end
                                            end
                                            return 1
                                            """;

    // Bulk upsert. One Lua call regardless of queues.Count. ARGV[1] is the JSON array payload;
    // ARGV[2] is the shared "now" (unix ms). is_paused is read off the existing hash so the
    // dashboard pause flag survives re-upserts; on first insert it takes the input value.
    private const string UpsertQueuesScript = """
                                              local payload = cjson.decode(ARGV[1])
                                              local now = ARGV[2]

                                              local function s(v) if v == nil or v == cjson.null then return '' else return tostring(v) end end
                                              local function bit(v) if v then return '1' else return '0' end end

                                              for i = 1, #payload do
                                                  local q = payload[i]
                                                  local key = '{surefire}:queue:' .. q.name
                                                  local exists = redis.call('EXISTS', key)
                                                  if exists == 1 then
                                                      local is_paused = redis.call('HGET', key, 'is_paused')
                                                      redis.call('HSET', key,
                                                          'name', q.name,
                                                          'priority', s(q.priority),
                                                          'max_concurrency', s(q.maxConcurrency),
                                                          'rate_limit_name', s(q.rateLimitName),
                                                          'last_heartbeat_at', now,
                                                          'is_paused', is_paused)
                                                  else
                                                      redis.call('HSET', key,
                                                          'name', q.name,
                                                          'priority', s(q.priority),
                                                          'max_concurrency', s(q.maxConcurrency),
                                                          'rate_limit_name', s(q.rateLimitName),
                                                          'last_heartbeat_at', now,
                                                          'is_paused', bit(q.isPaused))
                                                      redis.call('SADD', '{surefire}:queues', q.name)
                                                  end
                                              end
                                              return 1
                                              """;

    // Bulk upsert. One Lua call regardless of rateLimits.Count. Runtime counter fields
    // (current_count / previous_count / window_start) are only seeded on first insert and are
    // left alone on update so concurrent rate-limit acquisition is never rewound.
    private const string UpsertRateLimitsScript = """
                                                  local payload = cjson.decode(ARGV[1])
                                                  local now = ARGV[2]

                                                  local function s(v) if v == nil or v == cjson.null then return '' else return tostring(v) end end

                                                  for i = 1, #payload do
                                                      local r = payload[i]
                                                      local key = '{surefire}:rate_limit:' .. r.name
                                                      local exists = redis.call('EXISTS', key)
                                                      if exists == 1 then
                                                          redis.call('HSET', key,
                                                              'type', s(r.type),
                                                              'max_permits', s(r.maxPermits),
                                                              'window', s(r.window),
                                                              'last_heartbeat_at', now)
                                                      else
                                                          redis.call('HSET', key,
                                                              'name', r.name,
                                                              'type', s(r.type),
                                                              'max_permits', s(r.maxPermits),
                                                              'window', s(r.window),
                                                              'last_heartbeat_at', now,
                                                              'current_count', '0',
                                                              'previous_count', '0',
                                                              'window_start', '0')
                                                          redis.call('SADD', '{surefire}:rate_limits', r.name)
                                                      end
                                                  end
                                                  return 1
                                                  """;

    private static readonly RedisKey RoutingKey = new("{surefire}:");

    private readonly ConcurrentDictionary<string, byte[]> _scriptHashes = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _scriptLoadLock = new(1, 1);

    private IDatabase Db => connection.GetDatabase();

    public Task MigrateAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public async Task PingAsync(CancellationToken cancellationToken = default)
    {
        _ = cancellationToken;
        _ = await Db.PingAsync();
    }

    public async Task UpsertJobsAsync(IReadOnlyList<JobDefinition> jobs,
        CancellationToken cancellationToken = default)
    {
        if (jobs.Count == 0)
        {
            return;
        }

        var now = timeProvider.GetUtcNow().ToUnixTimeMilliseconds().ToString();
        await EvaluateScriptAsync(UpsertJobsScript,
            [RoutingKey],
            [UpsertPayloadFactory.SerializeJobs(jobs), now]);
    }

    public async Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        var hash = await Db.HashGetAllAsync($"{P}job:{name}");
        if (hash.Length == 0)
        {
            return null;
        }

        return HashToJob(hash);
    }

    public async Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null,
        CancellationToken cancellationToken = default)
    {
        var db = Db;
        var members = await db.SetMembersAsync($"{P}jobs");
        if (members.Length == 0)
        {
            return [];
        }

        var batch = db.CreateBatch();
        var tasks = members.Select(m => batch.HashGetAllAsync($"{P}job:{m}")).ToArray();
        batch.Execute();
        await Task.WhenAll(tasks);

        var results = new List<JobDefinition>();
        foreach (var task in tasks)
        {
            var hash = await task;
            if (hash.Length == 0)
            {
                continue;
            }

            var job = HashToJob(hash);

            if (filter?.Name is { } nameFilter &&
                !job.Name.Contains(nameFilter, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (filter?.Tag is { } tagFilter &&
                !job.Tags.Contains(tagFilter, StringComparer.OrdinalIgnoreCase))
            {
                continue;
            }

            if (filter?.IsEnabled is { } enabledFilter && job.IsEnabled != enabledFilter)
            {
                continue;
            }

            if (filter?.HeartbeatAfter is { } heartbeatFilter &&
                !(job.LastHeartbeatAt > heartbeatFilter))
            {
                continue;
            }

            results.Add(job);
        }

        results.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.Ordinal));
        return results;
    }

    public async Task SetJobEnabledAsync(string name, bool enabled, CancellationToken cancellationToken = default)
    {
        const string script = """
                              if redis.call('EXISTS', KEYS[1]) == 1 then
                                  redis.call('HSET', KEYS[1], 'is_enabled', ARGV[1])
                              end
                              return 1
                              """;

        await EvaluateScriptAsync(script,
            [new($"{P}job:{name}")],
            [enabled ? "1" : "0"]);
    }

    public async Task UpdateLastCronFireAtAsync(string jobName, DateTimeOffset fireAt,
        CancellationToken cancellationToken = default)
    {
        const string script = """
                              if redis.call('EXISTS', KEYS[1]) == 1 then
                                  redis.call('HSET', KEYS[1], 'last_cron_fire_at', ARGV[1])
                              end
                              return 1
                              """;

        await EvaluateScriptAsync(script,
            [new($"{P}job:{jobName}")],
            [fireAt.ToUnixTimeMilliseconds().ToString()]);
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
        var json = await Db.StringGetAsync($"{P}run:{id}");
        if (json.IsNullOrEmpty)
        {
            return null;
        }

        return DeserializeRun(json!);
    }

    public async Task<IReadOnlyList<JobRun>> GetRunsByIdsAsync(IReadOnlyList<string> ids,
        CancellationToken cancellationToken = default)
    {
        if (ids.Count == 0)
        {
            return [];
        }

        var keys = new RedisKey[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            keys[i] = $"{P}run:{ids[i]}";
        }

        var payloads = await Db.StringGetAsync(keys);
        var runs = new List<JobRun>(payloads.Length);
        for (var i = 0; i < payloads.Length; i++)
        {
            if (payloads[i].IsNullOrEmpty)
            {
                continue;
            }

            runs.Add(DeserializeRun(payloads[i]!));
        }

        return runs;
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
        var db = Db;
        var indexKey = $"{P}children:{parentRunId}";

        // Children index: ZSET score = 0, member = "{created_at_ms:D20}{runId}".
        // This yields deterministic lex ordering by (CreatedAtMs ASC, Id ASC) without
        // tie-bucket stitching. Cursor bounds are strict and exclusive.
        var min = RedisValue.Null;
        var max = RedisValue.Null;
        var exclude = Exclude.None;
        var order = Order.Ascending;

        if (before is { } b)
        {
            max = EncodeChildMember(b.CreatedAt, b.Id);
            exclude = Exclude.Stop;
            order = Order.Descending;
        }
        else if (after is { } a)
        {
            min = EncodeChildMember(a.CreatedAt, a.Id);
            exclude = Exclude.Start;
            order = Order.Ascending;
        }

        var members = await db.SortedSetRangeByValueAsync(indexKey,
            min,
            max,
            exclude,
            order,
            take: (long)take + 1);

        if (members.Length == 0)
        {
            return new() { Items = [], NextCursor = null };
        }

        var hasMore = members.Length > take;
        var pageMembers = hasMore ? members[..take] : members;

        var refs = new (long CreatedAtMs, string RunId)[pageMembers.Length];
        var keys = new RedisKey[pageMembers.Length];
        for (var i = 0; i < pageMembers.Length; i++)
        {
            if (!TryDecodeChildMember(pageMembers[i], out var createdAtMs, out var runId))
            {
                throw new FormatException($"Malformed child index member: '{pageMembers[i]}'.");
            }

            refs[i] = (createdAtMs, runId);
            keys[i] = $"{P}run:{runId}";
        }

        var payloads = await db.StringGetAsync(keys);
        var items = new List<JobRun>(payloads.Length);
        foreach (var payload in payloads)
        {
            if (payload.IsNullOrEmpty)
            {
                continue;
            }

            items.Add(DeserializeRun(payload!));
        }

        var nextCursor = hasMore
            ? DirectChildrenPage.EncodeCursor(
                DateTimeOffset.FromUnixTimeMilliseconds(refs[^1].CreatedAtMs),
                refs[^1].RunId)
            : null;

        return new() { Items = items, NextCursor = nextCursor };
    }

    public async Task<IReadOnlyList<JobRun>> GetAncestorChainAsync(string runId,
        CancellationToken cancellationToken = default)
    {
        // Iterative parent walk. Each GET is one round trip; trees are typically shallow
        // (< 10 levels for business workloads), so this is bounded and simple.
        var db = Db;
        var chain = new List<JobRun>();
        var visited = new HashSet<string>(StringComparer.Ordinal) { runId };

        var currentRun = await GetRunAsync(runId, cancellationToken);
        if (currentRun is null)
        {
            return chain;
        }

        // Parent IDs are immutable after creation; `visited` terminates any pathological
        // cycle even though the data model makes them impossible. No length cap: a
        // legitimately deep hierarchy should surface in full.
        while (currentRun.ParentRunId is { } parentId && visited.Add(parentId))
        {
            var parent = await GetRunAsync(parentId, cancellationToken);
            if (parent is null)
            {
                break;
            }

            chain.Add(parent);
            currentRun = parent;
        }

        chain.Reverse();
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

        var db = Db;

        RedisValue[] candidateIds;
        int? totalFromIndex = null;
        var paginatedAtIndex = false;

        if (filter.ParentRunId is { } parentId)
        {
            var key = $"{P}children:{parentId}";
            var (lexMin, lexMax, lexExclude) = GetChildrenLexWindow(filter);
            if (NoAdditionalFiltersExceptParentRunId(filter))
            {
                totalFromIndex = (int)await GetSortedSetLexCountAsync(key, lexMin, lexMax, lexExclude);
                var members = await db.SortedSetRangeByValueAsync(key,
                    lexMin,
                    lexMax,
                    lexExclude,
                    Order.Descending,
                    skip,
                    take);
                var ids = new RedisValue[members.Length];
                for (var i = 0; i < members.Length; i++)
                {
                    if (!TryDecodeChildMember(members[i], out _, out var runId))
                    {
                        throw new FormatException($"Malformed child index member: '{members[i]}'.");
                    }

                    ids[i] = runId;
                }

                candidateIds = ids;
                paginatedAtIndex = true;
            }
            else
            {
                return await GetRunsFromChildrenLexIndexWithFilterAsync(key, filter, skip, take,
                    lexMin,
                    lexMax,
                    lexExclude,
                    cancellationToken);
            }
        }
        else if (filter.RootRunId is { } rootId)
        {
            var key = $"{P}tree:{rootId}";
            var (minScore, maxScore, exclude) = GetCreatedScoreWindow(filter);
            if (NoAdditionalFiltersExceptRootRunId(filter))
            {
                totalFromIndex = (int)await db.SortedSetLengthAsync(key, minScore, maxScore, exclude);
                candidateIds = await db.SortedSetRangeByScoreAsync(key, minScore, maxScore, exclude,
                    Order.Descending, skip, take);
                paginatedAtIndex = true;
            }
            else
            {
                return await GetRunsFromSortedSetWithFilterAsync(key, filter, skip, take,
                    RunOrderBy.CreatedAt, minScore, maxScore, exclude,
                    Order.Descending, cancellationToken);
            }
        }
        else if (filter.BatchId is { } batchId)
        {
            var key = $"{P}batch_runs:{batchId}";
            var (minScore, maxScore, exclude) = GetCreatedScoreWindow(filter);
            if (NoAdditionalFiltersExceptBatchId(filter))
            {
                totalFromIndex = (int)await db.SortedSetLengthAsync(key, minScore, maxScore, exclude);
                candidateIds = await db.SortedSetRangeByScoreAsync(key, minScore, maxScore, exclude,
                    Order.Descending, skip, take);
                paginatedAtIndex = true;
            }
            else
            {
                return await GetRunsFromSortedSetWithFilterAsync(key, filter, skip, take,
                    RunOrderBy.CreatedAt, minScore, maxScore, exclude,
                    Order.Descending, cancellationToken);
            }
        }
        else if (filter.Status is { } status)
        {
            var (minScore, maxScore, exclude) = GetCreatedScoreWindow(filter);
            if (NoAdditionalFiltersExceptStatus(filter))
            {
                totalFromIndex = (int)await db.SortedSetLengthAsync($"{P}status:{(int)status}", minScore, maxScore,
                    exclude);
                candidateIds = await db.SortedSetRangeByScoreAsync(
                    $"{P}status:{(int)status}",
                    minScore,
                    maxScore,
                    exclude,
                    Order.Descending,
                    skip,
                    take);
                paginatedAtIndex = true;
            }
            else
            {
                return await GetRunsFromSortedSetWithFilterAsync(
                    $"{P}status:{(int)status}",
                    filter,
                    skip,
                    take,
                    RunOrderBy.CreatedAt,
                    minScore,
                    maxScore,
                    exclude,
                    Order.Descending,
                    cancellationToken);
            }
        }
        else if (filter.IsTerminal == true)
        {
            var (minScore, maxScore, exclude) = GetCreatedScoreWindow(filter);
            var noPostFilters = filter.JobName is null && filter.NodeName is null
                                                       && filter.ParentRunId is null && filter.RootRunId is null
                                                       && filter.CompletedAfter is null &&
                                                       filter.LastHeartbeatBefore is null
                                                       && filter.OrderBy == RunOrderBy.CreatedAt;

            if (noPostFilters)
            {
                totalFromIndex = (int)await db.SortedSetLengthAsync($"{P}runs:terminal", minScore, maxScore,
                    exclude);
                candidateIds = await db.SortedSetRangeByScoreAsync($"{P}runs:terminal",
                    minScore,
                    maxScore,
                    exclude,
                    Order.Descending,
                    skip,
                    take);
                paginatedAtIndex = true;
            }
            else
            {
                return await GetRunsFromSortedSetWithFilterAsync(
                    $"{P}runs:terminal",
                    filter,
                    skip,
                    take,
                    RunOrderBy.CreatedAt,
                    minScore,
                    maxScore,
                    exclude,
                    Order.Descending,
                    cancellationToken);
            }
        }
        else if (filter.IsTerminal == false)
        {
            var (minScore, maxScore, exclude) = GetCreatedScoreWindow(filter);
            var noPostFilters = filter.JobName is null && filter.NodeName is null
                                                       && filter.ParentRunId is null && filter.RootRunId is null
                                                       && filter.CompletedAfter is null &&
                                                       filter.LastHeartbeatBefore is null
                                                       && filter.OrderBy == RunOrderBy.CreatedAt;

            if (noPostFilters)
            {
                totalFromIndex = (int)await db.SortedSetLengthAsync($"{P}runs:nonterminal", minScore, maxScore,
                    exclude);
                candidateIds = await db.SortedSetRangeByScoreAsync($"{P}runs:nonterminal",
                    minScore,
                    maxScore,
                    exclude,
                    Order.Descending,
                    skip,
                    take);
                paginatedAtIndex = true;
            }
            else
            {
                return await GetRunsFromSortedSetWithFilterAsync(
                    $"{P}runs:nonterminal",
                    filter,
                    skip,
                    take,
                    RunOrderBy.CreatedAt,
                    minScore,
                    maxScore,
                    exclude,
                    Order.Descending,
                    cancellationToken);
            }
        }
        else if (filter.JobName is { } jobName && filter.ExactJobName)
        {
            var (minScore, maxScore, exclude) = GetCreatedScoreWindow(filter);
            if (NoAdditionalFiltersExceptExactJobName(filter))
            {
                totalFromIndex = (int)await db.SortedSetLengthAsync($"{P}job_runs:{jobName}", minScore, maxScore,
                    exclude);
                candidateIds = await db.SortedSetRangeByScoreAsync(
                    $"{P}job_runs:{jobName}",
                    minScore,
                    maxScore,
                    exclude,
                    Order.Descending,
                    skip,
                    take);
                paginatedAtIndex = true;
            }
            else
            {
                return await GetRunsFromSortedSetWithFilterAsync(
                    $"{P}job_runs:{jobName}",
                    filter,
                    skip,
                    take,
                    RunOrderBy.CreatedAt,
                    minScore,
                    maxScore,
                    exclude,
                    Order.Descending,
                    cancellationToken);
            }
        }
        else if (filter.OrderBy == RunOrderBy.CompletedAt && filter.CompletedAfter is { } completedAfter)
        {
            return await GetRunsFromSortedSetWithFilterAsync(
                $"{P}runs:completed",
                filter,
                skip,
                take,
                RunOrderBy.CompletedAt,
                completedAfter.ToUnixTimeMilliseconds(),
                double.PositiveInfinity,
                Exclude.Start,
                Order.Descending,
                cancellationToken);
        }
        else
        {
            var minScore = filter.CreatedAfter?.ToUnixTimeMilliseconds() ?? double.NegativeInfinity;
            var maxScore = filter.CreatedBefore?.ToUnixTimeMilliseconds() ?? double.PositiveInfinity;
            var exclude = Exclude.None;
            if (filter.CreatedAfter is { })
            {
                exclude |= Exclude.Start;
            }

            if (filter.CreatedBefore is { })
            {
                exclude |= Exclude.Stop;
            }

            if (NoAdditionalFilters(filter))
            {
                totalFromIndex = (int)await db.SortedSetLengthAsync($"{P}runs:created", minScore, maxScore, exclude);
                candidateIds = await db.SortedSetRangeByScoreAsync($"{P}runs:created",
                    minScore, maxScore, exclude, Order.Descending, skip, take);
                paginatedAtIndex = true;
            }
            else
            {
                return await GetRunsFromSortedSetWithFilterAsync(
                    $"{P}runs:created",
                    filter,
                    skip,
                    take,
                    RunOrderBy.CreatedAt,
                    minScore,
                    maxScore,
                    exclude,
                    Order.Descending,
                    cancellationToken);
            }
        }

        if (candidateIds.Length == 0)
        {
            return new() { Items = [], TotalCount = totalFromIndex ?? 0 };
        }

        var batch = db.CreateBatch();
        var fetchTasks = candidateIds.Select(id => batch.StringGetAsync($"{P}run:{id}")).ToArray();
        batch.Execute();
        await Task.WhenAll(fetchTasks);

        var runs = new List<JobRun>();
        foreach (var task in fetchTasks)
        {
            var json = await task;
            if (json.IsNullOrEmpty)
            {
                continue;
            }

            var run = DeserializeRun(json!);
            if (MatchesFilter(run, filter))
            {
                runs.Add(run);
            }
        }

        runs = SortRuns(runs, filter.OrderBy);

        if (paginatedAtIndex && totalFromIndex.HasValue)
        {
            return new() { Items = runs, TotalCount = totalFromIndex.Value };
        }

        var totalCount = totalFromIndex ?? runs.Count;
        var page = runs.Skip(skip).Take(take).ToList();
        return new() { Items = page, TotalCount = totalCount };
    }

    public async Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        const string script = """
                              local function s(v) return (v ~= nil and v ~= cjson.null) and v or '' end
                              local key = '{surefire}:run:' .. ARGV[1]
                              local data = redis.call('GET', key)
                              if not data then return 0 end

                              local r = cjson.decode(data)
                              if r.status == 2 or r.status == 4 or r.status == 5 then return 0 end
                              if s(r.nodeName) ~= ARGV[2] then return 0 end

                              r.progress = tonumber(ARGV[3])
                              r.result = ARGV[4] ~= '' and ARGV[4] or nil
                              r.reason =ARGV[5] ~= '' and ARGV[5] or nil
                              r.traceId = ARGV[6] ~= '' and ARGV[6] or nil
                              r.spanId = ARGV[7] ~= '' and ARGV[7] or nil
                              r.lastHeartbeatAt = ARGV[8] ~= '' and tonumber(ARGV[8]) or nil

                              redis.call('SET', key, cjson.encode(r))
                              return 1
                              """;

        await EvaluateScriptAsync(script,
            [RoutingKey],
            [
                run.Id,
                run.NodeName ?? "",
                run.Progress.ToString(),
                run.Result ?? "",
                run.Reason ?? "",
                run.TraceId ?? "",
                run.SpanId ?? "",
                run.LastHeartbeatAt?.ToUnixTimeMilliseconds().ToString() ?? ""
            ]);
    }

    public async Task<RunTransitionResult> TryTransitionRunAsync(RunStatusTransition transition,
        CancellationToken cancellationToken = default)
    {
        if (!RunTransitionRules.IsAllowed(transition.ExpectedStatus, transition.NewStatus)
            || !transition.HasRequiredFields())
        {
            return RunTransitionResult.NotApplied;
        }

        const string script = PendingMemberHelper + """
                                                    local function s(v) return (v ~= nil and v ~= cjson.null) and v or '' end
                                                    local function is_active(status)
                                                        return status == 1
                                                    end
                                                    local function append_status_event(run)
                                                        local id = redis.call('INCR', '{surefire}:event_seq')
                                                        local evt = {
                                                            id = id,
                                                            runId = run.id,
                                                            eventType = 0,
                                                            payload = tostring(run.status),
                                                            createdAt = tonumber(ARGV[14]),
                                                            attempt = run.attempt
                                                        }
                                                        local payload = cjson.encode(evt)
                                                        local id_str = tostring(id)
                                                        redis.call('SET', '{surefire}:event:' .. id_str, payload)
                                                        redis.call('ZADD', '{surefire}:events:run:' .. run.id, id, id_str)
                                                        redis.call('ZADD', '{surefire}:events:run_type:' .. run.id .. ':0', id, id_str)
                                                        redis.call('ZADD', '{surefire}:events:run_attempt:' .. run.id .. ':' .. tostring(run.attempt), id, id_str)
                                                        redis.call('SADD', '{surefire}:events:run_types:' .. run.id, '0')
                                                        redis.call('SADD', '{surefire}:events:run_attempts:' .. run.id, tostring(run.attempt))
                                                        if run.batchId and run.batchId ~= cjson.null and run.batchId ~= '' then
                                                            redis.call('ZADD', '{surefire}:events:batch:' .. run.batchId, id, id_str)
                                                        end
                                                    end
                                                    local id = ARGV[1]
                                                    local key = '{surefire}:run:' .. id
                                                    local data = redis.call('GET', key)
                                                    if not data then return 0 end

                                                    local r = cjson.decode(data)
                                                    if r.status ~= tonumber(ARGV[2]) then return 0 end
                                                    if r.attempt ~= tonumber(ARGV[3]) then return 0 end
                                                    if r.status == 2 or r.status == 4 or r.status == 5 then return 0 end

                                                    local old_status = r.status
                                                    local old_node = s(r.nodeName)
                                                    local new_status = tonumber(ARGV[4])
                                                    local was_terminal = old_status == 2 or old_status == 4 or old_status == 5
                                                    local is_terminal = new_status == 2 or new_status == 4 or new_status == 5
                                                    r.status = new_status
                                                    r.nodeName = ARGV[5] ~= '' and ARGV[5] or nil
                                                    r.startedAt = ARGV[6] ~= '' and tonumber(ARGV[6]) or r.startedAt
                                                    r.completedAt = ARGV[7] ~= '' and tonumber(ARGV[7]) or r.completedAt
                                                    r.cancelledAt = ARGV[8] ~= '' and tonumber(ARGV[8]) or r.cancelledAt
                                                    r.reason =ARGV[9] ~= '' and ARGV[9] or nil
                                                    r.result = ARGV[10] ~= '' and ARGV[10] or nil
                                                    r.progress = tonumber(ARGV[11])
                                                    r.notBefore = tonumber(ARGV[12])
                                                    r.lastHeartbeatAt = ARGV[13] ~= '' and tonumber(ARGV[13]) or r.lastHeartbeatAt

                                                    redis.call('SET', key, cjson.encode(r))

                                                    -- Resolve queue name (used by multiple branches below)
                                                    local q_field = redis.call('HGET', '{surefire}:job:' .. r.jobName, 'queue')
                                                    local queue_name = (q_field and q_field ~= '') and q_field or 'default'

                                                    -- Remove from old indexes
                                                    if old_status == 0 then
                                                        local pm = redis.call('GET', '{surefire}:pending_member:' .. id)
                                                        if pm then
                                                            redis.call('ZREM', '{surefire}:pending_q:' .. queue_name, pm)
                                                            redis.call('DEL', '{surefire}:pending_member:' .. id)
                                                        end
                                                    end
                                                    if is_active(old_status) then
                                                        redis.call('SREM', '{surefire}:running:' .. r.jobName, id)
                                                        redis.call('SREM', '{surefire}:running_q:' .. queue_name, id)
                                                        redis.call('ZREM', '{surefire}:heartbeat:running', id)
                                                        if old_node ~= '' then
                                                            redis.call('SREM', '{surefire}:node_runs:' .. old_node, id)
                                                        end
                                                    end

                                                    -- Status index maintenance
                                                    redis.call('ZREM', '{surefire}:status:' .. tostring(old_status), id)
                                                    redis.call('ZADD', '{surefire}:status:' .. tostring(new_status), r.createdAt, id)
                                                    redis.call('HINCRBY', '{surefire}:status_counts', tostring(old_status), -1)
                                                    redis.call('HINCRBY', '{surefire}:status_counts', tostring(new_status), 1)
                                                    redis.call('ZREM', '{surefire}:runs:terminal', id)
                                                    redis.call('ZREM', '{surefire}:runs:nonterminal', id)
                                                    if new_status == 2 or new_status == 4 or new_status == 5 then
                                                        redis.call('ZADD', '{surefire}:runs:terminal', r.createdAt, id)
                                                    else
                                                        redis.call('ZADD', '{surefire}:runs:nonterminal', r.createdAt, id)
                                                    end

                                                    -- Add to new indexes
                                                    if new_status == 0 then
                                                        local member = pending_member(r.priority, r.notBefore, id)
                                                        redis.call('ZADD', '{surefire}:pending_q:' .. queue_name, 0, member)
                                                        redis.call('SET', '{surefire}:pending_member:' .. id, member)
                                                    end
                                                    if is_active(new_status) then
                                                        redis.call('SADD', '{surefire}:running:' .. r.jobName, id)
                                                        redis.call('SADD', '{surefire}:running_q:' .. queue_name, id)
                                                        local hb = (r.lastHeartbeatAt and r.lastHeartbeatAt ~= cjson.null) and tonumber(r.lastHeartbeatAt) or tonumber(r.createdAt)
                                                        redis.call('ZADD', '{surefire}:heartbeat:running', hb, id)
                                                        if r.startedAt and r.startedAt ~= cjson.null then
                                                            redis.call('ZADD', '{surefire}:job_started:' .. r.jobName, tonumber(r.startedAt), id)
                                                        end
                                                        local new_node = s(r.nodeName)
                                                        if new_node ~= '' then
                                                            if old_node ~= '' and old_node ~= new_node then
                                                                redis.call('SREM', '{surefire}:node_runs:' .. old_node, id)
                                                            end
                                                            redis.call('SADD', '{surefire}:node_runs:' .. new_node, id)
                                                        end
                                                    end

                                                    -- Terminal transition: update secondary indexes
                                                    if new_status == 2 or new_status == 4 or new_status == 5 then
                                                        if r.deduplicationId and r.deduplicationId ~= cjson.null and r.deduplicationId ~= '' then
                                                            redis.call('DEL', '{surefire}:dedup:' .. r.jobName .. ':' .. r.deduplicationId)
                                                        end
                                                        redis.call('SREM', '{surefire}:nonterminal:' .. r.jobName, id)
                                                        if old_node ~= '' then
                                                            redis.call('SREM', '{surefire}:node_runs:' .. old_node, id)
                                                        end
                                                        local ca = r.completedAt
                                                        if ca then
                                                            redis.call('ZADD', '{surefire}:runs:completed', tonumber(ca), id)
                                                            local minute = math.floor(tonumber(ca) / 60000)
                                                            local field = new_status == 2 and 'completed' or (new_status == 4 and 'cancelled' or 'failed')
                                                            redis.call('HINCRBY', '{surefire}:timeline:' .. tostring(minute), field, 1)
                                                            redis.call('ZADD', '{surefire}:timeline:index', minute, tostring(minute))
                                                        end
                                                        redis.call('ZREM', '{surefire}:expiring', id)
                                                    end

                                                    if (not was_terminal) and is_terminal then
                                                        local stats_key = '{surefire}:job_stats:' .. r.jobName
                                                        redis.call('HINCRBY', stats_key, 'terminal_runs', 1)
                                                        if new_status == 2 then
                                                            redis.call('HINCRBY', stats_key, 'succeeded_runs', 1)
                                                            if r.startedAt and r.startedAt ~= cjson.null and r.completedAt and r.completedAt ~= cjson.null then
                                                                redis.call('HINCRBYFLOAT', stats_key, 'duration_sum_ms', tonumber(r.completedAt) - tonumber(r.startedAt))
                                                                redis.call('HINCRBY', stats_key, 'duration_count', 1)
                                                            end
                                                        elseif new_status == 5 then
                                                            redis.call('HINCRBY', stats_key, 'failed_runs', 1)
                                                        end
                                                    end

                                                    append_status_event(r)

                                                    -- Append caller-provided events (ARGV[16])
                                                    if ARGV[16] and ARGV[16] ~= '' then
                                                        local caller_events = cjson.decode(ARGV[16])
                                                        for _, evt in ipairs(caller_events) do
                                                            local eid = redis.call('INCR', '{surefire}:event_seq')
                                                            evt.id = eid
                                                            local epayload = cjson.encode(evt)
                                                            local eid_str = tostring(eid)
                                                            redis.call('SET', '{surefire}:event:' .. eid_str, epayload)
                                                            redis.call('ZADD', '{surefire}:events:run:' .. evt.runId, eid, eid_str)
                                                            redis.call('ZADD', '{surefire}:events:run_type:' .. evt.runId .. ':' .. tostring(evt.eventType), eid, eid_str)
                                                            redis.call('ZADD', '{surefire}:events:run_attempt:' .. evt.runId .. ':' .. tostring(evt.attempt), eid, eid_str)
                                                            redis.call('SADD', '{surefire}:events:run_types:' .. evt.runId, tostring(evt.eventType))
                                                            redis.call('SADD', '{surefire}:events:run_attempts:' .. evt.runId, tostring(evt.attempt))
                                                            if r.batchId and r.batchId ~= cjson.null and r.batchId ~= '' then
                                                                redis.call('ZADD', '{surefire}:events:batch:' .. r.batchId, eid, eid_str)
                                                                if evt.eventType == 3 then
                                                                    redis.call('ZADD', '{surefire}:events:batch_output:' .. r.batchId, eid, eid_str)
                                                                end
                                                            end
                                                        end
                                                    end

                                                    -- Atomic batch counter increment for terminal transitions
                                                    if is_terminal and r.batchId and r.batchId ~= cjson.null and r.batchId ~= '' then
                                                        local batch_key = '{surefire}:batch:' .. r.batchId
                                                        local batch_data = redis.call('GET', batch_key)
                                                        if batch_data then
                                                            local b = cjson.decode(batch_data)
                                                            if b.status ~= 2 and b.status ~= 4 and b.status ~= 5 then
                                                                if new_status == 2 then
                                                                    b.succeeded = (b.succeeded or 0) + 1
                                                                elseif new_status == 5 then
                                                                    b.failed = (b.failed or 0) + 1
                                                                elseif new_status == 4 then
                                                                    b.cancelled = (b.cancelled or 0) + 1
                                                                end
                                                                local total_done = (b.succeeded or 0) + (b.failed or 0) + (b.cancelled or 0)
                                                                if total_done >= b.total then
                                                                    local batch_status
                                                                    if (b.failed or 0) > 0 then
                                                                        batch_status = 5
                                                                    elseif (b.cancelled or 0) > 0 then
                                                                        batch_status = 4
                                                                    else
                                                                        batch_status = 2
                                                                    end
                                                                    local completed_at_ms = tonumber(ARGV[15])
                                                                    b.status = batch_status
                                                                    b.completedAt = completed_at_ms
                                                                    redis.call('SET', batch_key, cjson.encode(b))
                                                                    redis.call('SREM', '{surefire}:batches:active', r.batchId)
                                                                    redis.call('ZADD', '{surefire}:batches', completed_at_ms, r.batchId)
                                                                    return cjson.encode({batchId = r.batchId, batchStatus = batch_status, completedAt = completed_at_ms})
                                                                else
                                                                    redis.call('SET', batch_key, cjson.encode(b))
                                                                end
                                                            end
                                                        end
                                                    end

                                                    return 1
                                                    """;

        var now = timeProvider.GetUtcNow();
        var eventsJson = "";
        if (transition.Events is { Count: > 0 })
        {
            eventsJson = JsonSerializer.Serialize(
                new(transition.Events),
                SurefireJsonContext.Default.ListRunEvent);
        }

        var result = await EvaluateScriptAsync(script,
            [RoutingKey],
            [
                transition.RunId,
                ((int)transition.ExpectedStatus).ToString(),
                transition.ExpectedAttempt.ToString(),
                ((int)transition.NewStatus).ToString(),
                transition.NodeName ?? "",
                transition.StartedAt?.ToUnixTimeMilliseconds().ToString() ?? "",
                transition.CompletedAt?.ToUnixTimeMilliseconds().ToString() ?? "",
                transition.CancelledAt?.ToUnixTimeMilliseconds().ToString() ?? "",
                transition.Reason ?? "",
                transition.Result ?? "",
                transition.Progress.ToString(),
                transition.NotBefore.ToUnixTimeMilliseconds().ToString(),
                transition.LastHeartbeatAt?.ToUnixTimeMilliseconds().ToString() ?? "",
                now.ToUnixTimeMilliseconds().ToString(),
                now.ToUnixTimeMilliseconds().ToString(),
                eventsJson
            ]);

        if (result.IsNull || result.Resp2Type == ResultType.Error)
        {
            return RunTransitionResult.NotApplied;
        }

        var str = result.ToString();
        if (str == "0")
        {
            return RunTransitionResult.NotApplied;
        }

        if (str == "1")
        {
            return RunTransitionResult.Applied;
        }

        var payload = JsonSerializer.Deserialize(str!, SurefireJsonContext.Default.BatchCompletionPayload)
                      ?? throw new InvalidOperationException("Batch completion payload was null.");
        return new(true, new(payload.BatchId, (JobStatus)payload.BatchStatus, payload.CompletedAt));
    }

    public async Task<RunTransitionResult> TryCancelRunAsync(string runId,
        int? expectedAttempt = null,
        string? reason = null,
        IReadOnlyList<RunEvent>? events = null,
        CancellationToken cancellationToken = default)
    {
        const string script = """
                              local t = redis.call('TIME')
                              local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
                              local function append_event(evt, batch_id)
                                  local id = redis.call('INCR', '{surefire}:event_seq')
                                  evt.id = id
                                  local payload = cjson.encode(evt)
                                  local id_str = tostring(id)
                                  redis.call('SET', '{surefire}:event:' .. id_str, payload)
                                  redis.call('ZADD', '{surefire}:events:run:' .. evt.runId, id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_type:' .. evt.runId .. ':' .. tostring(evt.eventType), id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_attempt:' .. evt.runId .. ':' .. tostring(evt.attempt), id, id_str)
                                  redis.call('SADD', '{surefire}:events:run_types:' .. evt.runId, tostring(evt.eventType))
                                  redis.call('SADD', '{surefire}:events:run_attempts:' .. evt.runId, tostring(evt.attempt))
                                  if batch_id and batch_id ~= cjson.null and batch_id ~= '' then
                                      redis.call('ZADD', '{surefire}:events:batch:' .. batch_id, id, id_str)
                                      if evt.eventType == 3 then
                                          redis.call('ZADD', '{surefire}:events:batch_output:' .. batch_id, id, id_str)
                                      end
                                  end
                              end
                              local id = ARGV[1]
                              local expected_attempt = ARGV[2] ~= '' and tonumber(ARGV[2]) or nil
                              local err = ARGV[3] ~= '' and ARGV[3] or nil
                              local caller_events_json = ARGV[4]
                              local key = '{surefire}:run:' .. id
                              local data = redis.call('GET', key)
                              if not data then return '0' end

                              local r = cjson.decode(data)
                              if r.status == 2 or r.status == 4 or r.status == 5 then return '0' end
                              if expected_attempt and r.attempt ~= expected_attempt then return '0' end

                              local old_status = r.status
                              r.status = 4
                              r.cancelledAt = now_ms
                              r.completedAt = now_ms
                              if err then r.reason =err end
                              redis.call('SET', key, cjson.encode(r))

                              -- Resolve queue name
                              local q_field = redis.call('HGET', '{surefire}:job:' .. r.jobName, 'queue')
                              local queue_name = (q_field and q_field ~= '') and q_field or 'default'

                              if old_status == 0 then
                                  local pm = redis.call('GET', '{surefire}:pending_member:' .. id)
                                  if pm then
                                      redis.call('ZREM', '{surefire}:pending_q:' .. queue_name, pm)
                                      redis.call('DEL', '{surefire}:pending_member:' .. id)
                                  end
                              elseif old_status == 1 then
                                  redis.call('SREM', '{surefire}:running:' .. r.jobName, id)
                                  redis.call('SREM', '{surefire}:running_q:' .. queue_name, id)
                                  redis.call('ZREM', '{surefire}:heartbeat:running', id)
                                  local old_node = r.nodeName
                                  if old_node and old_node ~= cjson.null and old_node ~= '' then
                                      redis.call('SREM', '{surefire}:node_runs:' .. old_node, id)
                                  end
                              end

                              -- Status index maintenance
                              redis.call('ZREM', '{surefire}:status:' .. tostring(old_status), id)
                              redis.call('ZADD', '{surefire}:status:4', r.createdAt, id)
                              redis.call('HINCRBY', '{surefire}:status_counts', tostring(old_status), -1)
                              redis.call('HINCRBY', '{surefire}:status_counts', '4', 1)
                              redis.call('ZREM', '{surefire}:runs:nonterminal', id)
                              redis.call('ZADD', '{surefire}:runs:terminal', r.createdAt, id)
                              if r.deduplicationId and r.deduplicationId ~= cjson.null and r.deduplicationId ~= '' then
                                  redis.call('DEL', '{surefire}:dedup:' .. r.jobName .. ':' .. r.deduplicationId)
                              end
                              redis.call('SREM', '{surefire}:nonterminal:' .. r.jobName, id)
                              redis.call('HINCRBY', '{surefire}:job_stats:' .. r.jobName, 'terminal_runs', 1)
                              redis.call('ZADD', '{surefire}:runs:completed', now_ms, id)
                              local minute = math.floor(now_ms / 60000)
                              redis.call('HINCRBY', '{surefire}:timeline:' .. tostring(minute), 'cancelled', 1)
                              redis.call('ZADD', '{surefire}:timeline:index', minute, tostring(minute))
                              redis.call('ZREM', '{surefire}:expiring', id)

                              -- Status event
                              local batch_id = r.batchId
                              append_event({runId = id, eventType = 0, payload = '4', createdAt = now_ms, attempt = r.attempt}, batch_id)

                              -- Caller events
                              if caller_events_json and caller_events_json ~= '' then
                                  local caller_events = cjson.decode(caller_events_json)
                                  for _, evt in ipairs(caller_events) do
                                      append_event(evt, batch_id)
                                  end
                              end

                              -- Batch counter update
                              if batch_id and batch_id ~= cjson.null and batch_id ~= '' then
                                  local batch_key = '{surefire}:batch:' .. batch_id
                                  local batch_data = redis.call('GET', batch_key)
                                  if batch_data then
                                      local b = cjson.decode(batch_data)
                                      if b.status ~= 2 and b.status ~= 4 and b.status ~= 5 then
                                          b.cancelled = (b.cancelled or 0) + 1
                                          local total_done = (b.succeeded or 0) + (b.failed or 0) + (b.cancelled or 0)
                                          if total_done >= b.total then
                                              local batch_status
                                              if (b.failed or 0) > 0 then batch_status = 5
                                              elseif (b.cancelled or 0) > 0 then batch_status = 4
                                              else batch_status = 2 end
                                              b.status = batch_status
                                              b.completedAt = now_ms
                                              redis.call('SET', batch_key, cjson.encode(b))
                                              redis.call('SREM', '{surefire}:batches:active', batch_id)
                                              redis.call('ZADD', '{surefire}:batches', now_ms, batch_id)
                                              return cjson.encode({batchId = batch_id, batchStatus = batch_status, completedAt = now_ms})
                                          else
                                              redis.call('SET', batch_key, cjson.encode(b))
                                          end
                                      end
                                  end
                              end

                              return '1'
                              """;

        var eventsJson = "";
        if (events is { Count: > 0 })
        {
            eventsJson = JsonSerializer.Serialize(
                new(events),
                SurefireJsonContext.Default.ListRunEvent);
        }

        var result = await EvaluateScriptAsync(script,
            [RoutingKey],
            [
                runId,
                expectedAttempt?.ToString() ?? "",
                reason ?? "",
                eventsJson
            ]);

        if (result.IsNull || result.Resp2Type == ResultType.Error)
        {
            return RunTransitionResult.NotApplied;
        }

        var str = result.ToString();
        if (str == "0")
        {
            return RunTransitionResult.NotApplied;
        }

        if (str == "1")
        {
            return RunTransitionResult.Applied;
        }

        var payload = JsonSerializer.Deserialize(str!, SurefireJsonContext.Default.BatchCompletionPayload)
                      ?? throw new InvalidOperationException("Batch completion payload was null.");
        return new(true, new(payload.BatchId, (JobStatus)payload.BatchStatus, payload.CompletedAt));
    }

    public async Task<IReadOnlyList<JobRun>> ClaimRunsAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, int maxCount, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCount, 1);

        if (jobNames.Count == 0 || queueNames.Count == 0)
        {
            return Array.Empty<JobRun>();
        }

        // ARGV[1]=job_names_json, ARGV[2]=queue_names_json, ARGV[3]=node_name, ARGV[4]=now_ms, ARGV[5]=max_count
        const string fullScript = """
                                  local function _check_rate_limit(rl_name, now_ms)
                                      local rl_data = redis.call('HGETALL', '{surefire}:rate_limit:' .. rl_name)
                                      if #rl_data == 0 then return true end
                                      local rl = {}
                                      for i = 1, #rl_data, 2 do rl[rl_data[i]] = rl_data[i+1] end
                                      local max_permits = tonumber(rl.max_permits)
                                      local window_ticks = tonumber(rl.window)
                                      local window_ms = window_ticks / 10000
                                      local current_count = tonumber(rl.current_count) or 0
                                      local previous_count = tonumber(rl.previous_count) or 0
                                      local window_start = tonumber(rl.window_start) or 0
                                      local rl_type = tonumber(rl.type) or 0
                                      if window_start == 0 then return true end
                                      local elapsed_ms = now_ms - window_start
                                      if elapsed_ms >= window_ms * 2 then return true end
                                      if rl_type == 1 then
                                          if elapsed_ms >= window_ms then
                                              local new_elapsed = elapsed_ms - window_ms
                                              local weight = math.max(0, 1.0 - new_elapsed / window_ms)
                                              local effective = current_count * weight
                                              return effective < max_permits
                                          else
                                              local weight = math.max(0, 1.0 - elapsed_ms / window_ms)
                                              local effective = current_count + previous_count * weight
                                              return effective < max_permits
                                          end
                                      else
                                          if elapsed_ms >= window_ms then return true end
                                          return current_count < max_permits
                                      end
                                  end

                                  local function _acquire_rate_limit(rl_name, now_ms)
                                      -- Invoked once per claim; batch iterations call this once per run so the
                                      -- in-script counter state is consistent across iterations. HGET-after-HSET
                                      -- reads our own writes because Lua scripts are atomic on the server.
                                      local key = '{surefire}:rate_limit:' .. rl_name
                                      if redis.call('EXISTS', key) == 0 then return end
                                      local window_ticks = tonumber(redis.call('HGET', key, 'window'))
                                      local window_ms = window_ticks / 10000
                                      local window_start = tonumber(redis.call('HGET', key, 'window_start') or '0')
                                      if window_start == 0 then
                                          redis.call('HSET', key, 'window_start', now_ms, 'current_count', 1, 'previous_count', 0)
                                          return
                                      end
                                      local elapsed_ms = now_ms - window_start
                                      if elapsed_ms >= window_ms * 2 then
                                          local windows_elapsed = math.floor(elapsed_ms / window_ms)
                                          redis.call('HSET', key, 'window_start', window_start + window_ms * windows_elapsed, 'current_count', 1, 'previous_count', 0)
                                      elseif elapsed_ms >= window_ms then
                                          local current = tonumber(redis.call('HGET', key, 'current_count')) or 0
                                          redis.call('HSET', key, 'window_start', window_start + window_ms, 'current_count', 1, 'previous_count', current)
                                      else
                                          redis.call('HINCRBY', key, 'current_count', 1)
                                      end
                                  end

                                  local now_ms = tonumber(ARGV[4])
                                  local max_count = tonumber(ARGV[5])
                                  local job_names = cjson.decode(ARGV[1])
                                  local queue_names = cjson.decode(ARGV[2])
                                  local node_name = ARGV[3]

                                  local job_set = {}
                                  for _, n in ipairs(job_names) do job_set[n] = true end

                                  local job_cache = {}
                                  local queue_cache = {}

                                  -- Build ordered list of (queue_name, priority) sorted by priority DESC then queue name ASC.
                                  local ordered_queues = {}
                                  for _, qn in ipairs(queue_names) do
                                      local qdata = redis.call('HGETALL', '{surefire}:queue:' .. qn)
                                      local q = {}
                                      for i = 1, #qdata, 2 do q[qdata[i]] = qdata[i+1] end
                                      queue_cache[qn] = q
                                      local pri = tonumber(q.priority) or 0
                                      ordered_queues[#ordered_queues + 1] = {qn, pri}
                                  end
                                  table.sort(ordered_queues, function(a, b)
                                      if a[2] == b[2] then
                                          return a[1] < b[1]
                                      end
                                      return a[2] > b[2]
                                  end)

                                  local batch_size = 100

                                  local function is_better_candidate(a, b)
                                      if b == nil then
                                          return true
                                      end
                                      if a.run.priority ~= b.run.priority then
                                          return tonumber(a.run.priority) > tonumber(b.run.priority)
                                      end
                                      if a.run.notBefore ~= b.run.notBefore then
                                          return tonumber(a.run.notBefore) < tonumber(b.run.notBefore)
                                      end
                                      return a.runId < b.runId
                                  end

                                  local function find_best_for_queue(queue_name, queue)
                                      if queue.is_paused == '1' then
                                          return nil
                                      end

                                      if queue.max_concurrency and queue.max_concurrency ~= '' then
                                          local running = redis.call('SCARD', '{surefire}:running_q:' .. queue_name)
                                          if running >= tonumber(queue.max_concurrency) then
                                              return nil
                                          end
                                      end

                                      local queue_rl_name = queue.rate_limit_name or ''
                                      if queue_rl_name ~= '' and not _check_rate_limit(queue_rl_name, now_ms) then
                                          return nil
                                      end

                                      local offset = 0
                                      while true do
                                          local candidates = redis.call('ZRANGE', '{surefire}:pending_q:' .. queue_name, offset, offset + batch_size - 1)
                                          if #candidates == 0 then
                                              break
                                          end

                                          local retained = 0

                                          for _, entry in ipairs(candidates) do
                                              local pipe_pos = string.find(entry, '|', 1, true)
                                              local run_id = string.sub(entry, pipe_pos + 1)
                                              local run_data = redis.call('GET', '{surefire}:run:' .. run_id)
                                              if run_data then
                                                  local r = cjson.decode(run_data)
                                                  if r.status == 0 and r.notBefore <= now_ms
                                                      and (r.notAfter == nil or r.notAfter == cjson.null or tonumber(r.notAfter) > now_ms)
                                                      and job_set[r.jobName] then
                                                      local job = job_cache[r.jobName]
                                                      if not job then
                                                          local job_data = redis.call('HGETALL', '{surefire}:job:' .. r.jobName)
                                                          if #job_data == 0 then
                                                              -- Keep orphan runs pending until their job definition is registered.
                                                              retained = retained + 1
                                                          else
                                                              job = {}
                                                              for i = 1, #job_data, 2 do
                                                                  job[job_data[i]] = job_data[i+1]
                                                              end
                                                              job_cache[r.jobName] = job
                                                          end
                                                      end

                                                      if job then
                                                          local can_claim = true
                                                          if job.max_concurrency and job.max_concurrency ~= '' then
                                                              local running = redis.call('SCARD', '{surefire}:running:' .. r.jobName)
                                                              if running >= tonumber(job.max_concurrency) then
                                                                  can_claim = false
                                                              end
                                                          end

                                                          local job_rl_name = job.rate_limit_name or ''
                                                          if can_claim and job_rl_name ~= '' and not _check_rate_limit(job_rl_name, now_ms) then
                                                              can_claim = false
                                                          end

                                                          if can_claim and queue_rl_name ~= '' and queue_rl_name ~= job_rl_name
                                                              and not _check_rate_limit(queue_rl_name, now_ms) then
                                                              can_claim = false
                                                          end

                                                          if can_claim then
                                                              return {
                                                                  run = r,
                                                                  runId = run_id,
                                                                  entry = entry,
                                                                  queue_name = queue_name,
                                                                  job_rl_name = job_rl_name,
                                                                  queue_rl_name = queue_rl_name
                                                              }
                                                          end

                                                          retained = retained + 1
                                                      end
                                                  elseif r.status ~= 0 then
                                                      redis.call('ZREM', '{surefire}:pending_q:' .. queue_name, entry)
                                                      redis.call('DEL', '{surefire}:pending_member:' .. run_id)
                                                  else
                                                      retained = retained + 1
                                                  end
                                              else
                                                  redis.call('ZREM', '{surefire}:pending_q:' .. queue_name, entry)
                                                  redis.call('DEL', '{surefire}:pending_member:' .. run_id)
                                              end
                                          end

                                          offset = offset + retained
                                      end

                                      return nil
                                  end

                                  -- Batch claim: repeatedly find the highest-priority claimable candidate
                                  -- and claim it, up to max_count. Since Redis executes Lua scripts atomically,
                                  -- the SADD/HINCRBY/ZREM mutations in one iteration are visible to subsequent
                                  -- iterations via SCARD/HGET/ZRANGE — no external bookkeeping needed. Per-job
                                  -- max_concurrency, per-queue max_concurrency, and rate-limit capacity are
                                  -- therefore respected across the whole batch without double-claiming.
                                  local claimed = {}
                                  while #claimed < max_count do
                                      local idx = 1
                                      local best = nil
                                      while idx <= #ordered_queues do
                                          local tier_priority = ordered_queues[idx][2]
                                          local tier_end = idx
                                          while tier_end + 1 <= #ordered_queues and ordered_queues[tier_end + 1][2] == tier_priority do
                                              tier_end = tier_end + 1
                                          end

                                          for i = idx, tier_end do
                                              local queue_name = ordered_queues[i][1]
                                              local queue = queue_cache[queue_name]
                                              local candidate = find_best_for_queue(queue_name, queue)
                                              if candidate and is_better_candidate(candidate, best) then
                                                  best = candidate
                                              end
                                          end

                                          if best then
                                              break
                                          end

                                          idx = tier_end + 1
                                      end

                                      if best == nil then
                                          break
                                      end

                                      if best.job_rl_name ~= '' then
                                          _acquire_rate_limit(best.job_rl_name, now_ms)
                                      end
                                      if best.queue_rl_name ~= '' and best.queue_rl_name ~= best.job_rl_name then
                                          _acquire_rate_limit(best.queue_rl_name, now_ms)
                                      end

                                      local old_node = best.run.nodeName
                                      if old_node and old_node ~= cjson.null and old_node ~= node_name then
                                          redis.call('SREM', '{surefire}:node_runs:' .. old_node, best.runId)
                                      end

                                      best.run.status = 1
                                      best.run.nodeName = node_name
                                      best.run.startedAt = tonumber(now_ms)
                                      best.run.lastHeartbeatAt = tonumber(now_ms)
                                      best.run.attempt = best.run.attempt + 1

                                      redis.call('SET', '{surefire}:run:' .. best.runId, cjson.encode(best.run))
                                      redis.call('ZADD', '{surefire}:job_started:' .. best.run.jobName, tonumber(now_ms), best.runId)
                                      redis.call('ZREM', '{surefire}:pending_q:' .. best.queue_name, best.entry)
                                      redis.call('DEL', '{surefire}:pending_member:' .. best.runId)
                                      redis.call('SADD', '{surefire}:running:' .. best.run.jobName, best.runId)
                                      redis.call('SADD', '{surefire}:running_q:' .. best.queue_name, best.runId)
                                      redis.call('ZADD', '{surefire}:heartbeat:running', tonumber(now_ms), best.runId)

                                      redis.call('ZREM', '{surefire}:status:0', best.runId)
                                      redis.call('ZADD', '{surefire}:status:1', best.run.createdAt, best.runId)
                                      redis.call('HINCRBY', '{surefire}:status_counts', '0', -1)
                                      redis.call('HINCRBY', '{surefire}:status_counts', '1', 1)
                                      redis.call('SADD', '{surefire}:node_runs:' .. node_name, best.runId)

                                      local event_id = redis.call('INCR', '{surefire}:event_seq')
                                      local evt = {
                                          id = event_id,
                                          runId = best.run.id,
                                          eventType = 0,
                                          payload = '1',
                                          createdAt = tonumber(now_ms),
                                          attempt = best.run.attempt
                                      }
                                      local payload = cjson.encode(evt)
                                      local event_id_str = tostring(event_id)
                                      redis.call('SET', '{surefire}:event:' .. event_id_str, payload)
                                      redis.call('ZADD', '{surefire}:events:run:' .. best.run.id, event_id, event_id_str)
                                      redis.call('ZADD', '{surefire}:events:run_type:' .. best.run.id .. ':0', event_id, event_id_str)
                                      redis.call('ZADD', '{surefire}:events:run_attempt:' .. best.run.id .. ':' .. tostring(best.run.attempt), event_id, event_id_str)
                                      redis.call('SADD', '{surefire}:events:run_types:' .. best.run.id, '0')
                                      redis.call('SADD', '{surefire}:events:run_attempts:' .. best.run.id, tostring(best.run.attempt))
                                      if best.run.batchId and best.run.batchId ~= cjson.null and best.run.batchId ~= '' then
                                          redis.call('ZADD', '{surefire}:events:batch:' .. best.run.batchId, event_id, event_id_str)
                                      end

                                      claimed[#claimed + 1] = cjson.encode(best.run)
                                  end

                                  return claimed
                                  """;

        var result = await EvaluateScriptAsync(fullScript,
            [RoutingKey],
            [
                JsonSerializer.Serialize(jobNames.ToArray(), SurefireJsonContext.Default.StringArray),
                JsonSerializer.Serialize(queueNames.ToArray(), SurefireJsonContext.Default.StringArray),
                nodeName,
                timeProvider.GetUtcNow().ToUnixTimeMilliseconds().ToString(),
                maxCount.ToString()
            ]);

        if (result.IsNull || result.Resp2Type != ResultType.Array)
        {
            return Array.Empty<JobRun>();
        }

        var rows = (RedisResult[])result!;
        if (rows.Length == 0)
        {
            return Array.Empty<JobRun>();
        }

        var runs = new List<JobRun>(rows.Length);
        foreach (var row in rows)
        {
            runs.Add(DeserializeRun(row.ToString()!));
        }

        return runs;
    }

    public Task CreateBatchAsync(JobBatch batch, IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null, CancellationToken cancellationToken = default)
        => CreateRunsCoreAsync(runs, initialEvents, cancellationToken, SerializeBatch(batch));

    public async Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default)
    {
        var json = await Db.StringGetAsync($"{P}batch:{batchId}");
        return json.IsNullOrEmpty ? null : DeserializeBatch(json!);
    }

    public async Task<bool> TryCompleteBatchAsync(string batchId, JobStatus status, DateTimeOffset completedAt,
        CancellationToken cancellationToken = default)
    {
        const string script = """
                              local key = '{surefire}:batch:' .. ARGV[1]
                              local data = redis.call('GET', key)
                              if not data then return 0 end
                              local b = cjson.decode(data)
                              if b.status == 2 or b.status == 4 or b.status == 5 then return 0 end
                              b.status = tonumber(ARGV[2])
                              b.completedAt = tonumber(ARGV[3])
                              redis.call('SET', key, cjson.encode(b))
                              -- Remove from active set and track in completed set for purge
                              redis.call('SREM', '{surefire}:batches:active', ARGV[1])
                              redis.call('ZADD', '{surefire}:batches', tonumber(ARGV[3]), ARGV[1])
                              return 1
                              """;

        var result = await EvaluateScriptAsync(script,
            [RoutingKey],
            [
                batchId,
                ((int)status).ToString(),
                completedAt.ToUnixTimeMilliseconds().ToString()
            ]);

        return (int)result == 1;
    }

    public async Task<IReadOnlyList<string>> GetCompletableBatchIdsAsync(
        CancellationToken cancellationToken = default)
    {
        const string script = """
                              local active = redis.call('SMEMBERS', '{surefire}:batches:active')
                              local result = {}
                              for _, batch_id in ipairs(active) do
                                  local bdata = redis.call('GET', '{surefire}:batch:' .. batch_id)
                                  if bdata then
                                      local b = cjson.decode(bdata)
                                      local bstatus = tonumber(b.status)
                                      if bstatus ~= 2 and bstatus ~= 4 and bstatus ~= 5 then
                                          local run_ids = redis.call('ZRANGE', '{surefire}:batch_runs:' .. batch_id, 0, -1)
                                          local has_nonterminal = false
                                          for _, run_id in ipairs(run_ids) do
                                              local rdata = redis.call('GET', '{surefire}:run:' .. run_id)
                                              if rdata then
                                                  local r = cjson.decode(rdata)
                                                  local s = tonumber(r.status)
                                                  if s ~= 2 and s ~= 4 and s ~= 5 then
                                                      has_nonterminal = true
                                                      break
                                                  end
                                              end
                                          end
                                          if not has_nonterminal then
                                              result[#result + 1] = batch_id
                                          end
                                      end
                                  end
                              end
                              if #result == 0 then return nil end
                              return cjson.encode(result)
                              """;

        var raw = await EvaluateScriptAsync(script, [RoutingKey], []);
        if (raw.IsNull)
        {
            return [];
        }

        return JsonSerializer.Deserialize(raw.ToString()!, SurefireJsonContext.Default.StringArray) ?? [];
    }

    public async Task<IReadOnlyList<string>> CancelBatchRunsAsync(string batchId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        const string script = """
                              local batch_id = ARGV[1]
                              local now_ms = tonumber(ARGV[2])
                              local err = ARGV[3] ~= '' and ARGV[3] or nil
                              local batch_runs_key = '{surefire}:batch_runs:' .. batch_id
                              local function append_status_event(run)
                                  local id = redis.call('INCR', '{surefire}:event_seq')
                                  local evt = {id = id, runId = run.id, eventType = 0, payload = '4', createdAt = now_ms, attempt = run.attempt}
                                  local payload = cjson.encode(evt)
                                  local id_str = tostring(id)
                                  redis.call('SET', '{surefire}:event:' .. id_str, payload)
                                  redis.call('ZADD', '{surefire}:events:run:' .. run.id, id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_type:' .. run.id .. ':0', id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_attempt:' .. run.id .. ':' .. tostring(run.attempt), id, id_str)
                                  redis.call('SADD', '{surefire}:events:run_types:' .. run.id, '0')
                                  redis.call('SADD', '{surefire}:events:run_attempts:' .. run.id, tostring(run.attempt))
                                  redis.call('ZADD', '{surefire}:events:batch:' .. batch_id, id, id_str)
                              end
                              local run_ids = redis.call('ZRANGE', batch_runs_key, 0, -1)
                              local cancelled = {}
                              local cancel_count = 0
                              for _, run_id in ipairs(run_ids) do
                                  local key = '{surefire}:run:' .. run_id
                                  local data = redis.call('GET', key)
                                  if data then
                                      local r = cjson.decode(data)
                                      if r.status == 0 or r.status == 1 then
                                          local old_status = r.status
                                          r.status = 4
                                          r.cancelledAt = now_ms
                                          r.completedAt = now_ms
                                          if err then r.reason =err end
                                          redis.call('SET', key, cjson.encode(r))
                                          redis.call('ZREM', '{surefire}:runs:nonterminal', run_id)
                                          redis.call('ZREM', '{surefire}:status:' .. tostring(old_status), run_id)
                                          redis.call('ZADD', '{surefire}:status:4', r.createdAt, run_id)
                                          redis.call('HINCRBY', '{surefire}:status_counts', tostring(old_status), -1)
                                          redis.call('HINCRBY', '{surefire}:status_counts', '4', 1)
                                          if old_status == 0 then
                                              local pm = redis.call('GET', '{surefire}:pending_member:' .. run_id)
                                              if pm then
                                                  local q_field = redis.call('HGET', '{surefire}:job:' .. r.jobName, 'queue')
                                                  local qname = (q_field and q_field ~= '') and q_field or 'default'
                                                  redis.call('ZREM', '{surefire}:pending_q:' .. qname, pm)
                                                  redis.call('DEL', '{surefire}:pending_member:' .. run_id)
                                              end
                                          elseif old_status == 1 then
                                              redis.call('SREM', '{surefire}:running:' .. r.jobName, run_id)
                                              local q_field = redis.call('HGET', '{surefire}:job:' .. r.jobName, 'queue')
                                              local qname = (q_field and q_field ~= '') and q_field or 'default'
                                              redis.call('SREM', '{surefire}:running_q:' .. qname, run_id)
                                              redis.call('ZREM', '{surefire}:heartbeat:running', run_id)
                                              local old_node = r.nodeName
                                              if old_node and old_node ~= cjson.null and old_node ~= '' then
                                                  redis.call('SREM', '{surefire}:node_runs:' .. old_node, run_id)
                                              end
                                          end
                                          redis.call('ZADD', '{surefire}:runs:terminal', r.createdAt, run_id)
                                          redis.call('ZREM', '{surefire}:expiring', run_id)
                                          redis.call('SREM', '{surefire}:nonterminal:' .. r.jobName, run_id)
                                          redis.call('HINCRBY', '{surefire}:job_stats:' .. r.jobName, 'terminal_runs', 1)
                                          redis.call('ZADD', '{surefire}:runs:completed', now_ms, run_id)
                                          if r.deduplicationId and r.deduplicationId ~= cjson.null and r.deduplicationId ~= '' then
                                              redis.call('DEL', '{surefire}:dedup:' .. r.jobName .. ':' .. r.deduplicationId)
                                          end
                                          local minute = math.floor(now_ms / 60000)
                                          redis.call('HINCRBY', '{surefire}:timeline:' .. tostring(minute), 'cancelled', 1)
                                          redis.call('ZADD', '{surefire}:timeline:index', minute, tostring(minute))
                                          append_status_event(r)
                                          cancelled[#cancelled + 1] = run_id
                                          cancel_count = cancel_count + 1
                                      end
                                  end
                              end
                              -- Update batch counter
                              if cancel_count > 0 then
                                  local batch_key = '{surefire}:batch:' .. batch_id
                                  local batch_data = redis.call('GET', batch_key)
                                  if batch_data then
                                      local b = cjson.decode(batch_data)
                                      if b.status ~= 2 and b.status ~= 4 and b.status ~= 5 then
                                          b.cancelled = (b.cancelled or 0) + cancel_count
                                          local total_done = (b.succeeded or 0) + (b.failed or 0) + (b.cancelled or 0)
                                          if total_done >= b.total then
                                              local batch_status
                                              if (b.failed or 0) > 0 then batch_status = 5
                                              elseif (b.cancelled or 0) > 0 then batch_status = 4
                                              else batch_status = 2 end
                                              b.status = batch_status
                                              b.completedAt = now_ms
                                              redis.call('SET', batch_key, cjson.encode(b))
                                              redis.call('SREM', '{surefire}:batches:active', batch_id)
                                              redis.call('ZADD', '{surefire}:batches', now_ms, batch_id)
                                          else
                                              redis.call('SET', batch_key, cjson.encode(b))
                                          end
                                      end
                                  end
                              end
                              if #cancelled == 0 then return nil end
                              return cjson.encode(cancelled)
                              """;

        var result = await EvaluateScriptAsync(script,
            [RoutingKey],
            [batchId, timeProvider.GetUtcNow().ToUnixTimeMilliseconds().ToString(), reason ?? ""]);

        if (result.IsNull)
        {
            return [];
        }

        return JsonSerializer.Deserialize(result.ToString()!, SurefireJsonContext.Default.StringArray) ?? [];
    }

    public async Task<IReadOnlyList<string>> CancelChildRunsAsync(string parentRunId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        const string script = """
                              local children_key = '{surefire}:children:' .. ARGV[1]
                              local child_member_ts_width = 20
                              local now_ms = tonumber(ARGV[2])
                              local err = ARGV[3] ~= '' and ARGV[3] or nil
                              local function append_status_event(run)
                                  local id = redis.call('INCR', '{surefire}:event_seq')
                                  local evt = {id = id, runId = run.id, eventType = 0, payload = '4', createdAt = now_ms, attempt = run.attempt}
                                  local payload = cjson.encode(evt)
                                  local id_str = tostring(id)
                                  redis.call('SET', '{surefire}:event:' .. id_str, payload)
                                  redis.call('ZADD', '{surefire}:events:run:' .. run.id, id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_type:' .. run.id .. ':0', id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_attempt:' .. run.id .. ':' .. tostring(run.attempt), id, id_str)
                                  redis.call('SADD', '{surefire}:events:run_types:' .. run.id, '0')
                                  redis.call('SADD', '{surefire}:events:run_attempts:' .. run.id, tostring(run.attempt))
                                  if run.batchId and run.batchId ~= cjson.null and run.batchId ~= '' then
                                      redis.call('ZADD', '{surefire}:events:batch:' .. run.batchId, id, id_str)
                                  end
                              end
                              local child_members = redis.call('ZRANGE', children_key, 0, -1)
                              local cancelled = {}
                              local batch_counts = {}
                              for _, child_member in ipairs(child_members) do
                                  local run_id = string.sub(child_member, child_member_ts_width + 1)
                                  local key = '{surefire}:run:' .. run_id
                                  local data = redis.call('GET', key)
                                  if data then
                                      local r = cjson.decode(data)
                                      if r.status == 0 or r.status == 1 then
                                          local old_status = r.status
                                          r.status = 4
                                          r.cancelledAt = now_ms
                                          r.completedAt = now_ms
                                          if err then r.reason =err end
                                          redis.call('SET', key, cjson.encode(r))
                                          redis.call('ZREM', '{surefire}:runs:nonterminal', run_id)
                                          redis.call('ZREM', '{surefire}:status:' .. tostring(old_status), run_id)
                                          redis.call('ZADD', '{surefire}:status:4', r.createdAt, run_id)
                                          redis.call('HINCRBY', '{surefire}:status_counts', tostring(old_status), -1)
                                          redis.call('HINCRBY', '{surefire}:status_counts', '4', 1)
                                          if old_status == 0 then
                                              local pm = redis.call('GET', '{surefire}:pending_member:' .. run_id)
                                              if pm then
                                                  local q_field = redis.call('HGET', '{surefire}:job:' .. r.jobName, 'queue')
                                                  local qname = (q_field and q_field ~= '') and q_field or 'default'
                                                  redis.call('ZREM', '{surefire}:pending_q:' .. qname, pm)
                                                  redis.call('DEL', '{surefire}:pending_member:' .. run_id)
                                              end
                                          elseif old_status == 1 then
                                              redis.call('SREM', '{surefire}:running:' .. r.jobName, run_id)
                                              local q_field = redis.call('HGET', '{surefire}:job:' .. r.jobName, 'queue')
                                              local qname = (q_field and q_field ~= '') and q_field or 'default'
                                              redis.call('SREM', '{surefire}:running_q:' .. qname, run_id)
                                              redis.call('ZREM', '{surefire}:heartbeat:running', run_id)
                                              local old_node = r.nodeName
                                              if old_node and old_node ~= cjson.null and old_node ~= '' then
                                                  redis.call('SREM', '{surefire}:node_runs:' .. old_node, run_id)
                                              end
                                          end
                                          redis.call('ZADD', '{surefire}:runs:terminal', r.createdAt, run_id)
                                          redis.call('ZREM', '{surefire}:expiring', run_id)
                                          redis.call('SREM', '{surefire}:nonterminal:' .. r.jobName, run_id)
                                          redis.call('HINCRBY', '{surefire}:job_stats:' .. r.jobName, 'terminal_runs', 1)
                                          redis.call('ZADD', '{surefire}:runs:completed', now_ms, run_id)
                                          if r.deduplicationId and r.deduplicationId ~= cjson.null and r.deduplicationId ~= '' then
                                              redis.call('DEL', '{surefire}:dedup:' .. r.jobName .. ':' .. r.deduplicationId)
                                          end
                                          local minute = math.floor(now_ms / 60000)
                                          redis.call('HINCRBY', '{surefire}:timeline:' .. tostring(minute), 'cancelled', 1)
                                          redis.call('ZADD', '{surefire}:timeline:index', minute, tostring(minute))
                                          append_status_event(r)
                                          cancelled[#cancelled + 1] = run_id
                                          if r.batchId and r.batchId ~= cjson.null and r.batchId ~= '' then
                                              batch_counts[r.batchId] = (batch_counts[r.batchId] or 0) + 1
                                          end
                                      end
                                  end
                              end
                              -- Update batch counters
                              for bid, cnt in pairs(batch_counts) do
                                  local batch_key = '{surefire}:batch:' .. bid
                                  local batch_data = redis.call('GET', batch_key)
                                  if batch_data then
                                      local b = cjson.decode(batch_data)
                                      if b.status ~= 2 and b.status ~= 4 and b.status ~= 5 then
                                          b.cancelled = (b.cancelled or 0) + cnt
                                          local total_done = (b.succeeded or 0) + (b.failed or 0) + (b.cancelled or 0)
                                          if total_done >= b.total then
                                              local batch_status
                                              if (b.failed or 0) > 0 then batch_status = 5
                                              elseif (b.cancelled or 0) > 0 then batch_status = 4
                                              else batch_status = 2 end
                                              b.status = batch_status
                                              b.completedAt = now_ms
                                              redis.call('SET', batch_key, cjson.encode(b))
                                              redis.call('SREM', '{surefire}:batches:active', bid)
                                              redis.call('ZADD', '{surefire}:batches', now_ms, bid)
                                          else
                                              redis.call('SET', batch_key, cjson.encode(b))
                                          end
                                      end
                                  end
                              end
                              if #cancelled == 0 then return nil end
                              return cjson.encode(cancelled)
                              """;

        var result = await EvaluateScriptAsync(script,
            [RoutingKey],
            [parentRunId, timeProvider.GetUtcNow().ToUnixTimeMilliseconds().ToString(), reason ?? ""]);

        if (result.IsNull)
        {
            return [];
        }

        return JsonSerializer.Deserialize(result.ToString()!, SurefireJsonContext.Default.StringArray) ?? [];
    }

    public async Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default)
    {
        if (events.Count == 0)
        {
            return;
        }

        const string script = """
                              local seq_key = '{surefire}:event_seq'
                              local batch_ids = {}
                              for i = 1, #ARGV do
                                  local id = redis.call('INCR', seq_key)
                                  local evt = cjson.decode(ARGV[i])
                                  evt.id = id
                                  local payload = cjson.encode(evt)
                                 local id_str = tostring(id)
                                 redis.call('SET', '{surefire}:event:' .. id_str, payload)
                                 redis.call('ZADD', '{surefire}:events:run:' .. evt.runId, id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_type:' .. evt.runId .. ':' .. tostring(evt.eventType), id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_attempt:' .. evt.runId .. ':' .. tostring(evt.attempt), id, id_str)
                                  redis.call('SADD', '{surefire}:events:run_types:' .. evt.runId, tostring(evt.eventType))
                                  redis.call('SADD', '{surefire}:events:run_attempts:' .. evt.runId, tostring(evt.attempt))

                                  local batch_id = batch_ids[evt.runId]
                                  if batch_id == nil then
                                      local run_payload = redis.call('GET', '{surefire}:run:' .. evt.runId)
                                      if run_payload then
                                          local run = cjson.decode(run_payload)
                                          batch_id = run.batchId
                                      else
                                          batch_id = false
                                      end
                                      batch_ids[evt.runId] = batch_id
                                  end

                                  if batch_id and batch_id ~= cjson.null and batch_id ~= '' then
                                      redis.call('ZADD', '{surefire}:events:batch:' .. batch_id, id, id_str)
                                      if evt.eventType == 3 then
                                          redis.call('ZADD', '{surefire}:events:batch_output:' .. batch_id, id, id_str)
                                      end
                                  end
                              end
                              return 1
                              """;

        var args = new RedisValue[events.Count];
        for (var i = 0; i < events.Count; i++)
        {
            args[i] = JsonSerializer.Serialize(events[i], SurefireJsonContext.Default.RunEvent);
        }

        await EvaluateScriptAsync(script, [RoutingKey], args);
    }

    public async Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0,
        RunEventType[]? types = null, int? attempt = null, int? take = null,
        CancellationToken cancellationToken = default)
    {
        var db = Db;

        RedisValue[] ids;
        if (attempt is { } && (types is null || types.Length == 0))
        {
            var requestedAttemptKey = (RedisKey)$"{P}events:run_attempt:{runId}:{attempt.Value}";
            var requestedAttemptTask = db.SortedSetRangeByScoreAsync(requestedAttemptKey,
                sinceId,
                double.PositiveInfinity,
                Exclude.Start,
                Order.Ascending,
                0,
                take ?? -1);

            Task<RedisValue[]>? wildcardAttemptTask = null;
            if (attempt.Value != 0)
            {
                var wildcardAttemptKey = (RedisKey)$"{P}events:run_attempt:{runId}:0";
                wildcardAttemptTask = db.SortedSetRangeByScoreAsync(wildcardAttemptKey,
                    sinceId,
                    double.PositiveInfinity,
                    Exclude.Start,
                    Order.Ascending,
                    0,
                    take ?? -1);
            }

            if (wildcardAttemptTask is { })
            {
                await Task.WhenAll(requestedAttemptTask, wildcardAttemptTask);
                var merged = requestedAttemptTask.Result
                    .Concat(wildcardAttemptTask.Result)
                    .Select(id => id.ToString())
                    .Distinct(StringComparer.Ordinal)
                    .OrderBy(id => long.Parse(id), Comparer<long>.Default)
                    .Select(id => (RedisValue)id)
                    .ToArray();
                ids = merged;
                if (take is { })
                {
                    ids = ids.Take(take.Value).ToArray();
                }
            }
            else
            {
                ids = await requestedAttemptTask;
            }
        }
        else
        {
            RedisKey indexKey = $"{P}events:run:{runId}";
            if (types is { Length: 1 } singleType)
            {
                indexKey = $"{P}events:run_type:{runId}:{(int)singleType[0]}";
            }

            ids = await db.SortedSetRangeByScoreAsync(indexKey,
                sinceId,
                double.PositiveInfinity,
                Exclude.Start,
                Order.Ascending,
                0,
                take ?? -1);
        }

        if (ids.Length == 0)
        {
            return [];
        }

        var typeSet = types is { Length: > 0 }
            ? new HashSet<int>(types.Select(t => (int)t))
            : null;

        var batch = db.CreateBatch();
        var payloadTasks = ids
            .Select(id => batch.StringGetAsync($"{P}event:{id}"))
            .ToArray();
        batch.Execute();
        await Task.WhenAll(payloadTasks);

        var events = new List<RunEvent>(payloadTasks.Length);
        foreach (var payloadTask in payloadTasks)
        {
            var jsonValue = await payloadTask;
            if (jsonValue.IsNullOrEmpty)
            {
                continue;
            }

            try
            {
                var evt = JsonSerializer.Deserialize(jsonValue.ToString(), SurefireJsonContext.Default.RunEvent)
                          ?? throw new InvalidOperationException("Event payload was null.");
                if (typeSet is { } && !typeSet.Contains((int)evt.EventType))
                {
                    continue;
                }

                if (attempt is { } && evt.Attempt != attempt.Value && evt.Attempt != 0)
                {
                    continue;
                }

                events.Add(evt);
            }
            catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException)
            {
                Log.MalformedEventPayload(logger, ex);
            }
        }

        return events;
    }

    public async Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchId, long sinceEventId = 0,
        int take = 200, CancellationToken cancellationToken = default)
        => await GetBatchEventsCoreAsync(batchId, sinceEventId, take, RunEventType.Output);

    public async Task<IReadOnlyList<RunEvent>> GetBatchEventsAsync(string batchId, long sinceEventId = 0,
        int take = 200, CancellationToken cancellationToken = default)
        => await GetBatchEventsCoreAsync(batchId, sinceEventId, take, null);

    public async Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, IReadOnlyCollection<string> activeRunIds,
        CancellationToken cancellationToken = default)
    {
        var now = timeProvider.GetUtcNow();
        var nowMs = now.ToUnixTimeMilliseconds();
        var db = Db;
        var nodeKey = $"{P}node:{nodeName}";
        var startedAt = await db.HashGetAsync(nodeKey, "started_at");
        var startedAtValue = startedAt.IsNullOrEmpty ? nowMs.ToString() : startedAt.ToString();

        await db.HashSetAsync(nodeKey,
        [
            new("name", nodeName),
            new("started_at", startedAtValue),
            new("last_heartbeat_at", nowMs.ToString()),
            new("running_count", activeRunIds.Count.ToString()),
            new("registered_job_names",
                JsonSerializer.Serialize(jobNames.ToArray(), SurefireJsonContext.Default.StringArray)),
            new("registered_queue_names",
                JsonSerializer.Serialize(queueNames.ToArray(), SurefireJsonContext.Default.StringArray))
        ]);
        await db.SetAddAsync($"{P}nodes", nodeName);

        if (activeRunIds.Count == 0)
        {
            return;
        }

        const string script = """
                              local now_ms = tonumber(ARGV[1])
                              local node_name = ARGV[2]
                              for i = 3, #ARGV do
                                  local key = '{surefire}:run:' .. ARGV[i]
                                  local data = redis.call('GET', key)
                                  if data then
                                      local r = cjson.decode(data)
                                      if (r.status ~= 2 and r.status ~= 4 and r.status ~= 5)
                                          and r.nodeName == node_name then
                                          r.lastHeartbeatAt = now_ms
                                          redis.call('SET', key, cjson.encode(r))
                                          -- Maintain the heartbeat-sorted index used by
                                          -- GetStaleRunningRunIdsAsync. Update only for Running rows
                                          -- so non-running rows don't drift into the index.
                                          if r.status == 1 then
                                              redis.call('ZADD', '{surefire}:heartbeat:running', now_ms, ARGV[i])
                                          end
                                      end
                                  end
                              end
                              return 1
                              """;

        var distinctRunIds = activeRunIds.Distinct(StringComparer.Ordinal).ToArray();
        var args = new RedisValue[distinctRunIds.Length + 2];
        args[0] = nowMs.ToString();
        args[1] = nodeName;
        for (var i = 0; i < distinctRunIds.Length; i++)
        {
            args[i + 2] = distinctRunIds[i];
        }

        await EvaluateScriptAsync(script, [RoutingKey], args);
    }

    public async Task<IReadOnlyList<string>> GetExternallyStoppedRunIdsAsync(IReadOnlyCollection<string> runIds,
        CancellationToken cancellationToken = default)
    {
        if (runIds.Count == 0)
        {
            return [];
        }

        var db = Db;
        var batch = db.CreateBatch();
        var idsArr = runIds as IList<string> ?? runIds.ToList();
        var fetchTasks = new Task<RedisValue>[idsArr.Count];
        for (var i = 0; i < idsArr.Count; i++)
        {
            fetchTasks[i] = batch.StringGetAsync($"{P}run:{idsArr[i]}");
        }

        batch.Execute();
        await Task.WhenAll(fetchTasks);

        var stopped = new List<string>();
        for (var i = 0; i < idsArr.Count; i++)
        {
            var json = await fetchTasks[i];
            if (json.IsNullOrEmpty)
            {
                // Run was deleted entirely (purged); treat as stopped.
                stopped.Add(idsArr[i]);
                continue;
            }

            var run = DeserializeRun(json!);
            if (run.Status != JobStatus.Running)
            {
                stopped.Add(idsArr[i]);
            }
        }

        return stopped;
    }

    public async Task<IReadOnlyList<string>> GetStaleRunningRunIdsAsync(DateTimeOffset staleBefore, int take,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(take, 1);

        // Backed by the {surefire}:heartbeat:running ZSET (score = last_heartbeat_at in ms),
        // maintained on every transition into/out of Running status and on each heartbeat tick.
        // ZRANGEBYSCORE is O(log N + K) where K is the result size, so the cost is bounded by
        // `take` rather than by total running runs.
        var staleBeforeMs = staleBefore.ToUnixTimeMilliseconds();
        var values = await Db.SortedSetRangeByScoreAsync(
            $"{P}heartbeat:running",
            double.NegativeInfinity,
            staleBeforeMs,
            Exclude.None,
            Order.Ascending,
            0,
            take);

        var result = new List<string>(values.Length);
        foreach (var value in values)
        {
            if (!value.IsNullOrEmpty)
            {
                result.Add(value!);
            }
        }

        return result;
    }

    public async Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default)
    {
        var db = Db;
        var members = await db.SetMembersAsync($"{P}nodes");
        if (members.Length == 0)
        {
            return [];
        }

        var batch = db.CreateBatch();
        var tasks = members.Select(m => batch.HashGetAllAsync($"{P}node:{m}")).ToArray();
        batch.Execute();
        await Task.WhenAll(tasks);

        var results = new List<NodeInfo>();
        foreach (var task in tasks)
        {
            var hash = await task;
            if (hash.Length == 0)
            {
                continue;
            }

            results.Add(HashToNode(hash));
        }

        return results;
    }

    public async Task<NodeInfo?> GetNodeAsync(string name, CancellationToken cancellationToken = default)
    {
        var hash = await Db.HashGetAllAsync($"{P}node:{name}");
        if (hash.Length == 0)
        {
            return null;
        }

        return HashToNode(hash);
    }

    public async Task UpsertQueuesAsync(IReadOnlyList<QueueDefinition> queues,
        CancellationToken cancellationToken = default)
    {
        if (queues.Count == 0)
        {
            return;
        }

        var now = timeProvider.GetUtcNow().ToUnixTimeMilliseconds().ToString();
        await EvaluateScriptAsync(UpsertQueuesScript,
            [RoutingKey],
            [UpsertPayloadFactory.SerializeQueues(queues), now]);
    }

    public async Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken cancellationToken = default)
    {
        var db = Db;
        var members = await db.SetMembersAsync($"{P}queues");
        if (members.Length == 0)
        {
            return [];
        }

        var batch = db.CreateBatch();
        var tasks = members.Select(m => batch.HashGetAllAsync($"{P}queue:{m}")).ToArray();
        batch.Execute();
        await Task.WhenAll(tasks);

        var results = new List<QueueDefinition>();
        foreach (var task in tasks)
        {
            var hash = await task;
            if (hash.Length == 0)
            {
                continue;
            }

            results.Add(HashToQueue(hash));
        }

        return results;
    }

    public async Task<bool> SetQueuePausedAsync(string name, bool isPaused,
        CancellationToken cancellationToken = default)
    {
        // Atomic "update-if-exists" keeps contract aligned with the SQL stores: callers that pass
        // an unknown queue name get back `false` so the dashboard can 404 rather than silently
        // creating a phantom queue. Queue rows — including `default` — materialize only when
        // either (a) a job registered on this node references them, via SurefireInitializationService,
        // or (b) the user explicitly configures them via `options.AddQueue(...)`.
        const string script = """
                              local key = KEYS[1]
                              if redis.call('EXISTS', key) == 0 then
                                  return 0
                              end
                              redis.call('HSET', key, 'is_paused', ARGV[1])
                              return 1
                              """;

        var result = (long)await EvaluateScriptAsync(script,
            [new($"{P}queue:{name}")],
            [isPaused ? "1" : "0"]);
        return result == 1;
    }

    public async Task UpsertRateLimitsAsync(IReadOnlyList<RateLimitDefinition> rateLimits,
        CancellationToken cancellationToken = default)
    {
        if (rateLimits.Count == 0)
        {
            return;
        }

        var now = timeProvider.GetUtcNow().ToUnixTimeMilliseconds().ToString();
        await EvaluateScriptAsync(UpsertRateLimitsScript,
            [RoutingKey],
            [UpsertPayloadFactory.SerializeRateLimits(rateLimits), now]);
    }

    public async Task<IReadOnlyList<string>> CancelExpiredRunsWithIdsAsync(
        CancellationToken cancellationToken = default)
    {
        const string script = """
                              local t = redis.call('TIME')
                              local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
                              local function append_status_event(run)
                                  local id = redis.call('INCR', '{surefire}:event_seq')
                                  local evt = {
                                      id = id,
                                      runId = run.id,
                                      eventType = 0,
                                      payload = tostring(run.status),
                                      createdAt = now_ms,
                                      attempt = run.attempt
                                  }
                                  local payload = cjson.encode(evt)
                                  local id_str = tostring(id)
                                  redis.call('SET', '{surefire}:event:' .. id_str, payload)
                                  redis.call('ZADD', '{surefire}:events:run:' .. run.id, id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_type:' .. run.id .. ':0', id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_attempt:' .. run.id .. ':' .. tostring(run.attempt), id, id_str)
                                  redis.call('SADD', '{surefire}:events:run_types:' .. run.id, '0')
                                  redis.call('SADD', '{surefire}:events:run_attempts:' .. run.id, tostring(run.attempt))
                                  if run.batchId and run.batchId ~= cjson.null and run.batchId ~= '' then
                                      redis.call('ZADD', '{surefire}:events:batch:' .. run.batchId, id, id_str)
                                  end
                              end
                              local offset = tonumber(ARGV[1])
                              local expired = redis.call('ZRANGEBYSCORE', '{surefire}:expiring', '-inf', now_ms, 'LIMIT', offset, 500)
                              local cancelled_ids = {}
                              local cleaned = 0
                              local skipped = 0

                              for _, run_id in ipairs(expired) do
                                  local key = '{surefire}:run:' .. run_id
                                  local data = redis.call('GET', key)
                                  if not data then
                                      redis.call('ZREM', '{surefire}:expiring', run_id)
                                      cleaned = cleaned + 1
                                  else
                                      local r = cjson.decode(data)
                                      if r.status == 2 or r.status == 4 or r.status == 5 then
                                          redis.call('ZREM', '{surefire}:expiring', run_id)
                                          cleaned = cleaned + 1
                                      elseif r.status == 0 then
                                          r.status = 4
                                          r.cancelledAt = now_ms
                                          r.completedAt = now_ms
                                          r.reason = ARGV[2]
                                          redis.call('SET', key, cjson.encode(r))

                                          -- Resolve queue name for pending_q cleanup
                                          local q_field = redis.call('HGET', '{surefire}:job:' .. r.jobName, 'queue')
                                          local queue_name = (q_field and q_field ~= '') and q_field or 'default'

                                          local pm = redis.call('GET', '{surefire}:pending_member:' .. run_id)
                                          if pm then
                                              redis.call('ZREM', '{surefire}:pending_q:' .. queue_name, pm)
                                              redis.call('DEL', '{surefire}:pending_member:' .. run_id)
                                          end
                                          -- Status index maintenance
                                          redis.call('ZREM', '{surefire}:status:0', run_id)
                                          redis.call('ZADD', '{surefire}:status:4', r.createdAt, run_id)
                                          redis.call('HINCRBY', '{surefire}:status_counts', '0', -1)
                                          redis.call('HINCRBY', '{surefire}:status_counts', '4', 1)
                                          redis.call('ZREM', '{surefire}:runs:nonterminal', run_id)
                                          redis.call('ZADD', '{surefire}:runs:terminal', r.createdAt, run_id)

                                          if r.deduplicationId and r.deduplicationId ~= cjson.null and r.deduplicationId ~= '' then
                                              redis.call('DEL', '{surefire}:dedup:' .. r.jobName .. ':' .. r.deduplicationId)
                                          end
                                          redis.call('SREM', '{surefire}:nonterminal:' .. r.jobName, run_id)
                                          redis.call('HINCRBY', '{surefire}:job_stats:' .. r.jobName, 'terminal_runs', 1)
                                          redis.call('ZADD', '{surefire}:runs:completed', now_ms, run_id)
                                          local minute = math.floor(now_ms / 60000)
                                           redis.call('HINCRBY', '{surefire}:timeline:' .. tostring(minute), 'cancelled', 1)
                                           redis.call('ZADD', '{surefire}:timeline:index', minute, tostring(minute))
                                           redis.call('ZREM', '{surefire}:expiring', run_id)
                                           append_status_event(r)

                                           -- Batch counter update
                                           if r.batchId and r.batchId ~= cjson.null and r.batchId ~= '' then
                                               local batch_key = '{surefire}:batch:' .. r.batchId
                                               local batch_data = redis.call('GET', batch_key)
                                               if batch_data then
                                                   local b = cjson.decode(batch_data)
                                                   if b.status ~= 2 and b.status ~= 4 and b.status ~= 5 then
                                                       b.cancelled = (b.cancelled or 0) + 1
                                                       local total_done = (b.succeeded or 0) + (b.failed or 0) + (b.cancelled or 0)
                                                       if total_done >= b.total then
                                                           local batch_status
                                                           if (b.failed or 0) > 0 then batch_status = 5
                                                           elseif (b.cancelled or 0) > 0 then batch_status = 4
                                                           else batch_status = 2 end
                                                           b.status = batch_status
                                                           b.completedAt = now_ms
                                                           redis.call('SET', batch_key, cjson.encode(b))
                                                           redis.call('SREM', '{surefire}:batches:active', r.batchId)
                                                           redis.call('ZADD', '{surefire}:batches', now_ms, r.batchId)
                                                       else
                                                           redis.call('SET', batch_key, cjson.encode(b))
                                                       end
                                                   end
                                               end
                                           end

                                           cancelled_ids[#cancelled_ids + 1] = run_id
                                       elseif r.status == 1 then
                                           skipped = skipped + 1
                                       end
                                  end
                              end

                              -- Encode the ids separately: cjson.encode of an empty Lua table is
                              -- ambiguous (it picks {}, an object) and breaks typed deserialization
                              -- into string[]. Emit [] for the empty case explicitly; the non-empty
                              -- case runs through cjson so strings are escaped correctly.
                              local ids_json = '[]'
                              if #cancelled_ids > 0 then
                                  ids_json = cjson.encode(cancelled_ids)
                              end
                              return '{"cancelled":' .. ids_json .. ',"cleaned":' .. cleaned .. ',"skipped":' .. skipped .. '}'
                              """;

        var totalCancelled = new List<string>();
        var offset = 0;
        while (true)
        {
            var result = await EvaluateScriptAsync(script, [RoutingKey],
                [offset.ToString(), "Run expired past NotAfter deadline."]);
            var page = JsonSerializer.Deserialize(result.ToString()!,
                           SurefireJsonContext.Default.CancelExpiredRunsPayload)
                       ?? throw new InvalidOperationException("Cancel-expired payload was null.");

            totalCancelled.AddRange(page.Cancelled);

            if (page.Cancelled.Length == 0 && page.Cleaned == 0 && page.Skipped == 0)
            {
                break;
            }

            // Cancelled/cleaned entries were ZREM'd, shifting the set.
            // Only advance past skipped running entries, which stay in the expiring set
            // until executor/stale-run recovery handles them.
            offset += page.Skipped;
        }

        return totalCancelled;
    }

    public async Task PurgeAsync(DateTimeOffset threshold, CancellationToken cancellationToken = default)
    {
        var db = Db;
        var thresholdMs = threshold.ToUnixTimeMilliseconds();

        const string purgeRunsScript = """
                                       local threshold_ms = tonumber(ARGV[1])
                                       local batch_size = tonumber(ARGV[2])
                                       local purged = 0

                                       local function resolve_queue(job_name)
                                           local queue_name = redis.call('HGET', '{surefire}:job:' .. job_name, 'queue')
                                           if queue_name and queue_name ~= '' then
                                               return queue_name
                                           end
                                           return 'default'
                                       end

                                        local function cleanup_events(run_id)
                                            local event_ids = redis.call('ZRANGE', '{surefire}:events:run:' .. run_id, 0, -1)
                                            local batch_id = false
                                            local run_payload = redis.call('GET', '{surefire}:run:' .. run_id)
                                            if run_payload then
                                                local run = cjson.decode(run_payload)
                                                if run.batchId and run.batchId ~= cjson.null and run.batchId ~= '' then
                                                    batch_id = run.batchId
                                                end
                                            end

                                            for _, event_id in ipairs(event_ids) do
                                                redis.call('DEL', '{surefire}:event:' .. event_id)
                                                if batch_id then
                                                    redis.call('ZREM', '{surefire}:events:batch:' .. batch_id, event_id)
                                                end
                                            end

                                            if batch_id then
                                                local output_ids = redis.call('ZRANGE', '{surefire}:events:run_type:' .. run_id .. ':3', 0, -1)
                                                for _, output_id in ipairs(output_ids) do
                                                    redis.call('ZREM', '{surefire}:events:batch_output:' .. batch_id, output_id)
                                                end
                                            end

                                            local event_types_key = '{surefire}:events:run_types:' .. run_id
                                            local type_members = redis.call('SMEMBERS', event_types_key)
                                            for _, type_name in ipairs(type_members) do
                                                redis.call('DEL', '{surefire}:events:run_type:' .. run_id .. ':' .. type_name)
                                           end

                                           local event_attempts_key = '{surefire}:events:run_attempts:' .. run_id
                                           local attempt_members = redis.call('SMEMBERS', event_attempts_key)
                                           for _, attempt_name in ipairs(attempt_members) do
                                               redis.call('DEL', '{surefire}:events:run_attempt:' .. run_id .. ':' .. attempt_name)
                                           end

                                           redis.call('DEL', '{surefire}:events:run:' .. run_id)
                                           redis.call('DEL', event_types_key)
                                           redis.call('DEL', event_attempts_key)
                                       end

                                       local function decrement_timeline(status, completed_at)
                                           if not completed_at or completed_at == cjson.null then
                                               return
                                           end

                                           local field = nil
                                           if status == 2 then field = 'completed' end
                                           if status == 4 then field = 'cancelled' end
                                           if status == 5 then field = 'failed' end
                                           if not field then return end

                                           local minute = math.floor(tonumber(completed_at) / 60000)
                                           local bucket_key = '{surefire}:timeline:' .. tostring(minute)
                                           local current = tonumber(redis.call('HGET', bucket_key, field) or '0')
                                           if current <= 1 then
                                               redis.call('HDEL', bucket_key, field)
                                           else
                                               redis.call('HINCRBY', bucket_key, field, -1)
                                           end

                                           if redis.call('HLEN', bucket_key) == 0 then
                                               redis.call('DEL', bucket_key)
                                               redis.call('ZREM', '{surefire}:timeline:index', tostring(minute))
                                           end
                                       end

                                       local function child_member(created_at_ms, run_id)
                                           return string.format('%020d%s', tonumber(created_at_ms), run_id)
                                       end

                                       local function purge_run(run_id, r)
                                           local stats_key = '{surefire}:job_stats:' .. r.jobName
                                           if r.deduplicationId and r.deduplicationId ~= cjson.null and r.deduplicationId ~= '' then
                                               redis.call('DEL', '{surefire}:dedup:' .. r.jobName .. ':' .. r.deduplicationId)
                                           end

                                           redis.call('HINCRBY', stats_key, 'total_runs', -1)
                                           if r.startedAt and r.startedAt ~= cjson.null then
                                               redis.call('ZREM', '{surefire}:job_started:' .. r.jobName, run_id)
                                           end
                                           local status_num = tonumber(r.status)
                                           if status_num == 2 or status_num == 4 or status_num == 5 then
                                               redis.call('HINCRBY', stats_key, 'terminal_runs', -1)
                                               if status_num == 2 then
                                                   redis.call('HINCRBY', stats_key, 'succeeded_runs', -1)
                                                   if r.startedAt and r.startedAt ~= cjson.null and r.completedAt and r.completedAt ~= cjson.null then
                                                       redis.call('HINCRBYFLOAT', stats_key, 'duration_sum_ms', -(tonumber(r.completedAt) - tonumber(r.startedAt)))
                                                       redis.call('HINCRBY', stats_key, 'duration_count', -1)
                                                   end
                                               elseif status_num == 5 then
                                                   redis.call('HINCRBY', stats_key, 'failed_runs', -1)
                                               end
                                           end

                                           redis.call('SREM', '{surefire}:nonterminal:' .. r.jobName, run_id)

                                           local queue_name = resolve_queue(r.jobName)
                                           local pm = redis.call('GET', '{surefire}:pending_member:' .. run_id)
                                           if pm then
                                               redis.call('ZREM', '{surefire}:pending_q:' .. queue_name, pm)
                                           end
                                           redis.call('SREM', '{surefire}:running:' .. r.jobName, run_id)
                                           redis.call('SREM', '{surefire}:running_q:' .. queue_name, run_id)
                                           redis.call('ZREM', '{surefire}:heartbeat:running', run_id)

                                           redis.call('ZREM', '{surefire}:runs:created', run_id)
                                           redis.call('ZREM', '{surefire}:runs:completed', run_id)
                                           redis.call('ZREM', '{surefire}:runs:terminal', run_id)
                                           redis.call('ZREM', '{surefire}:runs:nonterminal', run_id)
                                           redis.call('ZREM', '{surefire}:job_runs:' .. r.jobName, run_id)

                                           local removed = redis.call('ZREM', '{surefire}:status:' .. tostring(r.status), run_id)
                                           if removed > 0 then
                                               redis.call('HINCRBY', '{surefire}:status_counts', tostring(r.status), -1)
                                           end

                                           if r.parentRunId and r.parentRunId ~= cjson.null and r.parentRunId ~= '' then
                                               redis.call('ZREM', '{surefire}:children:' .. r.parentRunId, child_member(r.createdAt, run_id))
                                           end
                                           if r.rootRunId and r.rootRunId ~= cjson.null and r.rootRunId ~= '' then
                                               redis.call('ZREM', '{surefire}:tree:' .. r.rootRunId, run_id)
                                           end
                                           if r.nodeName and r.nodeName ~= cjson.null and r.nodeName ~= '' then
                                               redis.call('SREM', '{surefire}:node_runs:' .. r.nodeName, run_id)
                                           end

                                           decrement_timeline(status_num, r.completedAt)
                                           cleanup_events(run_id)

                                           if r.batchId and r.batchId ~= cjson.null and r.batchId ~= '' then
                                               redis.call('ZREM', '{surefire}:batch_runs:' .. r.batchId, run_id)
                                           end
                                           redis.call('DEL', '{surefire}:run:' .. run_id)
                                           redis.call('DEL', '{surefire}:pending_member:' .. run_id)
                                           redis.call('ZREM', '{surefire}:expiring', run_id)
                                       end

                                       local terminal = redis.call('ZRANGEBYSCORE', '{surefire}:runs:completed', '-inf', threshold_ms, 'LIMIT', 0, batch_size)
                                       for _, run_id in ipairs(terminal) do
                                           local data = redis.call('GET', '{surefire}:run:' .. run_id)
                                           if data then
                                               local r = cjson.decode(data)
                                               local can_purge = true
                                               if r.batchId and r.batchId ~= cjson.null and r.batchId ~= '' then
                                                   local batch_data = redis.call('GET', '{surefire}:batch:' .. r.batchId)
                                                   if batch_data then
                                                       local batch = cjson.decode(batch_data)
                                                       local batch_status = tonumber(batch.status) or 0
                                                       local batch_completed = tonumber(batch.completedAt) or 0
                                                       if (batch_status ~= 2 and batch_status ~= 4 and batch_status ~= 5)
                                                           or batch.completedAt == nil
                                                           or batch.completedAt == cjson.null
                                                           or batch_completed >= threshold_ms then
                                                           can_purge = false
                                                       end
                                                   end
                                               end

                                               if can_purge then
                                                   purge_run(run_id, r)
                                                   purged = purged + 1
                                               end
                                           else
                                               redis.call('ZREM', '{surefire}:runs:completed', run_id)
                                               redis.call('ZREM', '{surefire}:runs:terminal', run_id)
                                               purged = purged + 1
                                           end
                                       end

                                       local pending_ids = redis.call('ZRANGEBYSCORE', '{surefire}:status:0', '-inf', threshold_ms, 'LIMIT', 0, batch_size)
                                       for _, run_id in ipairs(pending_ids) do
                                           if purged >= batch_size then break end
                                           local data = redis.call('GET', '{surefire}:run:' .. run_id)
                                           if data then
                                               local r = cjson.decode(data)
                                               if r.status == 0 and r.notBefore < threshold_ms then
                                                   purge_run(run_id, r)
                                                   purged = purged + 1
                                               end
                                           else
                                               local removed = redis.call('ZREM', '{surefire}:status:0', run_id)
                                               if removed > 0 then
                                                   redis.call('HINCRBY', '{surefire}:status_counts', '0', -1)
                                               end
                                               redis.call('ZREM', '{surefire}:runs:nonterminal', run_id)
                                           end
                                       end

                                       local retrying_offset = 0
                                       local retrying_batch = 100
                                       while retrying_offset < redis.call('ZCARD', '{surefire}:status:3') do
                                           if purged >= batch_size then break end
                                           local retrying_ids = redis.call('ZRANGE', '{surefire}:status:3', retrying_offset, retrying_offset + retrying_batch - 1)
                                           if #retrying_ids == 0 then break end
                                           local batch_purged = 0
                                           for _, run_id in ipairs(retrying_ids) do
                                               if purged >= batch_size then break end
                                               local data = redis.call('GET', '{surefire}:run:' .. run_id)
                                               if data then
                                                   local r = cjson.decode(data)
                                                   if r.status == 0 and r.notBefore < threshold_ms then
                                                       purge_run(run_id, r)
                                                       purged = purged + 1
                                                       batch_purged = batch_purged + 1
                                                   end
                                               else
                                                   local removed = redis.call('ZREM', '{surefire}:status:3', run_id)
                                                   if removed > 0 then
                                                       redis.call('HINCRBY', '{surefire}:status_counts', '3', -1)
                                                   end
                                                   redis.call('ZREM', '{surefire}:runs:nonterminal', run_id)
                                                   batch_purged = batch_purged + 1
                                               end
                                           end
                                           retrying_offset = retrying_offset + retrying_batch - batch_purged
                                       end

                                       -- Purge terminal batches whose runs have all been purged
                                       local old_batches = redis.call('ZRANGEBYSCORE', '{surefire}:batches', '-inf', threshold_ms)
                                       for _, bid in ipairs(old_batches) do
                                           local bdata = redis.call('GET', '{surefire}:batch:' .. bid)
                                           if bdata then
                                               local b = cjson.decode(bdata)
                                                if (b.status == 2 or b.status == 4 or b.status == 5)
                                                    and redis.call('ZCARD', '{surefire}:batch_runs:' .. bid) == 0 then
                                                    redis.call('DEL', '{surefire}:batch:' .. bid)
                                                    redis.call('DEL', '{surefire}:batch_runs:' .. bid)
                                                    redis.call('DEL', '{surefire}:events:batch:' .. bid)
                                                    redis.call('DEL', '{surefire}:events:batch_output:' .. bid)
                                                    redis.call('ZREM', '{surefire}:batches', bid)
                                                    purged = purged + 1
                                                end
                                           else
                                               redis.call('ZREM', '{surefire}:batches', bid)
                                           end
                                       end

                                       return purged
                                       """;

        const string purgeEntitiesScript = """
                                           local threshold_ms = tonumber(ARGV[1])
                                           local timeline_threshold_minute = math.floor(threshold_ms / 60000) - 1

                                           local function purge_stale_in_set(set_key, prefix)
                                               local cursor = '0'
                                               repeat
                                                   local page = redis.call('SSCAN', set_key, cursor, 'COUNT', 200)
                                                   cursor = page[1]
                                                   local members = page[2]
                                                   for _, name in ipairs(members) do
                                                       local ekey = prefix .. name
                                                       local hb = redis.call('HGET', ekey, 'last_heartbeat_at')
                                                       if hb and tonumber(hb) < threshold_ms then
                                                           redis.call('DEL', ekey)
                                                           redis.call('SREM', set_key, name)
                                                       end
                                                   end
                                               until cursor == '0'
                                           end

                                           purge_stale_in_set('{surefire}:queues', '{surefire}:queue:')
                                           purge_stale_in_set('{surefire}:rate_limits', '{surefire}:rate_limit:')
                                           purge_stale_in_set('{surefire}:nodes', '{surefire}:node:')

                                           local job_cursor = '0'
                                           repeat
                                               local page = redis.call('SSCAN', '{surefire}:jobs', job_cursor, 'COUNT', 200)
                                               job_cursor = page[1]
                                               local members = page[2]
                                               for _, name in ipairs(members) do
                                                   local jkey = '{surefire}:job:' .. name
                                                   local hb = redis.call('HGET', jkey, 'last_heartbeat_at')
                                                   if hb and tonumber(hb) < threshold_ms then
                                                       if redis.call('SCARD', '{surefire}:nonterminal:' .. name) == 0 then
                                                           redis.call('DEL', jkey)
                                                           redis.call('SREM', '{surefire}:jobs', name)
                                                       end
                                                   end
                                               end
                                           until job_cursor == '0'

                                           if timeline_threshold_minute >= 0 then
                                               local stale_minutes = redis.call('ZRANGEBYSCORE', '{surefire}:timeline:index', '-inf', timeline_threshold_minute)
                                               for _, minute_str in ipairs(stale_minutes) do
                                                   redis.call('DEL', '{surefire}:timeline:' .. minute_str)
                                                   redis.call('ZREM', '{surefire}:timeline:index', minute_str)
                                               end
                                           end

                                           return 1
                                           """;

        // Phase 1: Purge runs in batches
        int purged;
        do
        {
            purged = (int)await EvaluateScriptAsync(purgeRunsScript,
                [RoutingKey],
                [thresholdMs.ToString(), "1000"]);
        } while (purged > 0);

        // Phase 2: Purge stale entities (once)
        await EvaluateScriptAsync(purgeEntitiesScript,
            [RoutingKey],
            [thresholdMs.ToString()]);
    }

    public async Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60,
        CancellationToken cancellationToken = default)
    {
        if (bucketMinutes <= 0)
        {
            bucketMinutes = 60;
        }

        var db = Db;
        var sinceTime = since ?? timeProvider.GetUtcNow().AddHours(-24);
        var now = timeProvider.GetUtcNow();

        const string activeNodesScript = """
                                         local members = redis.call('SMEMBERS', '{surefire}:nodes')
                                         local threshold = tonumber(ARGV[1])
                                         local count = 0
                                         for _, name in ipairs(members) do
                                             local hb = redis.call('HGET', '{surefire}:node:' .. name, 'last_heartbeat_at')
                                             if hb and tonumber(hb) >= threshold then
                                                 count = count + 1
                                             end
                                         end
                                         return count
                                         """;
        var heartbeatThresholdMs = now.AddMinutes(-2).ToUnixTimeMilliseconds();

        var statusCountsTask = db.HashGetAllAsync($"{P}status_counts");
        var jobCountTask = db.SetLengthAsync($"{P}jobs");
        var nodeCountTask = EvaluateScriptAsync(activeNodesScript, [RoutingKey], [heartbeatThresholdMs.ToString()]);
        await Task.WhenAll(statusCountsTask, jobCountTask, nodeCountTask);

        var statusCounts = (await statusCountsTask).ToDictionary(
            h => h.Name.ToString(), h => (int)h.Value);

        var runsByStatus = new Dictionary<string, int>();
        foreach (var status in Enum.GetValues<JobStatus>())
        {
            var key = ((int)status).ToString();
            var count = statusCounts.GetValueOrDefault(key, 0);
            if (count > 0)
            {
                runsByStatus[status.ToString()] = count;
            }
        }

        var totalRuns = statusCounts.Values.Sum();
        var completedCount = statusCounts.GetValueOrDefault("2", 0);
        var terminalCount = completedCount
                            + statusCounts.GetValueOrDefault("4", 0)
                            + statusCounts.GetValueOrDefault("5", 0);
        var activeRuns = statusCounts.GetValueOrDefault("0", 0)
                         + statusCounts.GetValueOrDefault("1", 0);
        var successRate = terminalCount > 0 ? completedCount / (double)terminalCount : 0.0;

        var bucketSpan = TimeSpan.FromMinutes(bucketMinutes);
        var buckets = new List<TimelineBucket>();
        var bucketStart = sinceTime;
        var statusValues = Enum.GetValues<JobStatus>();

        while (bucketStart < now)
        {
            var bucketEnd = bucketStart + bucketSpan;

            var minScore = bucketStart.ToUnixTimeMilliseconds();
            var maxScore = bucketEnd.ToUnixTimeMilliseconds();

            var batch = db.CreateBatch();
            var countTasks = statusValues.ToDictionary(
                status => status,
                status => batch.SortedSetLengthAsync($"{P}status:{(int)status}", minScore, maxScore, Exclude.Stop));
            batch.Execute();
            await Task.WhenAll(countTasks.Values);

            buckets.Add(new()
            {
                Start = bucketStart,
                Pending = (int)await countTasks[JobStatus.Pending],
                Running = (int)await countTasks[JobStatus.Running],
                Succeeded = (int)await countTasks[JobStatus.Succeeded],
                Cancelled = (int)await countTasks[JobStatus.Cancelled],
                Failed = (int)await countTasks[JobStatus.Failed]
            });
            bucketStart = bucketEnd;
        }

        return new()
        {
            TotalJobs = (int)await jobCountTask,
            TotalRuns = totalRuns,
            ActiveRuns = activeRuns,
            SuccessRate = successRate,
            NodeCount = (int)await nodeCountTask,
            RunsByStatus = runsByStatus,
            Timeline = buckets
        };
    }

    public async Task<JobStats> GetJobStatsAsync(string jobName, CancellationToken cancellationToken = default)
    {
        var db = Db;
        var statsTask = db.HashGetAllAsync($"{P}job_stats:{jobName}");
        var lastStartedTask = db.SortedSetRangeByRankWithScoresAsync($"{P}job_started:{jobName}", 0, 0,
            Order.Descending);
        await Task.WhenAll(statsTask, lastStartedTask);

        var stats = (await statsTask).ToDictionary(h => h.Name.ToString(), h => h.Value.ToString(),
            StringComparer.Ordinal);

        static int GetInt(Dictionary<string, string> values, string key) =>
            values.TryGetValue(key, out var raw) && int.TryParse(raw, out var parsed) ? parsed : 0;

        static double GetDouble(Dictionary<string, string> values, string key) =>
            values.TryGetValue(key, out var raw)
            && double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : 0d;

        var total = Math.Max(0, GetInt(stats, "total_runs"));
        var succeeded = Math.Max(0, GetInt(stats, "succeeded_runs"));
        var failed = Math.Max(0, GetInt(stats, "failed_runs"));
        var terminal = Math.Max(0, GetInt(stats, "terminal_runs"));
        var durationSumMs = GetDouble(stats, "duration_sum_ms");
        var durationCount = Math.Max(0, GetInt(stats, "duration_count"));

        var lastStarted = await lastStartedTask;
        var lastStartedAtMs = lastStarted.Length > 0 ? (long)lastStarted[0].Score : 0L;

        return new()
        {
            TotalRuns = total,
            SucceededRuns = succeeded,
            FailedRuns = failed,
            SuccessRate = terminal > 0 ? succeeded / (double)terminal : 0.0,
            AvgDuration = durationCount > 0
                ? TimeSpan.FromMilliseconds(durationSumMs / durationCount)
                : null,
            LastRunAt = lastStartedAtMs > 0
                ? DateTimeOffset.FromUnixTimeMilliseconds(lastStartedAtMs)
                : null
        };
    }

    public async Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(
        CancellationToken cancellationToken = default)
    {
        var db = Db;
        var queueNames = new HashSet<string>(StringComparer.Ordinal);

        var explicitQueues = await GetQueuesAsync(cancellationToken);
        foreach (var queue in explicitQueues)
        {
            queueNames.Add(queue.Name);
        }

        var jobMembers = await db.SetMembersAsync($"{P}jobs");
        if (jobMembers.Length > 0)
        {
            var jobBatch = db.CreateBatch();
            var queueFieldTasks = jobMembers
                .Select(jobName => jobBatch.HashGetAsync($"{P}job:{jobName}", "queue"))
                .ToArray();
            jobBatch.Execute();
            await Task.WhenAll(queueFieldTasks);

            foreach (var task in queueFieldTasks)
            {
                var queueField = await task;
                queueNames.Add(queueField.IsNullOrEmpty ? "default" : queueField.ToString());
            }
        }

        var runningTasks = queueNames.ToDictionary(qn => qn, qn => db.SetLengthAsync($"{P}running_q:{qn}"));
        var pendingTasks = queueNames.ToDictionary(qn => qn, qn => db.SortedSetLengthAsync($"{P}pending_q:{qn}"));
        await Task.WhenAll(runningTasks.Values.Concat(pendingTasks.Values));

        var result = new Dictionary<string, QueueStats>();
        foreach (var queueName in queueNames)
        {
            result[queueName] = new()
            {
                PendingCount = (int)await pendingTasks[queueName],
                RunningCount = (int)await runningTasks[queueName]
            };
        }

        return result;
    }

    /// <summary>
    ///     Classifies transient Redis failures that should be retried by callers
    ///     (executor loop, BatchedEventWriter, terminal-transition retry wrapper).
    ///     Mirrors the Hangfire/Sidekiq/BullMQ policy: connection drops, timeouts,
    ///     and temporary server-side unavailability (LOADING/BUSY/MASTERDOWN/CLUSTERDOWN)
    ///     are recoverable. MOVED/ASK slot redirects and NOSCRIPT are intentionally
    ///     excluded — StackExchange.Redis handles MOVED/ASK internally and NOSCRIPT
    ///     is handled at the call site via script reload.
    /// </summary>
    public bool IsTransientException(Exception ex) => ex switch
    {
        RedisConnectionException => true,
        RedisTimeoutException => true,
        RedisServerException server
            when server.Message.StartsWith("LOADING", StringComparison.Ordinal)
                 || server.Message.StartsWith("BUSY", StringComparison.Ordinal)
                 || server.Message.StartsWith("MASTERDOWN", StringComparison.Ordinal)
                 || server.Message.StartsWith("CLUSTERDOWN", StringComparison.Ordinal) => true,
        SocketException => true,
        IOException => true,
        _ => false
    };

    private static string EncodeChildMember(DateTimeOffset createdAt, string runId)
    {
        var createdAtMs = createdAt.ToUnixTimeMilliseconds();
        return createdAtMs.ToString($"D{ChildMemberTimestampWidth}", CultureInfo.InvariantCulture) + runId;
    }

    private static bool TryDecodeChildMember(RedisValue member, out long createdAtMs, out string runId)
    {
        createdAtMs = default;
        runId = string.Empty;

        if (member.IsNullOrEmpty)
        {
            return false;
        }

        var value = (string)member!;
        if (value.Length <= ChildMemberTimestampWidth)
        {
            return false;
        }

        if (!long.TryParse(value.AsSpan(0, ChildMemberTimestampWidth), NumberStyles.None,
                CultureInfo.InvariantCulture, out createdAtMs))
        {
            return false;
        }

        runId = value[ChildMemberTimestampWidth..];
        return runId.Length > 0;
    }

    private async Task<PagedResult<JobRun>> GetRunsFromChildrenLexIndexWithFilterAsync(
        RedisKey key,
        RunFilter filter,
        int skip,
        int take,
        RedisValue lexMin,
        RedisValue lexMax,
        Exclude lexExclude,
        CancellationToken cancellationToken = default)
    {
        var requiresResort = filter.OrderBy != RunOrderBy.CreatedAt;
        var matchedCount = 0;
        var page = new List<JobRun>(take);
        if (skip > int.MaxValue - take)
        {
            throw new ArgumentOutOfRangeException(nameof(skip), "The sum of skip and take is too large.");
        }

        var topLimit = skip + take;
        var topRuns = requiresResort ? new List<JobRun>(Math.Min(topLimit, 128)) : null;
        IComparer<JobRun>? runOrderComparer = requiresResort ? new RunOrderComparer(filter.OrderBy) : null;
        var offset = 0L;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var members = await Db.SortedSetRangeByValueAsync(
                key,
                lexMin,
                lexMax,
                lexExclude,
                Order.Descending,
                offset,
                FilterScanChunkSize);

            if (members.Length == 0)
            {
                break;
            }

            offset += members.Length;

            var ids = new List<RedisValue>(members.Length);
            foreach (var member in members)
            {
                if (!TryDecodeChildMember(member, out _, out var runId))
                {
                    throw new FormatException($"Malformed child index member: '{member}'.");
                }

                ids.Add(runId);
            }

            if (ids.Count == 0)
            {
                continue;
            }

            var runs = await LoadRunsByIdsAsync([.. ids]);
            foreach (var run in runs)
            {
                if (!MatchesFilter(run, filter))
                {
                    continue;
                }

                if (requiresResort)
                {
                    InsertIntoTopRuns(topRuns!, run, runOrderComparer!, topLimit);
                }
                else if (matchedCount >= skip && page.Count < take)
                {
                    page.Add(run);
                }

                matchedCount++;
            }
        }

        if (!requiresResort)
        {
            return new() { Items = page, TotalCount = matchedCount };
        }

        var sorted = topRuns!;
        return new()
        {
            Items = sorted.Skip(skip).Take(take).ToList(),
            TotalCount = matchedCount
        };
    }

    private async Task<long> GetSortedSetLexCountAsync(RedisKey key, RedisValue min, RedisValue max, Exclude exclude)
    {
        static RedisValue ToLexBound(RedisValue value, bool isMin, Exclude ex)
        {
            if (value.IsNull)
            {
                return isMin ? "-" : "+";
            }

            var isExclusive = isMin
                ? ex.HasFlag(Exclude.Start)
                : ex.HasFlag(Exclude.Stop);

            return (isExclusive ? "(" : "[") + value;
        }

        var minBound = ToLexBound(min, true, exclude);
        var maxBound = ToLexBound(max, false, exclude);
        var result = await Db.ExecuteAsync("ZLEXCOUNT", key, minBound, maxBound);
        return (long)result;
    }

    private async Task<PagedResult<JobRun>> GetRunsFromSortedSetWithFilterAsync(
        RedisKey key,
        RunFilter filter,
        int skip,
        int take,
        RunOrderBy indexOrderBy,
        double minScore = double.NegativeInfinity,
        double maxScore = double.PositiveInfinity,
        Exclude exclude = Exclude.None,
        Order order = Order.Descending,
        CancellationToken cancellationToken = default)
    {
        var requiresResort = filter.OrderBy != indexOrderBy;
        var matchedCount = 0;
        var page = new List<JobRun>(take);
        if (skip > int.MaxValue - take)
        {
            throw new ArgumentOutOfRangeException(nameof(skip), "The sum of skip and take is too large.");
        }

        var topLimit = skip + take;
        var topRuns = requiresResort ? new List<JobRun>(Math.Min(topLimit, 128)) : null;
        IComparer<JobRun>? runOrderComparer = requiresResort ? new RunOrderComparer(filter.OrderBy) : null;
        var offset = 0L;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var ids = await Db.SortedSetRangeByScoreAsync(
                key,
                minScore,
                maxScore,
                exclude,
                order,
                offset,
                FilterScanChunkSize);
            if (ids.Length == 0)
            {
                break;
            }

            offset += ids.Length;

            var runs = await LoadRunsByIdsAsync(ids);
            foreach (var run in runs)
            {
                if (!MatchesFilter(run, filter))
                {
                    continue;
                }

                if (requiresResort)
                {
                    InsertIntoTopRuns(topRuns!, run, runOrderComparer!, topLimit);
                }
                else if (matchedCount >= skip && page.Count < take)
                {
                    page.Add(run);
                }

                matchedCount++;
            }
        }

        if (!requiresResort)
        {
            return new() { Items = page, TotalCount = matchedCount };
        }

        var sorted = topRuns!;
        return new()
        {
            Items = sorted.Skip(skip).Take(take).ToList(),
            TotalCount = matchedCount
        };
    }

    private static void InsertIntoTopRuns(List<JobRun> topRuns, JobRun candidate, IComparer<JobRun> comparer,
        int limit)
    {
        if (limit <= 0)
        {
            return;
        }

        var index = topRuns.BinarySearch(candidate, comparer);
        if (index < 0)
        {
            index = ~index;
        }

        if (topRuns.Count == limit && index >= limit)
        {
            return;
        }

        topRuns.Insert(index, candidate);
        if (topRuns.Count > limit)
        {
            topRuns.RemoveAt(topRuns.Count - 1);
        }
    }

    private async Task<List<JobRun>> LoadRunsByIdsAsync(RedisValue[] runIds)
    {
        if (runIds.Length == 0)
        {
            return [];
        }

        var batch = Db.CreateBatch();
        var fetchTasks = runIds.Select(id => batch.StringGetAsync($"{P}run:{id}")).ToArray();
        batch.Execute();
        await Task.WhenAll(fetchTasks);

        var runs = new List<JobRun>(fetchTasks.Length);
        foreach (var task in fetchTasks)
        {
            var json = await task;
            if (json.IsNullOrEmpty)
            {
                continue;
            }

            runs.Add(DeserializeRun(json!));
        }

        return runs;
    }

    private async Task<IReadOnlyList<RunEvent>> GetBatchEventsCoreAsync(string batchId, long sinceEventId, int take,
        RunEventType? eventType)
    {
        if (take <= 0)
        {
            return [];
        }

        var db = Db;
        var indexKey = eventType is { } type
            ? (RedisKey)$"{P}events:batch_output:{batchId}"
            : (RedisKey)$"{P}events:batch:{batchId}";
        var mergedIds = (await db.SortedSetRangeByScoreAsync(
                indexKey,
                sinceEventId,
                double.PositiveInfinity,
                Exclude.Start,
                Order.Ascending,
                0,
                take))
            .Select(v => v.ToString())
            .Where(v => !string.IsNullOrEmpty(v))
            .ToArray();

        if (mergedIds.Length == 0)
        {
            return [];
        }

        var payloadBatch = db.CreateBatch();
        var payloadTasks = mergedIds
            .Select(id => payloadBatch.StringGetAsync($"{P}event:{id}"))
            .ToArray();
        payloadBatch.Execute();
        await Task.WhenAll(payloadTasks);

        var events = new List<RunEvent>(payloadTasks.Length);
        foreach (var payloadTask in payloadTasks)
        {
            var jsonValue = await payloadTask;
            if (jsonValue.IsNullOrEmpty)
            {
                continue;
            }

            try
            {
                var evt = JsonSerializer.Deserialize(jsonValue.ToString(), SurefireJsonContext.Default.RunEvent)
                          ?? throw new InvalidOperationException("Event payload was null.");
                events.Add(evt);
            }
            catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException)
            {
                Log.MalformedBatchOutputPayload(logger, ex);
            }
        }

        events.Sort((a, b) => a.Id.CompareTo(b.Id));
        return events;
    }

    private async Task<RedisResult> EvaluateScriptAsync(string script, RedisKey[] keys, RedisValue[] args)
    {
        var hash = await EnsureScriptLoadedAsync(script);
        try
        {
            return await Db.ScriptEvaluateAsync(hash, keys, args);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("NOSCRIPT", StringComparison.OrdinalIgnoreCase))
        {
            hash = await EnsureScriptLoadedAsync(script, true);
            return await Db.ScriptEvaluateAsync(hash, keys, args);
        }
    }

    private async Task<byte[]> EnsureScriptLoadedAsync(string script, bool forceReload = false)
    {
        if (!forceReload && _scriptHashes.TryGetValue(script, out var cachedHash))
        {
            return cachedHash;
        }

        await _scriptLoadLock.WaitAsync();
        try
        {
            if (!forceReload && _scriptHashes.TryGetValue(script, out cachedHash))
            {
                return cachedHash;
            }

            var loadTasks = new List<Task<byte[]>>();
            foreach (var endpoint in connection.GetEndPoints())
            {
                var server = connection.GetServer(endpoint);
                if (!server.IsConnected || server.IsReplica)
                {
                    continue;
                }

                loadTasks.Add(server.ScriptLoadAsync(script));
            }

            byte[]? loadedHash = null;
            if (loadTasks.Count > 0)
            {
                var hashes = await Task.WhenAll(loadTasks);
                loadedHash = hashes[0];
            }

            if (loadedHash is null)
            {
                throw new InvalidOperationException("No connected primary Redis server available for script loading.");
            }

            _scriptHashes[script] = loadedHash;
            return loadedHash;
        }
        finally
        {
            _scriptLoadLock.Release();
        }
    }

    private async Task CreateRunsCoreAsync(IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken,
        string? batchJson = null)
    {
        if (runs.Count == 0 && (initialEvents is null || initialEvents.Count == 0))
        {
            if (batchJson is { })
            {
                // Empty batch: persist only the batch record with no runs
                await EvaluateScriptAsync(CreateRunsScript,
                    [RoutingKey],
                    [(RedisValue)"[]", (RedisValue)"[]", (RedisValue)batchJson]);
            }

            return;
        }

        var runsJson = JsonSerializer.Serialize(
            new(runs),
            SurefireJsonContext.Default.ListJobRun);
        var initialEventsJson = JsonSerializer.Serialize(
            initialEvents is null ? [] : new List<RunEvent>(initialEvents),
            SurefireJsonContext.Default.ListRunEvent);
        await EvaluateScriptAsync(CreateRunsScript,
            [RoutingKey],
            [(RedisValue)runsJson, (RedisValue)initialEventsJson, (RedisValue)(batchJson ?? "")]);
    }

    private async Task<bool> TryCreateRunCoreAsync(JobRun run, int? maxActiveForJob,
        DateTimeOffset? lastCronFireAt,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        const string script = PendingMemberHelper + """
                                                    local run_key = '{surefire}:run:' .. ARGV[1]
                                                    if redis.call('EXISTS', run_key) == 1 then
                                                        return 0
                                                    end

                                                    local job_name = ARGV[2]
                                                    local dedup_id = ARGV[3]
                                                    local max_active = ARGV[4]
                                                    local run_json = ARGV[5]
                                                    local parent_run_id = ARGV[6]
                                                    local root_run_id = ARGV[7]
                                                    local node_name_arg = ARGV[8]
                                                    local last_cron_fire_at = ARGV[9]
                                                    local initial_events = cjson.decode(ARGV[10])

                                                     local function append_events(events)
                                                         if not events then
                                                             return
                                                         end
                                                         local seq_key = '{surefire}:event_seq'
                                                         local batch_ids = {}
                                                         for _, evt in ipairs(events) do
                                                             local id = redis.call('INCR', seq_key)
                                                             evt.id = id
                                                             local payload = cjson.encode(evt)
                                                             local id_str = tostring(id)
                                                            redis.call('SET', '{surefire}:event:' .. id_str, payload)
                                                            redis.call('ZADD', '{surefire}:events:run:' .. evt.runId, id, id_str)
                                                             redis.call('ZADD', '{surefire}:events:run_type:' .. evt.runId .. ':' .. tostring(evt.eventType), id, id_str)
                                                             redis.call('ZADD', '{surefire}:events:run_attempt:' .. evt.runId .. ':' .. tostring(evt.attempt), id, id_str)
                                                             redis.call('SADD', '{surefire}:events:run_types:' .. evt.runId, tostring(evt.eventType))
                                                             redis.call('SADD', '{surefire}:events:run_attempts:' .. evt.runId, tostring(evt.attempt))

                                                             local batch_id = batch_ids[evt.runId]
                                                             if batch_id == nil then
                                                                 local run_payload = redis.call('GET', '{surefire}:run:' .. evt.runId)
                                                                 if run_payload then
                                                                     local run = cjson.decode(run_payload)
                                                                     batch_id = run.batchId
                                                                 else
                                                                     batch_id = false
                                                                 end
                                                                 batch_ids[evt.runId] = batch_id
                                                             end

                                                             if batch_id and batch_id ~= cjson.null and batch_id ~= '' then
                                                                 redis.call('ZADD', '{surefire}:events:batch:' .. batch_id, id, id_str)
                                                                 if evt.eventType == 3 then
                                                                     redis.call('ZADD', '{surefire}:events:batch_output:' .. batch_id, id, id_str)
                                                                 end
                                                             end
                                                         end
                                                     end

                                                    local function is_terminal(status)
                                                        return status == 2 or status == 4 or status == 5
                                                    end

                                                    local function child_member(created_at_ms, run_id)
                                                        return string.format('%020d%s', tonumber(created_at_ms), run_id)
                                                    end

                                                    local function increment_job_stats_for_new_run(run)
                                                        local stats_key = '{surefire}:job_stats:' .. run.jobName
                                                        redis.call('HINCRBY', stats_key, 'total_runs', 1)

                                                        if run.startedAt and run.startedAt ~= cjson.null then
                                                            redis.call('ZADD', '{surefire}:job_started:' .. run.jobName, tonumber(run.startedAt), ARGV[1])
                                                        end

                                                        if is_terminal(run.status) then
                                                            redis.call('HINCRBY', stats_key, 'terminal_runs', 1)
                                                            if run.status == 2 then
                                                                redis.call('HINCRBY', stats_key, 'succeeded_runs', 1)
                                                                if run.startedAt and run.startedAt ~= cjson.null and run.completedAt and run.completedAt ~= cjson.null then
                                                                    redis.call('HINCRBYFLOAT', stats_key, 'duration_sum_ms', tonumber(run.completedAt) - tonumber(run.startedAt))
                                                                    redis.call('HINCRBY', stats_key, 'duration_count', 1)
                                                                end
                                                            elseif run.status == 5 then
                                                                redis.call('HINCRBY', stats_key, 'failed_runs', 1)
                                                            end
                                                        end
                                                    end

                                                    if dedup_id ~= '' then
                                                        local existing_id = redis.call('GET', '{surefire}:dedup:' .. job_name .. ':' .. dedup_id)
                                                        if existing_id then
                                                            local existing_data = redis.call('GET', '{surefire}:run:' .. existing_id)
                                                            if existing_data then
                                                                local existing = cjson.decode(existing_data)
                                                                if existing.status ~= 2 and existing.status ~= 4 and existing.status ~= 5 then
                                                                    return 0
                                                                end
                                                            end
                                                            redis.call('DEL', '{surefire}:dedup:' .. job_name .. ':' .. dedup_id)
                                                        end
                                                    end

                                                    if max_active ~= '' then
                                                        local max = tonumber(max_active)
                                                        local job_key = '{surefire}:job:' .. job_name
                                                        local enabled = redis.call('HGET', job_key, 'is_enabled')
                                                        if enabled == '0' then
                                                            return 0
                                                        end
                                                        local active_count = redis.call('SCARD', '{surefire}:nonterminal:' .. job_name)
                                                        if active_count >= max then
                                                            return 0
                                                        end
                                                    end

                                                    local run = cjson.decode(run_json)

                                                    local q_field = redis.call('HGET', '{surefire}:job:' .. job_name, 'queue')
                                                    local qname = (q_field and q_field ~= '') and q_field or 'default'
                                                    run.priority = tonumber(run.priority) or 0

                                                    redis.call('SET', run_key, cjson.encode(run))
                                                    increment_job_stats_for_new_run(run)
                                                    redis.call('SADD', '{surefire}:nonterminal:' .. job_name, ARGV[1])

                                                    if dedup_id ~= '' then
                                                        redis.call('SET', '{surefire}:dedup:' .. job_name .. ':' .. dedup_id, ARGV[1])
                                                    end

                                                    if run.notAfter and run.notAfter ~= cjson.null then
                                                        redis.call('ZADD', '{surefire}:expiring', tonumber(run.notAfter), ARGV[1])
                                                    end

                                                    -- Secondary indexes
                                                    redis.call('ZADD', '{surefire}:runs:created', run.createdAt, ARGV[1])
                                                    redis.call('ZADD', '{surefire}:job_runs:' .. job_name, run.createdAt, ARGV[1])
                                                    redis.call('ZADD', '{surefire}:status:' .. tostring(run.status), run.createdAt, ARGV[1])
                                                    redis.call('HINCRBY', '{surefire}:status_counts', tostring(run.status), 1)
                                                    if run.status == 2 or run.status == 4 or run.status == 5 then
                                                        redis.call('ZADD', '{surefire}:runs:terminal', run.createdAt, ARGV[1])
                                                    else
                                                        redis.call('ZADD', '{surefire}:runs:nonterminal', run.createdAt, ARGV[1])
                                                    end
                                                    if parent_run_id ~= '' then
                                                        redis.call('ZADD', '{surefire}:children:' .. parent_run_id, 0, child_member(run.createdAt, ARGV[1]))
                                                    end
                                                    if root_run_id ~= '' then
                                                        redis.call('ZADD', '{surefire}:tree:' .. root_run_id, run.createdAt, ARGV[1])
                                                    end
                                                    if run.batchId and run.batchId ~= cjson.null and run.batchId ~= '' then
                                                        redis.call('ZADD', '{surefire}:batch_runs:' .. run.batchId, run.createdAt, ARGV[1])
                                                    end
                                                    if tonumber(run.status) == 1 then
                                                        redis.call('SADD', '{surefire}:running:' .. job_name, ARGV[1])
                                                        redis.call('SADD', '{surefire}:running_q:' .. qname, ARGV[1])
                                                        local hb = (run.lastHeartbeatAt and run.lastHeartbeatAt ~= cjson.null) and tonumber(run.lastHeartbeatAt) or tonumber(run.createdAt)
                                                        redis.call('ZADD', '{surefire}:heartbeat:running', hb, ARGV[1])
                                                        if node_name_arg ~= '' then
                                                            redis.call('SADD', '{surefire}:node_runs:' .. node_name_arg, ARGV[1])
                                                        end
                                                    end

                                                    if run.status == 0 then
                                                        local member = pending_member(run.priority, run.notBefore, ARGV[1])
                                                        redis.call('ZADD', '{surefire}:pending_q:' .. qname, 0, member)
                                                        redis.call('SET', '{surefire}:pending_member:' .. ARGV[1], member)
                                                    end

                                                    if (run.status == 2 or run.status == 4 or run.status == 5)
                                                        and run.completedAt and run.completedAt ~= cjson.null then
                                                        local minute = math.floor(tonumber(run.completedAt) / 60000)
                                                        local field = run.status == 2 and 'completed' or (run.status == 4 and 'cancelled' or 'failed')
                                                        redis.call('HINCRBY', '{surefire}:timeline:' .. tostring(minute), field, 1)
                                                        redis.call('ZADD', '{surefire}:timeline:index', minute, tostring(minute))
                                                    end

                                                    if last_cron_fire_at ~= '' then
                                                        redis.call('HSET', '{surefire}:job:' .. job_name, 'last_cron_fire_at', last_cron_fire_at)
                                                    end

                                                    append_events(initial_events)

                                                    return 1
                                                    """;

        var result = await EvaluateScriptAsync(script,
            [RoutingKey],
            [
                run.Id,
                run.JobName,
                run.DeduplicationId ?? "",
                maxActiveForJob?.ToString() ?? "",
                SerializeRun(run),
                run.ParentRunId ?? "",
                run.RootRunId ?? "",
                run.NodeName ?? "",
                lastCronFireAt?.ToUnixTimeMilliseconds().ToString() ?? "",
                JsonSerializer.Serialize(
                    initialEvents is null ? [] : new List<RunEvent>(initialEvents),
                    SurefireJsonContext.Default.ListRunEvent)
            ]);

        return (int)result == 1;
    }

    private static bool MatchesFilter(JobRun run, RunFilter filter)
    {
        if (filter.Status is { } && run.Status != filter.Status.Value)
        {
            return false;
        }

        if (filter.JobName is { })
        {
            if (filter.ExactJobName)
            {
                if (run.JobName != filter.JobName)
                {
                    return false;
                }
            }
            else
            {
                if (!run.JobName.Contains(filter.JobName, StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }
        }

        if (filter.ParentRunId is { } && run.ParentRunId != filter.ParentRunId)
        {
            return false;
        }

        if (filter.RootRunId is { } && run.RootRunId != filter.RootRunId)
        {
            return false;
        }

        if (filter.NodeName is { } && run.NodeName != filter.NodeName)
        {
            return false;
        }

        if (filter.CreatedAfter is { } && run.CreatedAt <= filter.CreatedAfter.Value)
        {
            return false;
        }

        if (filter.CreatedBefore is { } && run.CreatedAt >= filter.CreatedBefore.Value)
        {
            return false;
        }

        if (filter.CompletedAfter is { } && (run.CompletedAt is null || run.CompletedAt <= filter.CompletedAfter.Value))
        {
            return false;
        }

        if (filter.LastHeartbeatBefore is { } &&
            (run.LastHeartbeatAt is null || run.LastHeartbeatAt >= filter.LastHeartbeatBefore.Value))
        {
            return false;
        }

        if (filter.BatchId is { } batchIdFilter && run.BatchId != batchIdFilter)
        {
            return false;
        }

        if (filter.IsTerminal is true && !run.Status.IsTerminal)
        {
            return false;
        }

        if (filter.IsTerminal is false && run.Status.IsTerminal)
        {
            return false;
        }

        return true;
    }

    private static List<JobRun> SortRuns(List<JobRun> runs, RunOrderBy orderBy)
    {
        return orderBy switch
        {
            RunOrderBy.StartedAt => runs.OrderByDescending(r => r.StartedAt).ThenByDescending(r => r.Id).ToList(),
            RunOrderBy.CompletedAt => runs.OrderByDescending(r => r.CompletedAt).ThenByDescending(r => r.Id).ToList(),
            _ => runs.OrderByDescending(r => r.CreatedAt).ThenByDescending(r => r.Id).ToList()
        };
    }

    private static bool NoAdditionalFilters(RunFilter filter) =>
        filter.Status is null
        && filter.JobName is null
        && filter.ParentRunId is null
        && filter.RootRunId is null
        && filter.NodeName is null
        && filter.CompletedAfter is null
        && filter.LastHeartbeatBefore is null
        && filter.IsTerminal is null
        && filter.OrderBy == RunOrderBy.CreatedAt;

    private static bool NoAdditionalFiltersExceptStatus(RunFilter filter) =>
        filter.Status is { }
        && filter.JobName is null
        && filter.ParentRunId is null
        && filter.RootRunId is null
        && filter.NodeName is null
        && filter.CompletedAfter is null
        && filter.LastHeartbeatBefore is null
        && filter.IsTerminal is null
        && filter.OrderBy == RunOrderBy.CreatedAt;

    private static bool NoAdditionalFiltersExceptExactJobName(RunFilter filter) =>
        filter.JobName is { }
        && filter.ExactJobName
        && filter.Status is null
        && filter.ParentRunId is null
        && filter.RootRunId is null
        && filter.NodeName is null
        && filter.CompletedAfter is null
        && filter.LastHeartbeatBefore is null
        && filter.IsTerminal is null
        && filter.OrderBy == RunOrderBy.CreatedAt;

    private static bool NoAdditionalFiltersExceptParentRunId(RunFilter filter) =>
        filter.ParentRunId is { }
        && filter.Status is null
        && filter.JobName is null
        && filter.RootRunId is null
        && filter.NodeName is null
        && filter.CompletedAfter is null
        && filter.LastHeartbeatBefore is null
        && filter.IsTerminal is null
        && filter.OrderBy == RunOrderBy.CreatedAt;

    private static (RedisValue Min, RedisValue Max, Exclude Exclude) GetChildrenLexWindow(RunFilter filter)
    {
        var min = RedisValue.Null;
        var max = RedisValue.Null;
        var exclude = Exclude.None;

        if (filter.CreatedAfter is { } createdAfter)
        {
            var minMs = createdAfter.ToUnixTimeMilliseconds();
            if (minMs == long.MaxValue)
            {
                min = long.MaxValue.ToString($"D{ChildMemberTimestampWidth}", CultureInfo.InvariantCulture);
                max = min;
                exclude = Exclude.Start | Exclude.Stop;
                return (min, max, exclude);
            }

            min = (minMs + 1).ToString($"D{ChildMemberTimestampWidth}", CultureInfo.InvariantCulture);
        }

        if (filter.CreatedBefore is { } createdBefore)
        {
            max = createdBefore.ToUnixTimeMilliseconds()
                .ToString($"D{ChildMemberTimestampWidth}", CultureInfo.InvariantCulture);
            exclude |= Exclude.Stop;
        }

        return (min, max, exclude);
    }

    private static bool NoAdditionalFiltersExceptRootRunId(RunFilter filter) =>
        filter.RootRunId is { }
        && filter.Status is null
        && filter.JobName is null
        && filter.ParentRunId is null
        && filter.NodeName is null
        && filter.CompletedAfter is null
        && filter.LastHeartbeatBefore is null
        && filter.IsTerminal is null
        && filter.OrderBy == RunOrderBy.CreatedAt;

    private static bool NoAdditionalFiltersExceptBatchId(RunFilter filter) =>
        filter.BatchId is { }
        && filter.Status is null
        && filter.JobName is null
        && filter.ParentRunId is null
        && filter.RootRunId is null
        && filter.NodeName is null
        && filter.CompletedAfter is null
        && filter.LastHeartbeatBefore is null
        && filter.IsTerminal is null
        && filter.OrderBy == RunOrderBy.CreatedAt;

    private static (double MinScore, double MaxScore, Exclude Exclude) GetCreatedScoreWindow(RunFilter filter)
    {
        var minScore = filter.CreatedAfter?.ToUnixTimeMilliseconds() ?? double.NegativeInfinity;
        var maxScore = filter.CreatedBefore?.ToUnixTimeMilliseconds() ?? double.PositiveInfinity;
        var exclude = Exclude.None;

        if (filter.CreatedAfter is { })
        {
            exclude |= Exclude.Start;
        }

        if (filter.CreatedBefore is { })
        {
            exclude |= Exclude.Stop;
        }

        return (minScore, maxScore, exclude);
    }

    private static string SerializeRun(JobRun run) =>
        JsonSerializer.Serialize(run, SurefireJsonContext.Default.JobRun);

    private static JobRun DeserializeRun(string json) =>
        JsonSerializer.Deserialize(json, SurefireJsonContext.Default.JobRun)
        ?? throw new InvalidOperationException("Run payload was null.");

    private static JobDefinition HashToJob(HashEntry[] hash)
    {
        var dict = hash.ToDictionary(h => h.Name.ToString(), h => h.Value.ToString());
        return new()
        {
            Name = dict["name"],
            Tags = dict.TryGetValue("tags", out var tags) && !string.IsNullOrEmpty(tags)
                ? JsonSerializer.Deserialize(tags, SurefireJsonContext.Default.StringArray) ?? []
                : [],
            Priority = int.Parse(dict.GetValueOrDefault("priority", "0")!),
            IsContinuous = dict.GetValueOrDefault("is_continuous") == "1",
            IsEnabled = dict.GetValueOrDefault("is_enabled") != "0",
            MisfirePolicy = (MisfirePolicy)int.Parse(dict.GetValueOrDefault("misfire_policy", "0")!),
            FireAllLimit = dict.TryGetValue("fire_all_limit", out var limit) && !string.IsNullOrEmpty(limit)
                ? int.Parse(limit)
                : null,
            Description = dict.TryGetValue("description", out var desc) && !string.IsNullOrEmpty(desc) ? desc : null,
            CronExpression = dict.TryGetValue("cron_expression", out var cron) && !string.IsNullOrEmpty(cron)
                ? cron
                : null,
            TimeZoneId = dict.TryGetValue("time_zone_id", out var tz) && !string.IsNullOrEmpty(tz) ? tz : null,
            Timeout = dict.TryGetValue("timeout", out var timeout) && !string.IsNullOrEmpty(timeout)
                ? TimeSpan.FromTicks(long.Parse(timeout))
                : null,
            MaxConcurrency = dict.TryGetValue("max_concurrency", out var mc) && !string.IsNullOrEmpty(mc)
                ? int.Parse(mc)
                : null,
            RetryPolicy = dict.TryGetValue("retry_policy", out var rp) && !string.IsNullOrEmpty(rp)
                ? JsonSerializer.Deserialize(rp, SurefireJsonContext.Default.RetryPolicy)
                  ?? throw new InvalidOperationException("Retry policy payload was null.")
                : new(),
            Queue = dict.TryGetValue("queue", out var queue) && !string.IsNullOrEmpty(queue) ? queue : null,
            RateLimitName = dict.TryGetValue("rate_limit_name", out var rl) && !string.IsNullOrEmpty(rl) ? rl : null,
            ArgumentsSchema = dict.TryGetValue("arguments_schema", out var schema) && !string.IsNullOrEmpty(schema)
                ? schema
                : null,
            LastHeartbeatAt = dict.TryGetValue("last_heartbeat_at", out var hb) && !string.IsNullOrEmpty(hb)
                ? DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(hb))
                : null,
            LastCronFireAt = dict.TryGetValue("last_cron_fire_at", out var lcf) && !string.IsNullOrEmpty(lcf)
                ? DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(lcf))
                : null
        };
    }

    private static QueueDefinition HashToQueue(HashEntry[] hash)
    {
        var dict = hash.ToDictionary(h => h.Name.ToString(), h => h.Value.ToString());
        return new()
        {
            Name = dict["name"],
            Priority = int.Parse(dict.GetValueOrDefault("priority", "0")!),
            IsPaused = dict.GetValueOrDefault("is_paused") == "1",
            MaxConcurrency = dict.TryGetValue("max_concurrency", out var mc) && !string.IsNullOrEmpty(mc)
                ? int.Parse(mc)
                : null,
            RateLimitName = dict.TryGetValue("rate_limit_name", out var rl) && !string.IsNullOrEmpty(rl) ? rl : null,
            LastHeartbeatAt = dict.TryGetValue("last_heartbeat_at", out var hb) && !string.IsNullOrEmpty(hb)
                ? DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(hb))
                : null
        };
    }

    private static NodeInfo HashToNode(HashEntry[] hash)
    {
        var dict = hash.ToDictionary(h => h.Name.ToString(), h => h.Value.ToString());
        return new()
        {
            Name = dict["name"],
            StartedAt = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(dict["started_at"])),
            LastHeartbeatAt = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(dict["last_heartbeat_at"])),
            RunningCount = int.Parse(dict.GetValueOrDefault("running_count", "0")!),
            RegisteredJobNames = dict.TryGetValue("registered_job_names", out var jn) && !string.IsNullOrEmpty(jn)
                ? JsonSerializer.Deserialize(jn, SurefireJsonContext.Default.StringArray) ?? []
                : [],
            RegisteredQueueNames = dict.TryGetValue("registered_queue_names", out var qn) && !string.IsNullOrEmpty(qn)
                ? JsonSerializer.Deserialize(qn, SurefireJsonContext.Default.StringArray) ?? []
                : []
        };
    }

    private static string SerializeBatch(JobBatch batch) =>
        JsonSerializer.Serialize(batch, SurefireJsonContext.Default.JobBatch);

    private static JobBatch DeserializeBatch(string json) =>
        JsonSerializer.Deserialize(json, SurefireJsonContext.Default.JobBatch)
        ?? throw new InvalidOperationException("Batch payload was null.");

    private static partial class Log
    {
        [LoggerMessage(EventId = 1801, Level = LogLevel.Warning,
            Message = "Skipping malformed Redis event payload.")]
        public static partial void MalformedEventPayload(ILogger logger, Exception exception);

        [LoggerMessage(EventId = 1802, Level = LogLevel.Warning,
            Message = "Skipping malformed Redis batch output payload.")]
        public static partial void MalformedBatchOutputPayload(ILogger logger, Exception exception);
    }
}

file sealed class RunOrderComparer(RunOrderBy orderBy) : IComparer<JobRun>
{
    public int Compare(JobRun? x, JobRun? y)
    {
        if (ReferenceEquals(x, y))
        {
            return 0;
        }

        if (x is null)
        {
            return 1;
        }

        if (y is null)
        {
            return -1;
        }

        var valueComparison = orderBy switch
        {
            RunOrderBy.StartedAt => CompareDescendingNullable(x.StartedAt, y.StartedAt),
            RunOrderBy.CompletedAt => CompareDescendingNullable(x.CompletedAt, y.CompletedAt),
            _ => y.CreatedAt.CompareTo(x.CreatedAt)
        };

        if (valueComparison != 0)
        {
            return valueComparison;
        }

        return string.CompareOrdinal(y.Id, x.Id);
    }

    private static int CompareDescendingNullable(DateTimeOffset? left, DateTimeOffset? right)
    {
        if (left.HasValue && right.HasValue)
        {
            return right.Value.CompareTo(left.Value);
        }

        if (left.HasValue)
        {
            return -1;
        }

        if (right.HasValue)
        {
            return 1;
        }

        return 0;
    }
}