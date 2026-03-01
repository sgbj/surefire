using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using StackExchange.Redis;

namespace Surefire.Redis;

/// <summary>
///     Redis implementation of <see cref="IJobStore" />.
///     All keys use the <c>{surefire}:</c> hash tag prefix for Redis Cluster slot affinity.
///     Every mutation is a single Lua script for atomicity.
/// </summary>
internal sealed class RedisJobStore(RedisOptions options, TimeProvider timeProvider) : IJobStore, IAsyncDisposable
{
    private const string P = "{surefire}:";

    private const string CreateRunsScript = """
                                            local runs = cjson.decode(ARGV[1])
                                            local initial_events = cjson.decode(ARGV[2])

                                            local function append_events(events)
                                                if not events then
                                                    return
                                                end
                                                local seq_key = '{surefire}:event_seq'
                                                for _, evt in ipairs(events) do
                                                    local id = redis.call('INCR', seq_key)
                                                    evt.id = id
                                                    local payload = cjson.encode(evt)
                                                    local id_str = tostring(id)
                                                    redis.call('SET', '{surefire}:event:' .. id_str, payload)
                                                    redis.call('ZADD', '{surefire}:events:run:' .. evt.run_id, id, id_str)
                                                    redis.call('ZADD', '{surefire}:events:run_type:' .. evt.run_id .. ':' .. tostring(evt.event_type), id, id_str)
                                                    redis.call('ZADD', '{surefire}:events:run_attempt:' .. evt.run_id .. ':' .. tostring(evt.attempt), id, id_str)
                                                    redis.call('SADD', '{surefire}:events:run_types:' .. evt.run_id, tostring(evt.event_type))
                                                    redis.call('SADD', '{surefire}:events:run_attempts:' .. evt.run_id, tostring(evt.attempt))
                                                end
                                            end

                                            local function resolve_queue(job_name)
                                                local jdata = redis.call('HGETALL', '{surefire}:job:' .. job_name)
                                                local qname = 'default'
                                                for j = 1, #jdata, 2 do
                                                    if jdata[j] == 'queue' and jdata[j+1] ~= '' then qname = jdata[j+1] end
                                                end
                                                return qname
                                            end

                                            local function resolve_queue_priority(qname)
                                                local pri = redis.call('HGET', '{surefire}:queue:' .. qname, 'priority')
                                                return tonumber(pri) or 0
                                            end

                                            local function add_to_pending(run, run_id, qname)
                                                local member = string.format('%010d%020d%s|%s',
                                                    999999999 - run.priority,
                                                    run.not_before_ms,
                                                    run_id,
                                                    run_id)
                                                redis.call('ZADD', '{surefire}:pending_q:' .. qname, 0, member)
                                                redis.call('SET', '{surefire}:pending_member:' .. run_id, member)
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
                                                local stats_key = '{surefire}:job_stats:' .. run.job_name
                                                redis.call('HINCRBY', stats_key, 'total_runs', 1)

                                                if run.started_at and run.started_at ~= cjson.null then
                                                    redis.call('ZADD', '{surefire}:job_started:' .. run.job_name, tonumber(run.started_at), run.id)
                                                end

                                                if is_terminal(run.status) then
                                                    redis.call('HINCRBY', stats_key, 'terminal_runs', 1)
                                                    if run.status == 2 then
                                                        redis.call('HINCRBY', stats_key, 'succeeded_runs', 1)
                                                        if run.started_at and run.started_at ~= cjson.null and run.completed_at and run.completed_at ~= cjson.null then
                                                            redis.call('HINCRBYFLOAT', stats_key, 'duration_sum_ms', tonumber(run.completed_at) - tonumber(run.started_at))
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
                                                local qname = resolve_queue(run.job_name)
                                                run.priority = tonumber(run.priority) or 0
                                                run.queue_priority = resolve_queue_priority(qname)
                                                local run_json = cjson.encode(run)
                                                redis.call('SET', run_key, run_json)
                                                increment_job_stats_for_new_run(run)

                                                redis.call('SADD', '{surefire}:nonterminal:' .. run.job_name, run_id)

                                                if run.deduplication_id and run.deduplication_id ~= cjson.null then
                                                    redis.call('SET', '{surefire}:dedup:' .. run.job_name .. ':' .. run.deduplication_id, run_id)
                                                end

                                                if run.not_after and run.not_after ~= cjson.null then
                                                    redis.call('ZADD', '{surefire}:expiring', tonumber(run.not_after), run_id)
                                                end

                                                redis.call('ZADD', '{surefire}:runs:created', run.created_at, run_id)
                                                redis.call('ZADD', '{surefire}:job_runs:' .. run.job_name, run.created_at, run_id)
                                                redis.call('ZADD', '{surefire}:status:' .. tostring(run.status), run.created_at, run_id)
                                                redis.call('HINCRBY', '{surefire}:status_counts', tostring(run.status), 1)
                                                if is_terminal(run.status) then
                                                    redis.call('ZADD', '{surefire}:runs:terminal', run.created_at, run_id)
                                                else
                                                    redis.call('ZADD', '{surefire}:runs:nonterminal', run.created_at, run_id)
                                                end
                                                if run.parent_run_id and run.parent_run_id ~= cjson.null and run.parent_run_id ~= '' then
                                                    redis.call('SADD', '{surefire}:children:' .. run.parent_run_id, run_id)
                                                end
                                                if run.root_run_id and run.root_run_id ~= cjson.null and run.root_run_id ~= '' then
                                                    redis.call('SADD', '{surefire}:tree:' .. run.root_run_id, run_id)
                                                end

                                                if run.status == 0 and type(run.batch_total) ~= 'number' then
                                                    add_to_pending(run, run_id, qname)
                                                elseif run.status == 1 then
                                                    redis.call('SADD', '{surefire}:running:' .. run.job_name, run_id)
                                                    redis.call('SADD', '{surefire}:running_q:' .. qname, run_id)
                                                    if run.node_name and run.node_name ~= cjson.null and run.node_name ~= '' then
                                                        redis.call('SADD', '{surefire}:node_runs:' .. run.node_name, run_id)
                                                    end
                                                end
                                                if run.status == 2 or run.status == 4 or run.status == 5 then
                                                    redis.call('SREM', '{surefire}:nonterminal:' .. run.job_name, run_id)
                                                    local ca = run.completed_at
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

    private static readonly RedisKey RoutingKey = new("{surefire}:");

    private readonly ConcurrentDictionary<string, byte[]> _scriptHashes = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _scriptLoadLock = new(1, 1);

    private IDatabase? _db;
    private IConnectionMultiplexer? _mux;

    private IDatabase Db =>
        _db ?? throw new InvalidOperationException("Store not initialized. Call MigrateAsync first.");

    public ValueTask DisposeAsync() => options.DisposeAsync();

    public async Task MigrateAsync(CancellationToken cancellationToken = default)
    {
        var mux = await options.GetConnectionAsync();
        _mux = mux;
        _db = mux.GetDatabase();
    }

    public async Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        const string script = """
                              local key = '{surefire}:job:' .. ARGV[1]
                              local exists = redis.call('EXISTS', key)
                              local now = ARGV[2]

                              if exists == 1 then
                                  local is_enabled = redis.call('HGET', key, 'is_enabled')
                                  local last_cron_fire_at = redis.call('HGET', key, 'last_cron_fire_at')

                                  redis.call('HSET', key,
                                      'name', ARGV[1],
                                      'description', ARGV[3],
                                      'tags', ARGV[4],
                                      'cron_expression', ARGV[5],
                                      'time_zone_id', ARGV[6],
                                      'timeout', ARGV[7],
                                      'max_concurrency', ARGV[8],
                                      'priority', ARGV[9],
                                      'retry_policy', ARGV[10],
                                      'is_continuous', ARGV[11],
                                      'queue', ARGV[12],
                                      'rate_limit_name', ARGV[13],
                                      'misfire_policy', ARGV[14],
                                      'arguments_schema', ARGV[15],
                                      'last_heartbeat_at', now,
                                      'is_enabled', is_enabled,
                                      'last_cron_fire_at', last_cron_fire_at or '')
                              else
                                  redis.call('HSET', key,
                                      'name', ARGV[1],
                                      'description', ARGV[3],
                                      'tags', ARGV[4],
                                      'cron_expression', ARGV[5],
                                      'time_zone_id', ARGV[6],
                                      'timeout', ARGV[7],
                                      'max_concurrency', ARGV[8],
                                      'priority', ARGV[9],
                                      'retry_policy', ARGV[10],
                                      'is_continuous', ARGV[11],
                                      'queue', ARGV[12],
                                      'rate_limit_name', ARGV[13],
                                      'is_enabled', '1',
                                      'misfire_policy', ARGV[14],
                                      'arguments_schema', ARGV[15],
                                      'last_heartbeat_at', now,
                                      'last_cron_fire_at', '')
                                  redis.call('SADD', '{surefire}:jobs', ARGV[1])
                              end
                              return 1
                              """;

        var now = timeProvider.GetUtcNow().ToUnixTimeMilliseconds().ToString();
        await EvaluateScriptAsync(script,
            [RoutingKey],
            [
                job.Name,
                now,
                job.Description ?? "",
                JsonSerializer.Serialize(job.Tags),
                job.CronExpression ?? "",
                job.TimeZoneId ?? "",
                job.Timeout?.Ticks.ToString() ?? "",
                job.MaxConcurrency?.ToString() ?? "",
                job.Priority.ToString(),
                SerializeRetryPolicy(job.RetryPolicy),
                job.IsContinuous ? "1" : "0",
                job.Queue ?? "",
                job.RateLimitName ?? "",
                ((int)job.MisfirePolicy).ToString(),
                job.ArgumentsSchema ?? ""
            ]);
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
            candidateIds = await db.SetMembersAsync($"{P}children:{parentId}");
        }
        else if (filter.RootRunId is { } rootId)
        {
            candidateIds = await db.SetMembersAsync($"{P}tree:{rootId}");
        }
        else if (filter.Status is { } status)
        {
            candidateIds = (await db.SortedSetRangeByScoreAsync($"{P}status:{(int)status}"))
                .Select(v => (RedisValue)v.ToString()).ToArray();
        }
        else if (filter.IsTerminal == true)
        {
            var noPostFilters = filter.JobName is null && filter.NodeName is null
                                                       && filter.ParentRunId is null && filter.RootRunId is null
                                                       && filter.CompletedAfter is null &&
                                                       filter.LastHeartbeatBefore is null
                                                       && filter.IsBatchCoordinator is null &&
                                                       filter.CreatedAfter is null && filter.CreatedBefore is null
                                                       && filter.OrderBy == RunOrderBy.CreatedAt;

            if (noPostFilters)
            {
                totalFromIndex = (int)await db.SortedSetLengthAsync($"{P}runs:terminal");
                candidateIds = await db.SortedSetRangeByScoreAsync($"{P}runs:terminal",
                    order: Order.Descending,
                    skip: skip,
                    take: take);
                paginatedAtIndex = true;
            }
            else
            {
                candidateIds = await db.SortedSetRangeByScoreAsync($"{P}runs:terminal");
            }
        }
        else if (filter.IsTerminal == false)
        {
            var noPostFilters = filter.JobName is null && filter.NodeName is null
                                                       && filter.ParentRunId is null && filter.RootRunId is null
                                                       && filter.CompletedAfter is null &&
                                                       filter.LastHeartbeatBefore is null
                                                       && filter.IsBatchCoordinator is null &&
                                                       filter.CreatedAfter is null && filter.CreatedBefore is null
                                                       && filter.OrderBy == RunOrderBy.CreatedAt;

            if (noPostFilters)
            {
                totalFromIndex = (int)await db.SortedSetLengthAsync($"{P}runs:nonterminal");
                candidateIds = await db.SortedSetRangeByScoreAsync($"{P}runs:nonterminal",
                    order: Order.Descending,
                    skip: skip,
                    take: take);
                paginatedAtIndex = true;
            }
            else
            {
                candidateIds = await db.SortedSetRangeByScoreAsync($"{P}runs:nonterminal");
            }
        }
        else if (filter.JobName is { } jobName && filter.ExactJobName)
        {
            candidateIds = (await db.SortedSetRangeByScoreAsync($"{P}job_runs:{jobName}"))
                .Select(v => (RedisValue)v.ToString()).ToArray();
        }
        else if (filter.OrderBy == RunOrderBy.CompletedAt && filter.CompletedAfter is { } completedAfter)
        {
            candidateIds = await db.SortedSetRangeByScoreAsync($"{P}runs:completed",
                completedAfter.ToUnixTimeMilliseconds(), double.PositiveInfinity,
                Exclude.Start);
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
                candidateIds = await db.SortedSetRangeByScoreAsync($"{P}runs:created",
                    minScore, maxScore, exclude);
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
                              if s(r.node_name) ~= ARGV[2] then return 0 end

                              r.progress = tonumber(ARGV[3])
                              r.result = ARGV[4] ~= '' and ARGV[4] or nil
                              r.error = ARGV[5] ~= '' and ARGV[5] or nil
                              r.trace_id = ARGV[6] ~= '' and ARGV[6] or nil
                              r.span_id = ARGV[7] ~= '' and ARGV[7] or nil
                              r.last_heartbeat_at = ARGV[8] ~= '' and tonumber(ARGV[8]) or nil

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
                run.Error ?? "",
                run.TraceId ?? "",
                run.SpanId ?? "",
                run.LastHeartbeatAt?.ToUnixTimeMilliseconds().ToString() ?? ""
            ]);
    }

    public async Task<bool> TryTransitionRunAsync(RunStatusTransition transition,
        CancellationToken cancellationToken = default)
    {
        if (!RunTransitionRules.IsAllowed(transition.ExpectedStatus, transition.NewStatus)
            || !transition.HasRequiredFields())
        {
            return false;
        }

        const string script = """
                              local function s(v) return (v ~= nil and v ~= cjson.null) and v or '' end
                              local id = ARGV[1]
                              local key = '{surefire}:run:' .. id
                              local data = redis.call('GET', key)
                              if not data then return 0 end

                              local r = cjson.decode(data)
                              if r.status ~= tonumber(ARGV[2]) then return 0 end
                              if r.attempt ~= tonumber(ARGV[3]) then return 0 end
                              if r.status == 2 or r.status == 4 or r.status == 5 then return 0 end

                              local old_status = r.status
                              local old_node = s(r.node_name)
                              local new_status = tonumber(ARGV[4])
                              local was_terminal = old_status == 2 or old_status == 4 or old_status == 5
                              local is_terminal = new_status == 2 or new_status == 4 or new_status == 5
                              r.status = new_status
                              r.node_name = ARGV[5] ~= '' and ARGV[5] or nil
                              r.started_at = ARGV[6] ~= '' and tonumber(ARGV[6]) or r.started_at
                              r.completed_at = ARGV[7] ~= '' and tonumber(ARGV[7]) or r.completed_at
                              r.cancelled_at = ARGV[8] ~= '' and tonumber(ARGV[8]) or r.cancelled_at
                              r.error = ARGV[9] ~= '' and ARGV[9] or nil
                              r.result = ARGV[10] ~= '' and ARGV[10] or nil
                              r.progress = tonumber(ARGV[11])
                              r.not_before_ms = tonumber(ARGV[12])
                              r.last_heartbeat_at = ARGV[13] ~= '' and tonumber(ARGV[13]) or r.last_heartbeat_at

                              redis.call('SET', key, cjson.encode(r))

                              -- Resolve queue name (used by multiple branches below)
                              local queue_name = 'default'
                              local job_data = redis.call('HGETALL', '{surefire}:job:' .. r.job_name)
                              for i = 1, #job_data, 2 do
                                  if job_data[i] == 'queue' and job_data[i+1] ~= '' then queue_name = job_data[i+1] end
                              end

                              -- Remove from old indexes
                              if old_status == 0 then
                                  local pm = redis.call('GET', '{surefire}:pending_member:' .. id)
                                  if pm then
                                      redis.call('ZREM', '{surefire}:pending_q:' .. queue_name, pm)
                                      redis.call('DEL', '{surefire}:pending_member:' .. id)
                                  end
                              end
                              if old_status == 1 then
                                  redis.call('SREM', '{surefire}:running:' .. r.job_name, id)
                                  redis.call('SREM', '{surefire}:running_q:' .. queue_name, id)
                                  if old_node ~= '' then
                                      redis.call('SREM', '{surefire}:node_runs:' .. old_node, id)
                                  end
                              end

                              -- Status index maintenance
                              redis.call('ZREM', '{surefire}:status:' .. tostring(old_status), id)
                              redis.call('ZADD', '{surefire}:status:' .. tostring(new_status), r.created_at, id)
                              redis.call('HINCRBY', '{surefire}:status_counts', tostring(old_status), -1)
                              redis.call('HINCRBY', '{surefire}:status_counts', tostring(new_status), 1)
                              redis.call('ZREM', '{surefire}:runs:terminal', id)
                              redis.call('ZREM', '{surefire}:runs:nonterminal', id)
                              if new_status == 2 or new_status == 4 or new_status == 5 then
                                  redis.call('ZADD', '{surefire}:runs:terminal', r.created_at, id)
                              else
                                  redis.call('ZADD', '{surefire}:runs:nonterminal', r.created_at, id)
                              end

                              -- Add to new indexes
                              if new_status == 0 then
                                  local member = string.format('%010d%020d%s|%s',
                                      999999999 - r.priority,
                                      r.not_before_ms,
                                      id,
                                      id)
                                  redis.call('ZADD', '{surefire}:pending_q:' .. queue_name, 0, member)
                                  redis.call('SET', '{surefire}:pending_member:' .. id, member)
                              end
                              if new_status == 1 then
                                  redis.call('SADD', '{surefire}:running:' .. r.job_name, id)
                                  redis.call('SADD', '{surefire}:running_q:' .. queue_name, id)
                                  if r.started_at and r.started_at ~= cjson.null then
                                      redis.call('ZADD', '{surefire}:job_started:' .. r.job_name, tonumber(r.started_at), id)
                                  end
                                  local new_node = s(r.node_name)
                                  if new_node ~= '' then
                                      if old_node ~= '' and old_node ~= new_node then
                                          redis.call('SREM', '{surefire}:node_runs:' .. old_node, id)
                                      end
                                      redis.call('SADD', '{surefire}:node_runs:' .. new_node, id)
                                  end
                              end

                              -- Terminal transition: update secondary indexes
                              if new_status == 2 or new_status == 4 or new_status == 5 then
                                  if r.deduplication_id and r.deduplication_id ~= cjson.null and r.deduplication_id ~= '' then
                                      redis.call('DEL', '{surefire}:dedup:' .. r.job_name .. ':' .. r.deduplication_id)
                                  end
                                  redis.call('SREM', '{surefire}:nonterminal:' .. r.job_name, id)
                                  if old_node ~= '' then
                                      redis.call('SREM', '{surefire}:node_runs:' .. old_node, id)
                                  end
                                  local ca = r.completed_at
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
                                  local stats_key = '{surefire}:job_stats:' .. r.job_name
                                  redis.call('HINCRBY', stats_key, 'terminal_runs', 1)
                                  if new_status == 2 then
                                      redis.call('HINCRBY', stats_key, 'succeeded_runs', 1)
                                      if r.started_at and r.started_at ~= cjson.null and r.completed_at and r.completed_at ~= cjson.null then
                                          redis.call('HINCRBYFLOAT', stats_key, 'duration_sum_ms', tonumber(r.completed_at) - tonumber(r.started_at))
                                          redis.call('HINCRBY', stats_key, 'duration_count', 1)
                                      end
                                  elseif new_status == 5 then
                                      redis.call('HINCRBY', stats_key, 'failed_runs', 1)
                                  end
                              end

                              return 1
                              """;

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
                transition.Error ?? "",
                transition.Result ?? "",
                transition.Progress.ToString(),
                transition.NotBefore.ToUnixTimeMilliseconds().ToString(),
                transition.LastHeartbeatAt?.ToUnixTimeMilliseconds().ToString() ?? ""
            ]);

        return (int)result == 1;
    }

    public async Task<bool> TryCancelRunAsync(string runId,
        CancellationToken cancellationToken = default)
    {
        const string script = """
                              local t = redis.call('TIME')
                              local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
                              local function s(v) return (v ~= nil and v ~= cjson.null) and v or '' end
                              local id = ARGV[1]
                              local key = '{surefire}:run:' .. id
                              local data = redis.call('GET', key)
                              if not data then return 0 end

                              local r = cjson.decode(data)
                              if r.status == 2 or r.status == 4 or r.status == 5 then return 0 end

                              local old_status = r.status
                              local old_node = s(r.node_name)
                              r.status = 4
                              r.cancelled_at = now_ms
                              r.completed_at = now_ms
                              redis.call('SET', key, cjson.encode(r))

                              -- Resolve queue name
                              local queue_name = 'default'
                              local job_data = redis.call('HGETALL', '{surefire}:job:' .. r.job_name)
                              for i = 1, #job_data, 2 do
                                  if job_data[i] == 'queue' and job_data[i+1] ~= '' then queue_name = job_data[i+1] end
                              end

                              if old_status == 0 then
                                  local pm = redis.call('GET', '{surefire}:pending_member:' .. id)
                                  if pm then
                                      redis.call('ZREM', '{surefire}:pending_q:' .. queue_name, pm)
                                      redis.call('DEL', '{surefire}:pending_member:' .. id)
                                  end
                              end
                              if old_status == 1 then
                                  redis.call('SREM', '{surefire}:running:' .. r.job_name, id)
                                  redis.call('SREM', '{surefire}:running_q:' .. queue_name, id)
                              end

                              -- Status index maintenance
                              redis.call('ZREM', '{surefire}:status:' .. tostring(old_status), id)
                              redis.call('ZADD', '{surefire}:status:4', r.created_at, id)
                              redis.call('HINCRBY', '{surefire}:status_counts', tostring(old_status), -1)
                              redis.call('HINCRBY', '{surefire}:status_counts', '4', 1)
                              redis.call('ZREM', '{surefire}:runs:terminal', id)
                              redis.call('ZREM', '{surefire}:runs:nonterminal', id)
                              redis.call('ZADD', '{surefire}:runs:terminal', r.created_at, id)

                              -- Terminal transition indexes
                              if r.deduplication_id and r.deduplication_id ~= cjson.null and r.deduplication_id ~= '' then
                                  redis.call('DEL', '{surefire}:dedup:' .. r.job_name .. ':' .. r.deduplication_id)
                              end
                              redis.call('SREM', '{surefire}:nonterminal:' .. r.job_name, id)
                              if old_node ~= '' then
                                  redis.call('SREM', '{surefire}:node_runs:' .. old_node, id)
                              end
                              redis.call('HINCRBY', '{surefire}:job_stats:' .. r.job_name, 'terminal_runs', 1)
                              redis.call('ZADD', '{surefire}:runs:completed', now_ms, id)
                              local minute = math.floor(now_ms / 60000)
                              redis.call('HINCRBY', '{surefire}:timeline:' .. tostring(minute), 'cancelled', 1)
                              redis.call('ZADD', '{surefire}:timeline:index', minute, tostring(minute))
                              redis.call('ZREM', '{surefire}:expiring', id)

                              return 1
                              """;

        var result = await EvaluateScriptAsync(script,
            [RoutingKey],
            [
                runId
            ]);

        return (int)result == 1;
    }

    public async Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, CancellationToken cancellationToken = default)
    {
        if (jobNames.Count == 0 || queueNames.Count == 0)
        {
            return null;
        }

        // ARGV[1]=job_names_json, ARGV[2]=queue_names_json, ARGV[3]=node_name, ARGV[4]=now_ms
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
                                      if a.run.not_before_ms ~= b.run.not_before_ms then
                                          return tonumber(a.run.not_before_ms) < tonumber(b.run.not_before_ms)
                                      end
                                      return a.run_id < b.run_id
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
                                                  if r.status == 0 and type(r.batch_total) ~= 'number' and r.not_before_ms <= now_ms
                                                      and (r.not_after == nil or r.not_after == cjson.null or tonumber(r.not_after) > now_ms)
                                                      and job_set[r.job_name] then
                                                      local job = job_cache[r.job_name]
                                                      if not job then
                                                          local job_data = redis.call('HGETALL', '{surefire}:job:' .. r.job_name)
                                                          if #job_data == 0 then
                                                              -- Keep orphan runs pending until their job definition is registered.
                                                              retained = retained + 1
                                                          else
                                                              job = {}
                                                              for i = 1, #job_data, 2 do
                                                                  job[job_data[i]] = job_data[i+1]
                                                              end
                                                              job_cache[r.job_name] = job
                                                          end
                                                      end

                                                      if job then
                                                          local can_claim = true
                                                          if job.max_concurrency and job.max_concurrency ~= '' then
                                                              local running = redis.call('SCARD', '{surefire}:running:' .. r.job_name)
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
                                                                  run_id = run_id,
                                                                  entry = entry,
                                                                  queue_name = queue_name,
                                                                  job_rl_name = job_rl_name,
                                                                  queue_rl_name = queue_rl_name
                                                              }
                                                          end

                                                          retained = retained + 1
                                                      end
                                                  elseif r.status ~= 0 or type(r.batch_total) == 'number' then
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

                                  local idx = 1
                                  while idx <= #ordered_queues do
                                      local tier_priority = ordered_queues[idx][2]
                                      local tier_end = idx
                                      while tier_end + 1 <= #ordered_queues and ordered_queues[tier_end + 1][2] == tier_priority do
                                          tier_end = tier_end + 1
                                      end

                                      local best = nil
                                      for i = idx, tier_end do
                                          local queue_name = ordered_queues[i][1]
                                          local queue = queue_cache[queue_name]
                                          local candidate = find_best_for_queue(queue_name, queue)
                                          if candidate and is_better_candidate(candidate, best) then
                                              best = candidate
                                          end
                                      end

                                      if best then
                                          if best.job_rl_name ~= '' then
                                              _acquire_rate_limit(best.job_rl_name, now_ms)
                                          end
                                          if best.queue_rl_name ~= '' and best.queue_rl_name ~= best.job_rl_name then
                                              _acquire_rate_limit(best.queue_rl_name, now_ms)
                                          end

                                          local old_node = best.run.node_name
                                          if old_node and old_node ~= cjson.null and old_node ~= node_name then
                                              redis.call('SREM', '{surefire}:node_runs:' .. old_node, best.run_id)
                                          end

                                          best.run.status = 1
                                          best.run.node_name = node_name
                                          best.run.started_at = tonumber(now_ms)
                                          best.run.last_heartbeat_at = tonumber(now_ms)
                                          best.run.attempt = best.run.attempt + 1

                                          redis.call('SET', '{surefire}:run:' .. best.run_id, cjson.encode(best.run))
                                          redis.call('ZADD', '{surefire}:job_started:' .. best.run.job_name, tonumber(now_ms), best.run_id)
                                          redis.call('ZREM', '{surefire}:pending_q:' .. best.queue_name, best.entry)
                                          redis.call('DEL', '{surefire}:pending_member:' .. best.run_id)
                                          redis.call('SADD', '{surefire}:running:' .. best.run.job_name, best.run_id)
                                          redis.call('SADD', '{surefire}:running_q:' .. best.queue_name, best.run_id)

                                          redis.call('ZREM', '{surefire}:status:0', best.run_id)
                                          redis.call('ZADD', '{surefire}:status:1', best.run.created_at, best.run_id)
                                          redis.call('HINCRBY', '{surefire}:status_counts', '0', -1)
                                          redis.call('HINCRBY', '{surefire}:status_counts', '1', 1)
                                          redis.call('SADD', '{surefire}:node_runs:' .. node_name, best.run_id)

                                          return cjson.encode(best.run)
                                      end

                                      idx = tier_end + 1
                                  end

                                  return nil
                                  """;

        var result = await EvaluateScriptAsync(fullScript,
            [RoutingKey],
            [
                JsonSerializer.Serialize(jobNames),
                JsonSerializer.Serialize(queueNames),
                nodeName,
                timeProvider.GetUtcNow().ToUnixTimeMilliseconds().ToString()
            ]);

        if (result.IsNull)
        {
            return null;
        }

        return DeserializeRun(result.ToString()!);
    }

    public async Task<BatchCounters> IncrementBatchCounterAsync(string batchRunId, bool isFailed,
        CancellationToken cancellationToken = default)
    {
        const string script = """
                              local key = '{surefire}:run:' .. ARGV[1]
                              local data = redis.call('GET', key)
                              if not data then
                                  return redis.error_reply('Run not found: ' .. ARGV[1])
                              end

                              local r = cjson.decode(data)
                              if type(r.batch_total) ~= 'number' then
                                  return redis.error_reply('Not a batch coordinator: ' .. ARGV[1])
                              end

                              local function num(v) return type(v) == 'number' and v or 0 end

                              if r.status == 2 or r.status == 4 or r.status == 5 then
                                  return cjson.encode({r.batch_total, num(r.batch_completed), num(r.batch_failed)})
                              end

                              if ARGV[2] == '1' then
                                  r.batch_failed = num(r.batch_failed) + 1
                              else
                                  r.batch_completed = num(r.batch_completed) + 1
                              end

                              r.progress = r.batch_total == 0 and 1.0 or (num(r.batch_completed) + num(r.batch_failed)) / r.batch_total

                              redis.call('SET', key, cjson.encode(r))
                              return cjson.encode({r.batch_total, r.batch_completed, r.batch_failed})
                              """;

        RedisResult result;
        try
        {
            result = await EvaluateScriptAsync(script,
                [RoutingKey],
                [
                    batchRunId,
                    isFailed ? "1" : "0"
                ]);
        }
        catch (RedisServerException ex)
        {
            throw new InvalidOperationException(ex.Message, ex);
        }

        var arr = JsonSerializer.Deserialize<int[]>(result.ToString()!)!;
        return new(arr[0], arr[1], arr[2]);
    }

    public async Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default)
    {
        if (events.Count == 0)
        {
            return;
        }

        const string script = """
                              local seq_key = '{surefire}:event_seq'
                              for i = 1, #ARGV do
                                  local id = redis.call('INCR', seq_key)
                                  local evt = cjson.decode(ARGV[i])
                                  evt.id = id
                                  local payload = cjson.encode(evt)
                                  local id_str = tostring(id)
                                  redis.call('SET', '{surefire}:event:' .. id_str, payload)
                                  redis.call('ZADD', '{surefire}:events:run:' .. evt.run_id, id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_type:' .. evt.run_id .. ':' .. tostring(evt.event_type), id, id_str)
                                  redis.call('ZADD', '{surefire}:events:run_attempt:' .. evt.run_id .. ':' .. tostring(evt.attempt), id, id_str)
                                  redis.call('SADD', '{surefire}:events:run_types:' .. evt.run_id, tostring(evt.event_type))
                                  redis.call('SADD', '{surefire}:events:run_attempts:' .. evt.run_id, tostring(evt.attempt))
                              end
                              return 1
                              """;

        var args = new RedisValue[events.Count];
        for (var i = 0; i < events.Count; i++)
        {
            var evt = events[i];
            args[i] = JsonSerializer.Serialize(new
            {
                run_id = evt.RunId,
                event_type = (int)evt.EventType,
                payload = evt.Payload,
                created_at = evt.CreatedAt.ToUnixTimeMilliseconds(),
                attempt = evt.Attempt
            });
        }

        await EvaluateScriptAsync(script, [RoutingKey], args);
    }

    public async Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0,
        RunEventType[]? types = null, int? attempt = null, CancellationToken cancellationToken = default)
    {
        var db = Db;

        RedisValue[] ids;
        if (attempt is { } && (types is null || types.Length == 0))
        {
            var requestedAttemptKey = (RedisKey)$"{P}events:run_attempt:{runId}:{attempt.Value}";
            var requestedAttemptTask = db.SortedSetRangeByScoreAsync(requestedAttemptKey,
                sinceId,
                double.PositiveInfinity,
                Exclude.Start);

            Task<RedisValue[]>? wildcardAttemptTask = null;
            if (attempt.Value != 0)
            {
                var wildcardAttemptKey = (RedisKey)$"{P}events:run_attempt:{runId}:0";
                wildcardAttemptTask = db.SortedSetRangeByScoreAsync(wildcardAttemptKey,
                    sinceId,
                    double.PositiveInfinity,
                    Exclude.Start);
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
                Exclude.Start);
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
                var json = jsonValue.ToString();
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                var eventType = root.GetProperty("event_type").GetInt32();
                if (typeSet is { } && !typeSet.Contains(eventType))
                {
                    continue;
                }

                var eventAttempt = root.GetProperty("attempt").GetInt32();
                if (attempt is { } && eventAttempt != attempt.Value && eventAttempt != 0)
                {
                    continue;
                }

                events.Add(new()
                {
                    Id = root.GetProperty("id").GetInt64(),
                    RunId = root.GetProperty("run_id").GetString()!,
                    EventType = (RunEventType)eventType,
                    Payload = root.GetProperty("payload").GetString()!,
                    CreatedAt = DateTimeOffset.FromUnixTimeMilliseconds(root.GetProperty("created_at").GetInt64()),
                    Attempt = eventAttempt
                });
            }
            catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException)
            {
                Trace.TraceWarning("Skipping malformed Redis event payload: {0}", ex.Message);
            }
        }

        return events;
    }

    public async Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchRunId, long sinceEventId = 0,
        int take = 200, CancellationToken cancellationToken = default)
    {
        if (take <= 0)
        {
            return [];
        }

        var db = Db;
        var childIds = await db.SetMembersAsync($"{P}children:{batchRunId}");
        if (childIds.Length == 0)
        {
            return [];
        }

        var scanBatch = db.CreateBatch();
        var idTasks = new Task<RedisValue[]>[childIds.Length];
        for (var i = 0; i < childIds.Length; i++)
        {
            var childId = childIds[i].ToString();
            var key = (RedisKey)$"{P}events:run_type:{childId}:{(int)RunEventType.Output}";
            idTasks[i] = scanBatch.SortedSetRangeByScoreAsync(
                key,
                sinceEventId,
                double.PositiveInfinity,
                Exclude.Start,
                Order.Ascending,
                0,
                take);
        }

        scanBatch.Execute();
        await Task.WhenAll(idTasks);

        var mergedIds = idTasks
            .SelectMany(t => t.Result)
            .Select(v => v.ToString())
            .Where(v => !string.IsNullOrEmpty(v))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(v => long.Parse(v, CultureInfo.InvariantCulture), Comparer<long>.Default)
            .Take(take)
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
                using var doc = JsonDocument.Parse(jsonValue.ToString());
                var root = doc.RootElement;
                events.Add(new()
                {
                    Id = root.GetProperty("id").GetInt64(),
                    RunId = root.GetProperty("run_id").GetString()!,
                    EventType = (RunEventType)root.GetProperty("event_type").GetInt32(),
                    Payload = root.GetProperty("payload").GetString()!,
                    CreatedAt = DateTimeOffset.FromUnixTimeMilliseconds(root.GetProperty("created_at").GetInt64()),
                    Attempt = root.GetProperty("attempt").GetInt32()
                });
            }
            catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException)
            {
                Trace.TraceWarning("Skipping malformed Redis batch output payload: {0}", ex.Message);
            }
        }

        events.Sort((a, b) => a.Id.CompareTo(b.Id));
        return events;
    }

    public async Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, IReadOnlyCollection<string> activeRunIds,
        CancellationToken cancellationToken = default)
    {
        var now = timeProvider.GetUtcNow();
        var nowMs = now.ToUnixTimeMilliseconds();

        // ARGV: [1]=node_name, [2]=now_ms, [3]=running_count, [4]=job_names_json, [5]=queue_names_json, [6..]=run_ids
        const string script = """
                              local node_name = ARGV[1]
                              local now_ms = tonumber(ARGV[2])
                              local running_count = ARGV[3]
                              local job_names_json = ARGV[4]
                              local queue_names_json = ARGV[5]

                              local node_key = '{surefire}:node:' .. node_name
                              local started_at = redis.call('HGET', node_key, 'started_at')
                              if not started_at then
                                  started_at = ARGV[2]
                              end

                              redis.call('HSET', node_key,
                                  'name', node_name,
                                  'started_at', started_at,
                                  'last_heartbeat_at', ARGV[2],
                                  'running_count', running_count,
                                  'registered_job_names', job_names_json,
                                  'registered_queue_names', queue_names_json)
                              redis.call('SADD', '{surefire}:nodes', node_name)

                              local now_str = ARGV[2]
                              for i = 6, #ARGV do
                                  local run_key = '{surefire}:run:' .. ARGV[i]
                                  local json = redis.call('GET', run_key)
                                  if json then
                                      local r = cjson.decode(json)
                                      if r.status ~= 2 and r.status ~= 4 and r.status ~= 5 then
                                          if r.node_name ~= nil and r.node_name ~= cjson.null and r.node_name == node_name then
                                              r.last_heartbeat_at = tonumber(now_str)
                                              redis.call('SET', run_key, cjson.encode(r))
                                          end
                                      end
                                  end
                              end
                              return 1
                              """;

        var args = new List<RedisValue>
        {
            nodeName,
            nowMs.ToString(),
            activeRunIds.Count.ToString(),
            JsonSerializer.Serialize(jobNames),
            JsonSerializer.Serialize(queueNames)
        };
        foreach (var runId in activeRunIds)
        {
            args.Add(runId);
        }

        await EvaluateScriptAsync(script, [RoutingKey], args.ToArray());
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

    public async Task UpsertQueueAsync(QueueDefinition queue, CancellationToken cancellationToken = default)
    {
        const string script = """
                              local queue_name = ARGV[1]
                              local key = '{surefire}:queue:' .. queue_name
                              local exists = redis.call('EXISTS', key)
                              local now = ARGV[2]

                              if exists == 1 then
                                  local is_paused = redis.call('HGET', key, 'is_paused')
                                  redis.call('HSET', key,
                                      'name', queue_name,
                                      'priority', ARGV[3],
                                      'max_concurrency', ARGV[4],
                                      'rate_limit_name', ARGV[5],
                                      'last_heartbeat_at', now,
                                      'is_paused', is_paused)
                              else
                                  redis.call('HSET', key,
                                      'name', queue_name,
                                      'priority', ARGV[3],
                                      'max_concurrency', ARGV[4],
                                      'rate_limit_name', ARGV[5],
                                      'last_heartbeat_at', now,
                                      'is_paused', '0')
                                  redis.call('SADD', '{surefire}:queues', queue_name)
                              end
                              return 1
                              """;

        await EvaluateScriptAsync(script,
            [RoutingKey],
            [
                queue.Name,
                timeProvider.GetUtcNow().ToUnixTimeMilliseconds().ToString(),
                queue.Priority.ToString(),
                queue.MaxConcurrency?.ToString() ?? "",
                queue.RateLimitName ?? ""
            ]);
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

    public async Task SetQueuePausedAsync(string name, bool isPaused, CancellationToken cancellationToken = default)
    {
        const string script = """
                              if redis.call('EXISTS', KEYS[1]) == 1 then
                                  redis.call('HSET', KEYS[1], 'is_paused', ARGV[1])
                              end
                              return 1
                              """;

        await EvaluateScriptAsync(script,
            [new($"{P}queue:{name}")],
            [isPaused ? "1" : "0"]);
    }

    public async Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken cancellationToken = default)
    {
        const string script = """
                              local key = '{surefire}:rate_limit:' .. ARGV[1]
                              local exists = redis.call('EXISTS', key)

                              if exists == 1 then
                                  redis.call('HSET', key,
                                      'type', ARGV[2],
                                      'max_permits', ARGV[3],
                                      'window', ARGV[4],
                                      'last_heartbeat_at', ARGV[5])
                              else
                                  redis.call('HSET', key,
                                      'name', ARGV[1],
                                      'type', ARGV[2],
                                      'max_permits', ARGV[3],
                                      'window', ARGV[4],
                                      'last_heartbeat_at', ARGV[5],
                                      'current_count', '0',
                                      'previous_count', '0',
                                      'window_start', '0')
                                  redis.call('SADD', '{surefire}:rate_limits', ARGV[1])
                              end
                              return 1
                              """;

        await EvaluateScriptAsync(script,
            [RoutingKey],
            [
                rateLimit.Name,
                ((int)rateLimit.Type).ToString(),
                rateLimit.MaxPermits.ToString(),
                rateLimit.Window.Ticks.ToString(),
                timeProvider.GetUtcNow().ToUnixTimeMilliseconds().ToString()
            ]);
    }

    public async Task<int> CancelExpiredRunsAsync(CancellationToken cancellationToken = default)
    {
        const string script = """
                              local t = redis.call('TIME')
                              local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
                              local offset = tonumber(ARGV[1])
                              local expired = redis.call('ZRANGEBYSCORE', '{surefire}:expiring', '-inf', now_ms, 'LIMIT', offset, 500)
                              local cancelled = 0
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
                                      elseif type(r.batch_total) == 'number' then
                                          redis.call('ZREM', '{surefire}:expiring', run_id)
                                          cleaned = cleaned + 1
                                      elseif r.status == 0 or r.status == 3 then
                                          local old_status = r.status
                                          r.status = 4
                                          r.cancelled_at = now_ms
                                          r.completed_at = now_ms
                                          redis.call('SET', key, cjson.encode(r))

                                          -- Resolve queue name for pending_q cleanup
                                          local queue_name = 'default'
                                          local job_data = redis.call('HGETALL', '{surefire}:job:' .. r.job_name)
                                          for qi = 1, #job_data, 2 do
                                              if job_data[qi] == 'queue' and job_data[qi+1] ~= '' then queue_name = job_data[qi+1] end
                                          end

                                          if old_status == 0 then
                                              local pm = redis.call('GET', '{surefire}:pending_member:' .. run_id)
                                              if pm then
                                                  redis.call('ZREM', '{surefire}:pending_q:' .. queue_name, pm)
                                                  redis.call('DEL', '{surefire}:pending_member:' .. run_id)
                                              end
                                          end
                                          -- Status index maintenance
                                          redis.call('ZREM', '{surefire}:status:' .. tostring(old_status), run_id)
                                          redis.call('ZADD', '{surefire}:status:4', r.created_at, run_id)
                                          redis.call('HINCRBY', '{surefire}:status_counts', tostring(old_status), -1)
                                          redis.call('HINCRBY', '{surefire}:status_counts', '4', 1)
                                          redis.call('ZREM', '{surefire}:runs:terminal', run_id)
                                          redis.call('ZREM', '{surefire}:runs:nonterminal', run_id)
                                          redis.call('ZADD', '{surefire}:runs:terminal', r.created_at, run_id)

                                          if r.deduplication_id and r.deduplication_id ~= cjson.null and r.deduplication_id ~= '' then
                                              redis.call('DEL', '{surefire}:dedup:' .. r.job_name .. ':' .. r.deduplication_id)
                                          end
                                          redis.call('SREM', '{surefire}:nonterminal:' .. r.job_name, run_id)
                                          redis.call('HINCRBY', '{surefire}:job_stats:' .. r.job_name, 'terminal_runs', 1)
                                          redis.call('ZADD', '{surefire}:runs:completed', now_ms, run_id)
                                          local minute = math.floor(now_ms / 60000)
                                          redis.call('HINCRBY', '{surefire}:timeline:' .. tostring(minute), 'cancelled', 1)
                                          redis.call('ZADD', '{surefire}:timeline:index', minute, tostring(minute))
                                          redis.call('ZREM', '{surefire}:expiring', run_id)

                                          cancelled = cancelled + 1
                                      elseif r.status == 1 then
                                          skipped = skipped + 1
                                      end
                                  end
                              end

                              return cjson.encode({cancelled, cleaned, skipped})
                              """;

        var totalCancelled = 0;
        var offset = 0;
        while (true)
        {
            var result = await EvaluateScriptAsync(script, [RoutingKey],
                [offset.ToString()]);
            var arr = JsonSerializer.Deserialize<int[]>(result.ToString()!)!;
            int batchCancelled = arr[0], batchCleaned = arr[1], batchSkipped = arr[2];
            totalCancelled += batchCancelled;

            if (batchCancelled == 0 && batchCleaned == 0 && batchSkipped == 0)
            {
                break;
            }

            // Cancelled/cleaned entries were ZREM'd, shifting the set.
            // Only advance offset past skipped (Running) entries that remain.
            offset += batchSkipped;
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
                                           for _, event_id in ipairs(event_ids) do
                                               redis.call('DEL', '{surefire}:event:' .. event_id)
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

                                       local function purge_run(run_id, r)
                                           local stats_key = '{surefire}:job_stats:' .. r.job_name
                                           if r.deduplication_id and r.deduplication_id ~= cjson.null and r.deduplication_id ~= '' then
                                               redis.call('DEL', '{surefire}:dedup:' .. r.job_name .. ':' .. r.deduplication_id)
                                           end

                                           redis.call('HINCRBY', stats_key, 'total_runs', -1)
                                           if r.started_at and r.started_at ~= cjson.null then
                                               redis.call('ZREM', '{surefire}:job_started:' .. r.job_name, run_id)
                                           end
                                           local status_num = tonumber(r.status)
                                           if status_num == 2 or status_num == 4 or status_num == 5 then
                                               redis.call('HINCRBY', stats_key, 'terminal_runs', -1)
                                               if status_num == 2 then
                                                   redis.call('HINCRBY', stats_key, 'succeeded_runs', -1)
                                                   if r.started_at and r.started_at ~= cjson.null and r.completed_at and r.completed_at ~= cjson.null then
                                                       redis.call('HINCRBYFLOAT', stats_key, 'duration_sum_ms', -(tonumber(r.completed_at) - tonumber(r.started_at)))
                                                       redis.call('HINCRBY', stats_key, 'duration_count', -1)
                                                   end
                                               elseif status_num == 5 then
                                                   redis.call('HINCRBY', stats_key, 'failed_runs', -1)
                                               end
                                           end

                                           redis.call('SREM', '{surefire}:nonterminal:' .. r.job_name, run_id)

                                           local queue_name = resolve_queue(r.job_name)
                                           local pm = redis.call('GET', '{surefire}:pending_member:' .. run_id)
                                           if pm then
                                               redis.call('ZREM', '{surefire}:pending_q:' .. queue_name, pm)
                                           end
                                           redis.call('SREM', '{surefire}:running:' .. r.job_name, run_id)
                                           redis.call('SREM', '{surefire}:running_q:' .. queue_name, run_id)

                                           redis.call('ZREM', '{surefire}:runs:created', run_id)
                                           redis.call('ZREM', '{surefire}:runs:completed', run_id)
                                           redis.call('ZREM', '{surefire}:runs:terminal', run_id)
                                           redis.call('ZREM', '{surefire}:runs:nonterminal', run_id)
                                           redis.call('ZREM', '{surefire}:job_runs:' .. r.job_name, run_id)

                                           local removed = redis.call('ZREM', '{surefire}:status:' .. tostring(r.status), run_id)
                                           if removed > 0 then
                                               redis.call('HINCRBY', '{surefire}:status_counts', tostring(r.status), -1)
                                           end

                                           if r.parent_run_id and r.parent_run_id ~= cjson.null and r.parent_run_id ~= '' then
                                               redis.call('SREM', '{surefire}:children:' .. r.parent_run_id, run_id)
                                           end
                                           if r.root_run_id and r.root_run_id ~= cjson.null and r.root_run_id ~= '' then
                                               redis.call('SREM', '{surefire}:tree:' .. r.root_run_id, run_id)
                                           end
                                           if r.node_name and r.node_name ~= cjson.null and r.node_name ~= '' then
                                               redis.call('SREM', '{surefire}:node_runs:' .. r.node_name, run_id)
                                           end

                                           decrement_timeline(status_num, r.completed_at)
                                           cleanup_events(run_id)

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
                                               if r.parent_run_id and r.parent_run_id ~= cjson.null and r.parent_run_id ~= '' then
                                                   local parent_data = redis.call('GET', '{surefire}:run:' .. r.parent_run_id)
                                                   if parent_data then
                                                       local parent = cjson.decode(parent_data)
                                                       if parent.batch_total and parent.batch_total ~= cjson.null then
                                                           local parent_status = tonumber(parent.status)
                                                           local parent_completed = tonumber(parent.completed_at) or 0
                                                           if (parent_status ~= 2 and parent_status ~= 4 and parent_status ~= 5)
                                                               or parent.completed_at == nil
                                                               or parent.completed_at == cjson.null
                                                               or parent_completed >= threshold_ms then
                                                               can_purge = false
                                                           end
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
                                               if r.status == 0 and r.not_before_ms < threshold_ms then
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
                                                   if r.status == 3 and r.not_before_ms < threshold_ms then
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
                         + statusCounts.GetValueOrDefault("1", 0)
                         + statusCounts.GetValueOrDefault("3", 0);
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
                Completed = (int)await countTasks[JobStatus.Completed],
                Retrying = (int)await countTasks[JobStatus.Retrying],
                Cancelled = (int)await countTasks[JobStatus.Cancelled],
                DeadLetter = (int)await countTasks[JobStatus.DeadLetter]
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

            var mux = _mux ?? throw new InvalidOperationException("Store not initialized. Call MigrateAsync first.");
            var loadTasks = new List<Task<byte[]>>();
            foreach (var endpoint in mux.GetEndPoints())
            {
                var server = mux.GetServer(endpoint);
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
        CancellationToken cancellationToken)
    {
        if (runs.Count == 0 && (initialEvents is null || initialEvents.Count == 0))
        {
            return;
        }

        var runsJson = JsonSerializer.Serialize(runs.Select(SerializeRunAsObject).ToList());
        var initialEventsJson = JsonSerializer.Serialize(SerializeEventsAsObjects(initialEvents));
        await EvaluateScriptAsync(CreateRunsScript,
            [RoutingKey],
            [(RedisValue)runsJson, (RedisValue)initialEventsJson]);
    }

    private async Task<bool> TryCreateRunCoreAsync(JobRun run, int? maxActiveForJob,
        DateTimeOffset? lastCronFireAt,
        IReadOnlyList<RunEvent>? initialEvents,
        CancellationToken cancellationToken)
    {
        const string script = """
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
                                  for _, evt in ipairs(events) do
                                      local id = redis.call('INCR', seq_key)
                                      evt.id = id
                                      local payload = cjson.encode(evt)
                                      local id_str = tostring(id)
                                      redis.call('SET', '{surefire}:event:' .. id_str, payload)
                                      redis.call('ZADD', '{surefire}:events:run:' .. evt.run_id, id, id_str)
                                      redis.call('ZADD', '{surefire}:events:run_type:' .. evt.run_id .. ':' .. tostring(evt.event_type), id, id_str)
                                      redis.call('ZADD', '{surefire}:events:run_attempt:' .. evt.run_id .. ':' .. tostring(evt.attempt), id, id_str)
                                      redis.call('SADD', '{surefire}:events:run_types:' .. evt.run_id, tostring(evt.event_type))
                                      redis.call('SADD', '{surefire}:events:run_attempts:' .. evt.run_id, tostring(evt.attempt))
                                  end
                              end

                              local function is_terminal(status)
                                  return status == 2 or status == 4 or status == 5
                              end

                              local function increment_job_stats_for_new_run(run)
                                  local stats_key = '{surefire}:job_stats:' .. run.job_name
                                  redis.call('HINCRBY', stats_key, 'total_runs', 1)

                                  if run.started_at and run.started_at ~= cjson.null then
                                      redis.call('ZADD', '{surefire}:job_started:' .. run.job_name, tonumber(run.started_at), ARGV[1])
                                  end

                                  if is_terminal(run.status) then
                                      redis.call('HINCRBY', stats_key, 'terminal_runs', 1)
                                      if run.status == 2 then
                                          redis.call('HINCRBY', stats_key, 'succeeded_runs', 1)
                                          if run.started_at and run.started_at ~= cjson.null and run.completed_at and run.completed_at ~= cjson.null then
                                              redis.call('HINCRBYFLOAT', stats_key, 'duration_sum_ms', tonumber(run.completed_at) - tonumber(run.started_at))
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

                              -- Resolve queue priority at enqueue time
                              local jdata = redis.call('HGETALL', '{surefire}:job:' .. job_name)
                              local jinfo = {}
                              for j = 1, #jdata, 2 do jinfo[jdata[j]] = jdata[j+1] end
                              run.priority = tonumber(run.priority) or 0
                              local qname = (jinfo.queue and jinfo.queue ~= '') and jinfo.queue or 'default'
                              local qpri = redis.call('HGET', '{surefire}:queue:' .. qname, 'priority')
                              run.queue_priority = tonumber(qpri) or 0

                              redis.call('SET', run_key, cjson.encode(run))
                              increment_job_stats_for_new_run(run)
                              redis.call('SADD', '{surefire}:nonterminal:' .. job_name, ARGV[1])

                              if dedup_id ~= '' then
                                  redis.call('SET', '{surefire}:dedup:' .. job_name .. ':' .. dedup_id, ARGV[1])
                              end

                              if run.not_after and run.not_after ~= cjson.null then
                                  redis.call('ZADD', '{surefire}:expiring', tonumber(run.not_after), ARGV[1])
                              end

                              -- Secondary indexes
                              redis.call('ZADD', '{surefire}:runs:created', run.created_at, ARGV[1])
                              redis.call('ZADD', '{surefire}:job_runs:' .. job_name, run.created_at, ARGV[1])
                              redis.call('ZADD', '{surefire}:status:' .. tostring(run.status), run.created_at, ARGV[1])
                              redis.call('HINCRBY', '{surefire}:status_counts', tostring(run.status), 1)
                              if run.status == 2 or run.status == 4 or run.status == 5 then
                                  redis.call('ZADD', '{surefire}:runs:terminal', run.created_at, ARGV[1])
                              else
                                  redis.call('ZADD', '{surefire}:runs:nonterminal', run.created_at, ARGV[1])
                              end
                              if parent_run_id ~= '' then
                                  redis.call('SADD', '{surefire}:children:' .. parent_run_id, ARGV[1])
                              end
                              if root_run_id ~= '' then
                                  redis.call('SADD', '{surefire}:tree:' .. root_run_id, ARGV[1])
                              end
                              if tonumber(run.status) == 1 then
                                  redis.call('SADD', '{surefire}:running:' .. job_name, ARGV[1])
                                  redis.call('SADD', '{surefire}:running_q:' .. qname, ARGV[1])
                                  if node_name_arg ~= '' then
                                      redis.call('SADD', '{surefire}:node_runs:' .. node_name_arg, ARGV[1])
                                  end
                              end

                              if run.status == 0 and type(run.batch_total) ~= 'number' then
                                  local member = string.format('%010d%020d%s|%s',
                                      999999999 - run.priority,
                                      run.not_before_ms,
                                      ARGV[1],
                                      ARGV[1])
                                  redis.call('ZADD', '{surefire}:pending_q:' .. qname, 0, member)
                                  redis.call('SET', '{surefire}:pending_member:' .. ARGV[1], member)
                              end

                              if (run.status == 2 or run.status == 4 or run.status == 5)
                                  and run.completed_at and run.completed_at ~= cjson.null then
                                  local minute = math.floor(tonumber(run.completed_at) / 60000)
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
                JsonSerializer.Serialize(SerializeEventsAsObjects(initialEvents))
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

        if (filter.CompletedAfter is { } && (run.CompletedAt is null || run.CompletedAt < filter.CompletedAfter.Value))
        {
            return false;
        }

        if (filter.LastHeartbeatBefore is { } &&
            (run.LastHeartbeatAt is null || run.LastHeartbeatAt >= filter.LastHeartbeatBefore.Value))
        {
            return false;
        }

        if (filter.IsBatchCoordinator is true && run.BatchTotal is null)
        {
            return false;
        }

        if (filter.IsBatchCoordinator is false && run.BatchTotal is { })
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
        && filter.IsBatchCoordinator is null
        && filter.IsTerminal is null
        && filter.OrderBy == RunOrderBy.CreatedAt;

    private static string SerializeRun(JobRun run) =>
        JsonSerializer.Serialize(SerializeRunAsObject(run));

    private static List<object> SerializeEventsAsObjects(IReadOnlyList<RunEvent>? events)
    {
        if (events is null || events.Count == 0)
        {
            return [];
        }

        return events.Select(evt => (object)new
        {
            run_id = evt.RunId,
            event_type = (int)evt.EventType,
            payload = evt.Payload,
            created_at = evt.CreatedAt.ToUnixTimeMilliseconds(),
            attempt = evt.Attempt
        }).ToList();
    }

    private static object SerializeRunAsObject(JobRun run) => new
    {
        id = run.Id,
        job_name = run.JobName,
        status = (int)run.Status,
        arguments = run.Arguments,
        result = run.Result,
        error = run.Error,
        progress = run.Progress,
        created_at = run.CreatedAt.ToUnixTimeMilliseconds(),
        started_at = run.StartedAt?.ToUnixTimeMilliseconds(),
        completed_at = run.CompletedAt?.ToUnixTimeMilliseconds(),
        cancelled_at = run.CancelledAt?.ToUnixTimeMilliseconds(),
        node_name = run.NodeName,
        attempt = run.Attempt,
        trace_id = run.TraceId,
        span_id = run.SpanId,
        parent_run_id = run.ParentRunId,
        root_run_id = run.RootRunId,
        rerun_of_run_id = run.RerunOfRunId,
        not_before_ms = run.NotBefore.ToUnixTimeMilliseconds(),
        not_after = run.NotAfter?.ToUnixTimeMilliseconds(),
        priority = run.Priority,
        deduplication_id = run.DeduplicationId,
        last_heartbeat_at = run.LastHeartbeatAt?.ToUnixTimeMilliseconds(),
        queue_priority = run.QueuePriority,
        batch_total = run.BatchTotal,
        batch_completed = run.BatchCompleted,
        batch_failed = run.BatchFailed
    };

    private static long GetMs(JsonElement v) =>
        v.ValueKind == JsonValueKind.String ? long.Parse(v.GetString()!) : v.GetInt64();

    private static JobRun DeserializeRun(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var r = doc.RootElement;

        var run = new JobRun
        {
            Id = r.GetProperty("id").GetString()!,
            JobName = r.GetProperty("job_name").GetString()!,
            Status = (JobStatus)r.GetProperty("status").GetInt32(),
            Progress = r.GetProperty("progress").GetDouble(),
            CreatedAt = DateTimeOffset.FromUnixTimeMilliseconds(GetMs(r.GetProperty("created_at"))),
            Attempt = r.GetProperty("attempt").GetInt32(),
            NotBefore = DateTimeOffset.FromUnixTimeMilliseconds(GetMs(r.GetProperty("not_before_ms"))),
            Priority = r.GetProperty("priority").GetInt32()
        };

        if (r.TryGetProperty("arguments", out var v) && v.ValueKind != JsonValueKind.Null)
        {
            run.Arguments = v.GetString();
        }

        if (r.TryGetProperty("result", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.Result = v.GetString();
        }

        if (r.TryGetProperty("error", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.Error = v.GetString();
        }

        if (r.TryGetProperty("started_at", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.StartedAt = DateTimeOffset.FromUnixTimeMilliseconds(GetMs(v));
        }

        if (r.TryGetProperty("completed_at", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.CompletedAt = DateTimeOffset.FromUnixTimeMilliseconds(GetMs(v));
        }

        if (r.TryGetProperty("cancelled_at", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.CancelledAt = DateTimeOffset.FromUnixTimeMilliseconds(GetMs(v));
        }

        if (r.TryGetProperty("node_name", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.NodeName = v.GetString();
        }

        if (r.TryGetProperty("trace_id", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.TraceId = v.GetString();
        }

        if (r.TryGetProperty("span_id", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.SpanId = v.GetString();
        }

        if (r.TryGetProperty("parent_run_id", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.ParentRunId = v.GetString();
        }

        if (r.TryGetProperty("root_run_id", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.RootRunId = v.GetString();
        }

        if (r.TryGetProperty("rerun_of_run_id", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.RerunOfRunId = v.GetString();
        }

        if (r.TryGetProperty("not_after", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.NotAfter = DateTimeOffset.FromUnixTimeMilliseconds(GetMs(v));
        }

        if (r.TryGetProperty("deduplication_id", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.DeduplicationId = v.GetString();
        }

        if (r.TryGetProperty("last_heartbeat_at", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.LastHeartbeatAt = DateTimeOffset.FromUnixTimeMilliseconds(GetMs(v));
        }

        if (r.TryGetProperty("queue_priority", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.QueuePriority = v.GetInt32();
        }

        if (r.TryGetProperty("batch_total", out v) && v.ValueKind != JsonValueKind.Null)
        {
            run.BatchTotal = v.GetInt32();

            if (r.TryGetProperty("batch_completed", out var bc) && bc.ValueKind != JsonValueKind.Null)
            {
                run.BatchCompleted = bc.GetInt32();
            }

            if (r.TryGetProperty("batch_failed", out var bf) && bf.ValueKind != JsonValueKind.Null)
            {
                run.BatchFailed = bf.GetInt32();
            }
        }

        return run;
    }

    private static JobDefinition HashToJob(HashEntry[] hash)
    {
        var dict = hash.ToDictionary(h => h.Name.ToString(), h => h.Value.ToString());

        var job = new JobDefinition
        {
            Name = dict["name"],
            Tags = dict.TryGetValue("tags", out var tags) && !string.IsNullOrEmpty(tags)
                ? JsonSerializer.Deserialize<string[]>(tags) ?? []
                : [],
            Priority = int.Parse(dict.GetValueOrDefault("priority", "0")!),
            IsContinuous = dict.GetValueOrDefault("is_continuous") == "1",
            IsEnabled = dict.GetValueOrDefault("is_enabled") != "0",
            MisfirePolicy = (MisfirePolicy)int.Parse(dict.GetValueOrDefault("misfire_policy", "0")!)
        };

        if (dict.TryGetValue("description", out var desc) && !string.IsNullOrEmpty(desc))
        {
            job.Description = desc;
        }

        if (dict.TryGetValue("cron_expression", out var cron) && !string.IsNullOrEmpty(cron))
        {
            job.CronExpression = cron;
        }

        if (dict.TryGetValue("time_zone_id", out var tz) && !string.IsNullOrEmpty(tz))
        {
            job.TimeZoneId = tz;
        }

        if (dict.TryGetValue("timeout", out var timeout) && !string.IsNullOrEmpty(timeout))
        {
            job.Timeout = TimeSpan.FromTicks(long.Parse(timeout));
        }

        if (dict.TryGetValue("max_concurrency", out var mc) && !string.IsNullOrEmpty(mc))
        {
            job.MaxConcurrency = int.Parse(mc);
        }

        if (dict.TryGetValue("retry_policy", out var rp) && !string.IsNullOrEmpty(rp))
        {
            job.RetryPolicy = DeserializeRetryPolicy(rp);
        }

        if (dict.TryGetValue("queue", out var queue) && !string.IsNullOrEmpty(queue))
        {
            job.Queue = queue;
        }

        if (dict.TryGetValue("rate_limit_name", out var rl) && !string.IsNullOrEmpty(rl))
        {
            job.RateLimitName = rl;
        }

        if (dict.TryGetValue("arguments_schema", out var schema) && !string.IsNullOrEmpty(schema))
        {
            job.ArgumentsSchema = schema;
        }

        if (dict.TryGetValue("last_heartbeat_at", out var hb) && !string.IsNullOrEmpty(hb))
        {
            job.LastHeartbeatAt = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(hb));
        }

        if (dict.TryGetValue("last_cron_fire_at", out var lcf) && !string.IsNullOrEmpty(lcf))
        {
            job.LastCronFireAt = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(lcf));
        }

        return job;
    }

    private static QueueDefinition HashToQueue(HashEntry[] hash)
    {
        var dict = hash.ToDictionary(h => h.Name.ToString(), h => h.Value.ToString());
        var queue = new QueueDefinition
        {
            Name = dict["name"],
            Priority = int.Parse(dict.GetValueOrDefault("priority", "0")!),
            IsPaused = dict.GetValueOrDefault("is_paused") == "1"
        };

        if (dict.TryGetValue("max_concurrency", out var mc) && !string.IsNullOrEmpty(mc))
        {
            queue.MaxConcurrency = int.Parse(mc);
        }

        if (dict.TryGetValue("rate_limit_name", out var rl) && !string.IsNullOrEmpty(rl))
        {
            queue.RateLimitName = rl;
        }

        if (dict.TryGetValue("last_heartbeat_at", out var hb) && !string.IsNullOrEmpty(hb))
        {
            queue.LastHeartbeatAt = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(hb));
        }

        return queue;
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
                ? JsonSerializer.Deserialize<string[]>(jn) ?? []
                : [],
            RegisteredQueueNames = dict.TryGetValue("registered_queue_names", out var qn) && !string.IsNullOrEmpty(qn)
                ? JsonSerializer.Deserialize<string[]>(qn) ?? []
                : []
        };
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
}