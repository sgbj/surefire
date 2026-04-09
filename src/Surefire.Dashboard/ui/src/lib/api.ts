const BASE = `${new URL(document.baseURI).pathname.replace(/\/$/, "")}/api`;

interface ProblemDetailsLike {
  title?: string;
  detail?: string;
}

async function fetchApi<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, init);
  if (!res.ok) {
    const contentType = res.headers.get("content-type") ?? "";
    if (contentType.includes("application/problem+json")) {
      const problem = (await res
        .json()
        .catch(() => null)) as ProblemDetailsLike | null;
      const title = problem?.title?.trim();
      const detail = problem?.detail?.trim();
      const message = [title, detail].filter(Boolean).join(": ");
      throw new Error(message.length > 0 ? message : `API error ${res.status}`);
    }

    const body = await res.text().catch(() => "");
    throw new Error(`API error ${res.status}: ${body}`);
  }
  if (res.status === 204) {
    return undefined as T;
  }
  return res.json();
}

export interface TimelineBucket {
  timestamp: string;
  start?: string;
  pending: number;
  running: number;
  completed: number;
  retrying: number;
  cancelled: number;
  deadLetter: number;
}

export interface DashboardStats {
  totalJobs: number;
  totalRuns: number;
  activeRuns: number;
  successRate: number;
  nodeCount: number;
  recentRuns: JobRun[];
  runsByStatus: Record<string, number>;
  timeline: TimelineBucket[];
}

function normalizeDashboardStats(raw: DashboardStats): DashboardStats {
  const timeline = Array.isArray(raw.timeline)
    ? raw.timeline.map((bucket) => ({
        ...bucket,
        timestamp: bucket.timestamp ?? bucket.start ?? "",
        pending: bucket.pending ?? 0,
        running: bucket.running ?? 0,
        completed: bucket.completed ?? 0,
        retrying: bucket.retrying ?? 0,
        cancelled: bucket.cancelled ?? 0,
        deadLetter: bucket.deadLetter ?? 0,
      }))
    : [];
  return {
    ...raw,
    successRate: raw.successRate ?? 0,
    recentRuns: Array.isArray(raw.recentRuns) ? raw.recentRuns : [],
    runsByStatus: raw.runsByStatus ?? {},
    timeline,
  };
}

function normalizeRunIdResponse(raw: { runId?: string; RunId?: string }): {
  runId: string;
} {
  return { runId: raw.runId ?? raw.RunId ?? "" };
}

function normalizeJobResponse(raw: JobResponse): JobResponse {
  return {
    ...raw,
    tags: Array.isArray(raw.tags) ? raw.tags : [],
  };
}

function normalizeNodeResponse(raw: NodeResponse): NodeResponse {
  return {
    ...raw,
    registeredJobNames: Array.isArray(raw.registeredJobNames)
      ? raw.registeredJobNames
      : [],
    registeredQueueNames: Array.isArray(raw.registeredQueueNames)
      ? raw.registeredQueueNames
      : [],
  };
}

function normalizeQueueResponse(raw: QueueResponse): QueueResponse {
  return {
    ...raw,
    processingNodes: Array.isArray(raw.processingNodes)
      ? raw.processingNodes
      : [],
  };
}

export interface JobResponse {
  name: string;
  description?: string;
  tags: string[];
  cronExpression?: string;
  timeZoneId?: string;
  timeout?: string;
  maxConcurrency: number | null;
  priority: number;
  retryPolicy: RetryPolicy;
  isContinuous: boolean;
  isEnabled: boolean;
  queue?: string;
  rateLimitName?: string;
  lastHeartbeatAt?: string;
  isActive: boolean;
  nextRunAt?: string;
  misfirePolicy: number;
  argumentsSchema?: JsonSchema;
}

export interface JsonSchema {
  $defs?: Record<string, JsonSchemaProperty>;
  definitions?: Record<string, JsonSchemaProperty>;
  $ref?: string;
  type?: string | string[];
  properties?: Record<string, JsonSchemaProperty>;
  required?: string[];
  additionalProperties?: boolean;
  oneOf?: JsonSchemaProperty[];
  anyOf?: JsonSchemaProperty[];
  allOf?: JsonSchemaProperty[];
}

export interface JsonSchemaProperty {
  type?: string | string[];
  format?: string;
  enum?: unknown[];
  default?: unknown;
  description?: string;
  minimum?: number;
  maximum?: number;
  minLength?: number;
  maxLength?: number;
  pattern?: string;
  items?: JsonSchemaProperty;
  properties?: Record<string, JsonSchemaProperty>;
  required?: string[];
  oneOf?: JsonSchemaProperty[];
  anyOf?: JsonSchemaProperty[];
  allOf?: JsonSchemaProperty[];
  $ref?: string;
}

export interface RetryPolicy {
  maxAttempts: number;
  backoffType: number;
  initialDelay: string;
  maxDelay: string;
}

export interface JobRun {
  id: string;
  jobName: string;
  status: number;
  arguments?: string;
  result?: string;
  error?: string;
  progress: number;
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
  cancelledAt?: string;
  nodeName?: string;
  attempt: number;
  traceId?: string;
  spanId?: string;
  parentRunId?: string;
  rootRunId?: string;
  rerunOfRunId?: string;
  notBefore?: string;
  notAfter?: string;
  priority: number;
  queuePriority?: number;
  deduplicationId?: string;
  lastHeartbeatAt?: string;
  batchTotal?: number;
  batchCompleted?: number;
  batchFailed?: number;
  streamBindings?: Record<string, string[]>;
}

export interface NodeResponse {
  name: string;
  startedAt: string;
  lastHeartbeatAt: string;
  runningCount: number;
  registeredJobNames: string[];
  registeredQueueNames: string[];
  isActive: boolean;
}

export interface QueueResponse {
  name: string;
  priority: number;
  maxConcurrency: number | null;
  isPaused: boolean;
  rateLimitName: string | null;
  pendingCount: number;
  runningCount: number;
  processingNodes: string[];
}

export interface JobStats {
  totalRuns: number;
  succeededRuns: number;
  failedRuns: number;
  successRate: number;
  avgDuration?: string;
  lastRunAt?: string;
}

export interface PagedResult<T> {
  items: T[];
  totalCount: number;
}

export interface TracePageParams {
  skip?: number;
  take?: number;
}

export interface RunLogEntry {
  timestamp: string;
  level: number;
  message: string;
  category?: string;
}

export interface LogPage {
  items: RunLogEntry[];
  nextCursor: number | null;
}

export const JobStatusLabels: Record<number, string> = {
  0: "Pending",
  1: "Running",
  2: "Completed",
  3: "Retrying",
  4: "Cancelled",
  5: "Dead letter",
};

export const JobStatusColors: Record<number, string> = {
  0: "bg-status-pending/15 text-status-pending",
  1: "bg-status-running/15 text-status-running",
  2: "bg-status-completed/15 text-status-completed",
  3: "bg-status-retrying/15 text-status-retrying",
  4: "bg-status-cancelled/15 text-status-cancelled",
  5: "bg-status-dead-letter/15 text-status-dead-letter",
};

export const LogLevelLabels: Record<number, string> = {
  0: "Trace",
  1: "Debug",
  2: "Information",
  3: "Warning",
  4: "Error",
  5: "Critical",
};

export interface RunsQueryParams {
  jobName?: string;
  exactJobName?: boolean;
  status?: number;
  nodeName?: string;
  parentRunId?: string;
  skip?: number;
  take?: number;
  createdAfter?: string;
  createdBefore?: string;
}

export const api = {
  getStats: (params?: { since?: string; bucketMinutes?: number }) => {
    const search = new URLSearchParams();
    if (params?.since) search.set("since", params.since);
    if (params?.bucketMinutes)
      search.set("bucketMinutes", params.bucketMinutes.toString());
    const qs = search.toString();
    return fetchApi<DashboardStats>(`/stats${qs ? `?${qs}` : ""}`).then(
      normalizeDashboardStats,
    );
  },
  getJobs: (params?: { includeInactive?: boolean }) => {
    const search = new URLSearchParams();
    if (params?.includeInactive) search.set("includeInactive", "true");
    const qs = search.toString();
    return fetchApi<JobResponse[]>(`/jobs${qs ? `?${qs}` : ""}`).then((jobs) =>
      jobs.map(normalizeJobResponse),
    );
  },
  getJob: (name: string) =>
    fetchApi<JobResponse>(`/jobs/${encodeURIComponent(name)}`).then(
      normalizeJobResponse,
    ),
  getJobStats: (name: string) =>
    fetchApi<JobStats>(`/jobs/${encodeURIComponent(name)}/stats`),
  triggerJob: (
    name: string,
    opts?: {
      args?: unknown;
      notBefore?: string;
      notAfter?: string;
      priority?: number;
      deduplicationId?: string;
    },
  ) =>
    fetchApi<{ runId?: string; RunId?: string }>(
      `/jobs/${encodeURIComponent(name)}/trigger`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(opts ?? {}),
      },
    ).then(normalizeRunIdResponse),
  getRuns: (params?: RunsQueryParams) => {
    const search = new URLSearchParams();
    if (params?.jobName) search.set("jobName", params.jobName);
    if (params?.exactJobName) search.set("exactJobName", "true");
    if (params?.status !== undefined)
      search.set("status", params.status.toString());
    if (params?.nodeName) search.set("nodeName", params.nodeName);
    if (params?.parentRunId) search.set("parentRunId", params.parentRunId);
    if (params?.skip != null) search.set("skip", params.skip.toString());
    if (params?.take != null) search.set("take", params.take.toString());
    if (params?.createdAfter) search.set("createdAfter", params.createdAfter);
    if (params?.createdBefore)
      search.set("createdBefore", params.createdBefore);
    const qs = search.toString();
    return fetchApi<PagedResult<JobRun>>(`/runs${qs ? `?${qs}` : ""}`);
  },
  getRun: (id: string) => fetchApi<JobRun>(`/runs/${encodeURIComponent(id)}`),
  cancelRun: (id: string) =>
    fetchApi<void>(`/runs/${encodeURIComponent(id)}/cancel`, {
      method: "POST",
    }),
  rerunRun: (id: string) =>
    fetchApi<{ runId?: string; RunId?: string }>(
      `/runs/${encodeURIComponent(id)}/rerun`,
      {
        method: "POST",
      },
    ).then(normalizeRunIdResponse),
  getRunLogs: async (id: string): Promise<RunLogEntry[]> => {
    const all: RunLogEntry[] = [];
    let cursor: number | null = null;
    while (true) {
      const params = new URLSearchParams({ take: "1000" });
      if (cursor != null) params.set("sinceEventId", cursor.toString());
      const page = await fetchApi<LogPage>(
        `/runs/${encodeURIComponent(id)}/logs?${params}`,
      );
      all.push(...page.items);
      if (page.nextCursor == null) break;
      cursor = page.nextCursor;
    }
    return all;
  },
  getRunTracePage: (id: string, params?: TracePageParams) => {
    const search = new URLSearchParams();
    if (params?.skip != null) search.set("skip", params.skip.toString());
    if (params?.take != null) search.set("take", params.take.toString());
    const qs = search.toString();
    return fetchApi<PagedResult<JobRun>>(
      `/runs/${encodeURIComponent(id)}/trace${qs ? `?${qs}` : ""}`,
    );
  },
  updateJob: (name: string, patch: { isEnabled?: boolean }) =>
    fetchApi<JobResponse>(`/jobs/${encodeURIComponent(name)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    }),
  getQueues: () =>
    fetchApi<QueueResponse[]>("/queues").then((queues) =>
      queues.map(normalizeQueueResponse),
    ),
  updateQueue: (name: string, patch: { isPaused?: boolean }) =>
    fetchApi<void>(`/queues/${encodeURIComponent(name)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    }),
  getNodes: (params?: { includeInactive?: boolean }) => {
    const search = new URLSearchParams();
    if (params?.includeInactive) search.set("includeInactive", "true");
    const qs = search.toString();
    return fetchApi<NodeResponse[]>(`/nodes${qs ? `?${qs}` : ""}`).then(
      (nodes) => nodes.map(normalizeNodeResponse),
    );
  },
  getNode: (name: string) =>
    fetchApi<NodeResponse>(`/nodes/${encodeURIComponent(name)}`).then(
      normalizeNodeResponse,
    ),
  streamRun: (id: string, sinceEventId?: number) => {
    const search = new URLSearchParams();
    if (sinceEventId && sinceEventId > 0)
      search.set("sinceEventId", sinceEventId.toString());
    const qs = search.toString();
    return new EventSource(
      `${BASE}/runs/${encodeURIComponent(id)}/stream${qs ? `?${qs}` : ""}`,
    );
  },
};
