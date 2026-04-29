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
  succeeded: number;
  cancelled: number;
  failed: number;
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
      succeeded: bucket.succeeded ?? 0,
      cancelled: bucket.cancelled ?? 0,
      failed: bucket.failed ?? 0,
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
  return {runId: raw.runId ?? raw.RunId ?? ""};
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
  status: JobStatus;
  arguments?: string;
  result?: string;
  reason?: string;
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
  /** Populated by the trace endpoint so the client can render the tree without rebuilding it. */
  depth?: number;
}

/** Focused trace response returned by GET /api/runs/{id}/trace. */
export interface RunTraceResponse {
  /** Ancestors from root to immediate parent; each has `depth` starting at 0. */
  ancestors: JobRun[];
  /** Focus run; depth == ancestors.length. */
  focus: JobRun;
  /** Siblings ordered before the focus (same depth). */
  siblingsBefore: JobRun[];
  /** Siblings ordered after the focus (same depth). */
  siblingsAfter: JobRun[];
  /** Cursors for paginating siblings beyond the initial window in either direction. */
  siblingsCursor?: { after?: string; before?: string };
  /** First page of direct children of the focus (depth == focus.depth + 1). */
  children: JobRun[];
  /** Cursor for loading more children. */
  childrenCursor?: string;
}

/** Direct children pagination response. GET /api/runs/{id}/children. */
export interface RunChildrenResponse {
  items: JobRun[];
  nextCursor?: string;
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

export const JobStatus = {
  Pending: 0,
  Running: 1,
  Succeeded: 2,
  Cancelled: 4,
  Failed: 5,
} as const;
export type JobStatus = (typeof JobStatus)[keyof typeof JobStatus];

export const JobStatusLabels: Record<number, string> = {
  [JobStatus.Pending]: "Pending",
  [JobStatus.Running]: "Running",
  [JobStatus.Succeeded]: "Succeeded",
  [JobStatus.Cancelled]: "Cancelled",
  [JobStatus.Failed]: "Failed",
};

export const JobStatusColors: Record<number, string> = {
  [JobStatus.Pending]: "bg-status-pending/15 text-status-pending",
  [JobStatus.Running]: "bg-status-running/15 text-status-running",
  [JobStatus.Succeeded]: "bg-status-succeeded/15 text-status-succeeded",
  [JobStatus.Cancelled]: "bg-status-cancelled/15 text-status-cancelled",
  [JobStatus.Failed]: "bg-status-failed/15 text-status-failed",
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
  jobNameContains?: string;
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
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(opts ?? {}),
      },
    ).then(normalizeRunIdResponse),
  getRuns: (params?: RunsQueryParams) => {
    const search = new URLSearchParams();
    if (params?.jobName) search.set("jobName", params.jobName);
    if (params?.jobNameContains)
      search.set("jobNameContains", params.jobNameContains);
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
      const params = new URLSearchParams({take: "1000"});
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
  getRunTrace: (
    id: string,
    params?: { siblingWindow?: number; childrenTake?: number },
  ) => {
    const search = new URLSearchParams();
    if (params?.siblingWindow != null)
      search.set("siblingWindow", params.siblingWindow.toString());
    if (params?.childrenTake != null)
      search.set("childrenTake", params.childrenTake.toString());
    const qs = search.toString();
    return fetchApi<RunTraceResponse>(
      `/runs/${encodeURIComponent(id)}/trace${qs ? `?${qs}` : ""}`,
    );
  },
  getRunChildren: (
    id: string,
    params?: { afterCursor?: string; beforeCursor?: string; take?: number },
  ) => {
    const search = new URLSearchParams();
    if (params?.afterCursor) search.set("afterCursor", params.afterCursor);
    if (params?.beforeCursor) search.set("beforeCursor", params.beforeCursor);
    if (params?.take != null) search.set("take", params.take.toString());
    const qs = search.toString();
    return fetchApi<RunChildrenResponse>(
      `/runs/${encodeURIComponent(id)}/children${qs ? `?${qs}` : ""}`,
    );
  },
  updateJob: (name: string, patch: { isEnabled?: boolean }) =>
    fetchApi<JobResponse>(`/jobs/${encodeURIComponent(name)}`, {
      method: "PATCH",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(patch),
    }),
  getQueues: () =>
    fetchApi<QueueResponse[]>("/queues").then((queues) =>
      queues.map(normalizeQueueResponse),
    ),
  updateQueue: (name: string, patch: { isPaused?: boolean }) =>
    fetchApi<void>(`/queues/${encodeURIComponent(name)}`, {
      method: "PATCH",
      headers: {"Content-Type": "application/json"},
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
