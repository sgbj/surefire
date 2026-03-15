const BASE = `${new URL(document.baseURI).pathname.replace(/\/$/, '')}/api`;

async function fetchApi<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, init);
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`API error ${res.status}: ${body}`);
  }
  if (res.status === 204) {
    return undefined as T;
  }
  return res.json();
}

export interface TimelineBucket {
  timestamp: string;
  completed: number;
  failed: number;
  pending: number;
  running: number;
  cancelled: number;
  deadLetter: number;
  skipped: number;
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
  isPlan: boolean;
  planGraph?: string;
  argumentsSchema?: JsonSchema;
}

export interface JsonSchema {
  type?: string;
  properties?: Record<string, JsonSchemaProperty>;
  required?: string[];
  additionalProperties?: boolean;
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
  retryOfRunId?: string;
  rerunOfRunId?: string;
  notBefore?: string;
  planRunId?: string;
  planStepId?: string;
  planStepName?: string;
  planGraph?: string;
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

export const JobStatusLabels: Record<number, string> = {
  0: 'Pending',
  1: 'Running',
  2: 'Completed',
  3: 'Failed',
  4: 'Cancelled',
  5: 'Dead letter',
  6: 'Skipped',
};

export const JobStatusColors: Record<number, string> = {
  0: 'bg-status-pending/15 text-status-pending',
  1: 'bg-status-running/15 text-status-running',
  2: 'bg-status-completed/15 text-status-completed',
  3: 'bg-status-failed/15 text-status-failed',
  4: 'bg-status-cancelled/15 text-status-cancelled',
  5: 'bg-status-dead-letter/15 text-status-dead-letter',
  6: 'bg-status-skipped/15 text-status-skipped',
};

export const LogLevelLabels: Record<number, string> = {
  0: 'Trace',
  1: 'Debug',
  2: 'Information',
  3: 'Warning',
  4: 'Error',
  5: 'Critical',
};

export interface RunsQueryParams {
  jobName?: string;
  exactJobName?: boolean;
  status?: number;
  nodeName?: string;
  parentRunId?: string;
  retryOfRunId?: string;
  rerunOfRunId?: string;
  planRunId?: string;
  planStepName?: string;
  skip?: number;
  take?: number;
  createdAfter?: string;
  createdBefore?: string;
}

export const api = {
  getStats: (params?: { since?: string; bucketMinutes?: number }) => {
    const search = new URLSearchParams();
    if (params?.since) search.set('since', params.since);
    if (params?.bucketMinutes) search.set('bucketMinutes', params.bucketMinutes.toString());
    const qs = search.toString();
    return fetchApi<DashboardStats>(`/stats${qs ? `?${qs}` : ''}`);
  },
  getJobs: (params?: { includeInactive?: boolean; includeInternal?: boolean }) => {
    const search = new URLSearchParams();
    if (params?.includeInactive) search.set('includeInactive', 'true');
    if (params?.includeInternal) search.set('includeInternal', 'true');
    const qs = search.toString();
    return fetchApi<JobResponse[]>(`/jobs${qs ? `?${qs}` : ''}`);
  },
  getJob: (name: string) => fetchApi<JobResponse>(`/jobs/${encodeURIComponent(name)}`),
  getJobStats: (name: string) => fetchApi<JobStats>(`/jobs/${encodeURIComponent(name)}/stats`),
  triggerJob: (name: string, opts?: { args?: unknown; notBefore?: string; notAfter?: string; priority?: number; deduplicationId?: string }) =>
    fetchApi<{ runId: string }>(`/jobs/${encodeURIComponent(name)}/trigger`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(opts ?? {}),
    }),
  getRuns: (params?: RunsQueryParams) => {
    const search = new URLSearchParams();
    if (params?.jobName) search.set('jobName', params.jobName);
    if (params?.exactJobName) search.set('exactJobName', 'true');
    if (params?.status !== undefined) search.set('status', params.status.toString());
    if (params?.nodeName) search.set('nodeName', params.nodeName);
    if (params?.parentRunId) search.set('parentRunId', params.parentRunId);
    if (params?.retryOfRunId) search.set('retryOfRunId', params.retryOfRunId);
    if (params?.rerunOfRunId) search.set('rerunOfRunId', params.rerunOfRunId);
    if (params?.planRunId) search.set('planRunId', params.planRunId);
    if (params?.planStepName) search.set('planStepName', params.planStepName);
    if (params?.skip != null) search.set('skip', params.skip.toString());
    if (params?.take != null) search.set('take', params.take.toString());
    if (params?.createdAfter) search.set('createdAfter', params.createdAfter);
    if (params?.createdBefore) search.set('createdBefore', params.createdBefore);
    const qs = search.toString();
    return fetchApi<PagedResult<JobRun>>(`/runs${qs ? `?${qs}` : ''}`);
  },
  getAllRuns: async (params?: RunsQueryParams) => {
    const all: JobRun[] = [];
    const pageSize = 500;
    let skip = 0;
    let total = 0;
    do {
      const page = await api.getRuns({ ...params, skip, take: pageSize });
      all.push(...page.items);
      total = page.totalCount;
      skip += pageSize;
    } while (all.length < total);
    return { items: all, totalCount: total } as PagedResult<JobRun>;
  },
  getRun: (id: string) => fetchApi<JobRun>(`/runs/${id}`),
  cancelRun: (id: string) => fetchApi<void>(`/runs/${id}/cancel`, { method: 'POST' }),
  rerunRun: (id: string) => fetchApi<{ runId: string }>(`/runs/${id}/rerun`, { method: 'POST' }),
  getRunLogs: (id: string) => fetchApi<RunLogEntry[]>(`/runs/${id}/logs`),
  getRunTrace: (id: string) => fetchApi<JobRun[]>(`/runs/${id}/trace`),
  updateJob: (name: string, patch: { isEnabled?: boolean }) =>
    fetchApi<JobResponse>(`/jobs/${encodeURIComponent(name)}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(patch),
    }),
  getQueues: () => fetchApi<QueueResponse[]>('/queues'),
  updateQueue: (name: string, patch: { isPaused?: boolean }) =>
    fetchApi<QueueResponse>(`/queues/${encodeURIComponent(name)}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(patch),
    }),
  getNodes: (params?: { includeInactive?: boolean }) => {
    const search = new URLSearchParams();
    if (params?.includeInactive) search.set('includeInactive', 'true');
    const qs = search.toString();
    return fetchApi<NodeResponse[]>(`/nodes${qs ? `?${qs}` : ''}`);
  },
  getNode: (name: string) => fetchApi<NodeResponse>(`/nodes/${encodeURIComponent(name)}`),
  streamRun: (id: string) => {
    return new EventSource(`${BASE}/runs/${id}/stream`);
  },
};
