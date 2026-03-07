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

export interface JobDefinition {
  name: string;
  description?: string;
  tags: string[];
  cronExpression?: string;
  timeout?: string;
  maxConcurrency: number | null;
  retryPolicy: RetryPolicy;
  isContinuous: boolean;
  isEnabled: boolean;
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
}

export interface NodeInfo {
  name: string;
  startedAt: string;
  lastHeartbeatAt: string;
  runningCount: number;
  registeredJobNames: string[];
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
};

export const JobStatusColors: Record<number, string> = {
  0: 'bg-status-pending/15 text-status-pending',
  1: 'bg-status-running/15 text-status-running',
  2: 'bg-status-completed/15 text-status-completed',
  3: 'bg-status-failed/15 text-status-failed',
  4: 'bg-status-cancelled/15 text-status-cancelled',
  5: 'bg-status-dead-letter/15 text-status-dead-letter',
};

export const LogLevelLabels: Record<number, string> = {
  0: 'Trace',
  1: 'Debug',
  2: 'Information',
  3: 'Warning',
  4: 'Error',
  5: 'Critical',
};

export const api = {
  getStats: (params?: { since?: string; bucketMinutes?: number }) => {
    const search = new URLSearchParams();
    if (params?.since) search.set('since', params.since);
    if (params?.bucketMinutes) search.set('bucketMinutes', params.bucketMinutes.toString());
    const qs = search.toString();
    return fetchApi<DashboardStats>(`/stats${qs ? `?${qs}` : ''}`);
  },
  getJobs: () => fetchApi<JobDefinition[]>('/jobs'),
  getJob: (name: string) => fetchApi<JobDefinition>(`/jobs/${encodeURIComponent(name)}`),
  triggerJob: (name: string, opts?: { args?: unknown; notBefore?: string }) =>
    fetchApi<{ runId: string }>(`/jobs/${encodeURIComponent(name)}/trigger`, {
      method: 'POST',
      ...(opts ? { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(opts) } : {}),
    }),
  getRuns: (params?: { jobName?: string; status?: number; nodeName?: string; parentRunId?: string; retryOfRunId?: string; rerunOfRunId?: string; skip?: number; take?: number; createdAfter?: string; createdBefore?: string }) => {
    const search = new URLSearchParams();
    if (params?.jobName) search.set('jobName', params.jobName);
    if (params?.status !== undefined) search.set('status', params.status.toString());
    if (params?.nodeName) search.set('nodeName', params.nodeName);
    if (params?.parentRunId) search.set('parentRunId', params.parentRunId);
    if (params?.retryOfRunId) search.set('retryOfRunId', params.retryOfRunId);
    if (params?.rerunOfRunId) search.set('rerunOfRunId', params.rerunOfRunId);
    if (params?.skip != null) search.set('skip', params.skip.toString());
    if (params?.take != null) search.set('take', params.take.toString());
    if (params?.createdAfter) search.set('createdAfter', params.createdAfter);
    if (params?.createdBefore) search.set('createdBefore', params.createdBefore);
    const qs = search.toString();
    return fetchApi<PagedResult<JobRun>>(`/runs${qs ? `?${qs}` : ''}`);
  },
  getRun: (id: string) => fetchApi<JobRun>(`/runs/${id}`),
  cancelRun: (id: string) => fetchApi<void>(`/runs/${id}/cancel`, { method: 'POST' }),
  rerunRun: (id: string) => fetchApi<{ runId: string }>(`/runs/${id}/rerun`, { method: 'POST' }),
  getRunLogs: (id: string) => fetchApi<RunLogEntry[]>(`/runs/${id}/logs`),
  getRunTrace: (id: string) => fetchApi<JobRun[]>(`/runs/${id}/trace`),
  updateJob: (name: string, patch: { isEnabled?: boolean }) =>
    fetchApi<JobDefinition>(`/jobs/${encodeURIComponent(name)}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(patch),
    }),
  getNodes: () => fetchApi<NodeInfo[]>('/nodes'),
  getNode: (name: string) => fetchApi<NodeInfo>(`/nodes/${encodeURIComponent(name)}`),
  streamRun: (id: string) => {
    return new EventSource(`${BASE}/runs/${id}/stream`);
  },
};
