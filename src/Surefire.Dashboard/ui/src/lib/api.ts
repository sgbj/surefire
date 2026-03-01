const BASE = `${new URL(document.baseURI).pathname.replace(/\/$/, '')}/api`;

async function fetchApi<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, init);
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`API error ${res.status}: ${body}`);
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
  originalRunId?: string;
  notBefore?: string;
}

export interface NodeInfo {
  name: string;
  startedAt: string;
  lastHeartbeatAt: string;
  status: number;
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
  5: 'Dead Letter',
};

export const JobStatusColors: Record<number, string> = {
  0: 'bg-amber-100/80 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300',
  1: 'bg-sky-100/80 text-sky-700 dark:bg-sky-900/30 dark:text-sky-300',
  2: 'bg-emerald-100/80 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300',
  3: 'bg-rose-100/80 text-rose-700 dark:bg-rose-900/30 dark:text-rose-300',
  4: 'bg-slate-100/80 text-slate-600 dark:bg-slate-800/30 dark:text-slate-400',
  5: 'bg-violet-100/80 text-violet-700 dark:bg-violet-900/30 dark:text-violet-300',
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
      headers: { 'Content-Type': 'application/json' },
      body: opts ? JSON.stringify(opts) : undefined,
    }),
  getRuns: (params?: { jobName?: string; status?: number; nodeName?: string; parentRunId?: string; originalRunId?: string; skip?: number; take?: number; createdAfter?: string; createdBefore?: string }) => {
    const search = new URLSearchParams();
    if (params?.jobName) search.set('jobName', params.jobName);
    if (params?.status !== undefined) search.set('status', params.status.toString());
    if (params?.nodeName) search.set('nodeName', params.nodeName);
    if (params?.parentRunId) search.set('parentRunId', params.parentRunId);
    if (params?.originalRunId) search.set('originalRunId', params.originalRunId);
    if (params?.skip) search.set('skip', params.skip.toString());
    if (params?.take) search.set('take', params.take.toString());
    if (params?.createdAfter) search.set('createdAfter', params.createdAfter);
    if (params?.createdBefore) search.set('createdBefore', params.createdBefore);
    const qs = search.toString();
    return fetchApi<PagedResult<JobRun>>(`/runs${qs ? `?${qs}` : ''}`);
  },
  getRun: (id: string) => fetchApi<JobRun>(`/runs/${id}`),
  cancelRun: (id: string) => fetchApi<void>(`/runs/${id}/cancel`, { method: 'POST' }),
  rerunRun: (id: string) => fetchApi<{ runId: string }>(`/runs/${id}/rerun`, { method: 'POST' }),
  getRunLogs: (id: string) => fetchApi<RunLogEntry[]>(`/runs/${id}/logs`),
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
