import { useQuery, useMutation, useQueryClient, keepPreviousData } from '@tanstack/react-query';
import { type ColumnDef, type PaginationState } from '@tanstack/react-table';
import { useParams, Link } from 'react-router';
import { useState, useMemo } from 'react';
import { Pause, CirclePlay, CircleAlert } from 'lucide-react';
import { toast } from 'sonner';
import { api, type JobRun } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { DataTable } from '@/components/data-table';
import { StatusBadge } from '@/components/status-badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { formatDate, formatDuration, formatTimeSpan } from '@/lib/format';
import { DtDd } from '@/components/dt-dd';
import { PlanGraphView } from '@/components/plan-graph-view';
import { TriggerDialog } from '@/components/trigger-dialog';

const runColumns: ColumnDef<JobRun>[] = [
  {
    accessorKey: "id",
    header: "ID",
    cell: ({ row }) => (
      <Link to={`/runs/${row.original.id}`} className="text-sm text-primary hover:underline truncate max-w-[140px] inline-block" title={row.original.id}>
        {row.original.id}
      </Link>
    ),
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ row }) => <StatusBadge status={row.original.status} />,
  },
  {
    accessorKey: "createdAt",
    header: "Created",
    cell: ({ row }) => <span className="text-sm">{formatDate(row.original.createdAt)}</span>,
  },
  {
    id: "duration",
    header: "Duration",
    cell: ({ row }) => <span className="text-sm">{formatDuration(row.original.startedAt, row.original.completedAt)}</span>,
  },
  {
    accessorKey: "attempt",
    header: "Attempt",
  },
];

export function JobDetailPage() {
  const { name } = useParams();
  const queryClient = useQueryClient();

  const [pagination, setPagination] = useState<PaginationState>({ pageIndex: 0, pageSize: 15 });

  const { data: job, isError } = useQuery({
    queryKey: ['job', name],
    queryFn: () => api.getJob(name!),
    refetchInterval: (query) => query.state.error ? false : 5000,
  });

  const { data: stats } = useQuery({
    queryKey: ['job-stats', name],
    queryFn: () => api.getJobStats(name!),
    refetchInterval: 10000,
  });

  const runsQueryParams = useMemo(() => ({
    jobName: name!,
    exactJobName: true,
    skip: pagination.pageIndex * pagination.pageSize,
    take: pagination.pageSize,
  }), [name, pagination]);

  const { data: runs } = useQuery({
    queryKey: ['runs', 'job', name, runsQueryParams],
    queryFn: () => api.getRuns(runsQueryParams),
    refetchInterval: 5000,
    placeholderData: keepPreviousData,
  });

  const trigger = useMutation({
    mutationFn: (opts?: { args?: unknown; notBefore?: string; notAfter?: string; priority?: number; deduplicationId?: string }) => api.triggerJob(name!, opts),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['runs', 'job', name] });
      toast.success('Job triggered');
    },
    onError: () => toast.error('Failed to trigger job'),
  });

  const toggleEnabled = useMutation({
    mutationFn: (isEnabled: boolean) => api.updateJob(name!, { isEnabled }),
    onSuccess: (_data, isEnabled) => {
      queryClient.invalidateQueries({ queryKey: ['job', name] });
      queryClient.invalidateQueries({ queryKey: ['jobs'] });
      toast.success(isEnabled ? 'Job enabled' : 'Job disabled');
    },
    onError: () => toast.error('Failed to update job'),
  });

  if (isError) return <Alert variant="destructive"><CircleAlert /><AlertDescription>Failed to load job</AlertDescription></Alert>;

  if (!job) return (
    <div className="space-y-6">
      <div>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Skeleton className="h-7 w-48" />
          </div>
          <div className="flex items-center gap-2">
            <Skeleton className="h-9 w-[5.5rem]" />
            <Skeleton className="h-9 w-16" />
          </div>
        </div>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i}>
            <Skeleton className="h-3 w-16 mb-1.5" />
            <Skeleton className="h-4 w-24" />
          </div>
        ))}
      </div>
      <Skeleton className="h-64 w-full rounded-lg" />
    </div>
  );

  return (
    <div className="space-y-6">
      <div>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3 min-w-0">
            <h2 className="text-xl font-semibold tracking-tight truncate">{job.name}</h2>
            {job.isPlan && <Badge variant="secondary" className="shrink-0">Plan</Badge>}
            {!job.isEnabled && <Badge variant="outline" className="text-muted-foreground shrink-0">Disabled</Badge>}
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              className="cursor-pointer"
              onClick={() => toggleEnabled.mutate(!job.isEnabled)}
              disabled={toggleEnabled.isPending}
            >
              {job.isEnabled ? <Pause className="size-3.5" /> : <CirclePlay className="size-3.5" />}
              {job.isEnabled ? 'Disable' : 'Enable'}
            </Button>
            <TriggerDialog
              jobName={job.name}
              argumentsSchema={job.argumentsSchema}
              isPending={trigger.isPending}
              onTrigger={(opts) => trigger.mutate(opts)}
            />
          </div>
        </div>
        {job.description && <p className="mt-1 text-muted-foreground">{job.description}</p>}
      </div>

      <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
        <DtDd label="Schedule">{job.isContinuous ? 'Continuous' : job.cronExpression ? `${job.cronExpression}${job.timeZoneId ? ` (${job.timeZoneId})` : ''}` : 'Manual'}</DtDd>
        {job.maxConcurrency != null && <DtDd label="Max concurrency">{job.maxConcurrency}</DtDd>}
        {job.retryPolicy.maxAttempts > 1 && (
          <DtDd label="Retries">
            {`${job.retryPolicy.maxAttempts} attempts, ${job.retryPolicy.backoffType === 1 ? 'exponential' : 'fixed'} ${formatTimeSpan(job.retryPolicy.initialDelay)}–${formatTimeSpan(job.retryPolicy.maxDelay)}`}
          </DtDd>
        )}
        <DtDd label="Queue">{job.queue ?? 'default'}</DtDd>
        {job.timeout && <DtDd label="Timeout">{formatTimeSpan(job.timeout)}</DtDd>}
        {job.nextRunAt && <DtDd label="Next run">{formatDate(job.nextRunAt)}</DtDd>}
        {job.tags.length > 0 && (
          <DtDd label="Tags">
            <div className="flex flex-wrap gap-1">{job.tags.map(t => <Badge key={t} variant="secondary">{t}</Badge>)}</div>
          </DtDd>
        )}
        {stats && stats.totalRuns > 0 && (
          <>
            <DtDd label="Total runs">{stats.totalRuns}</DtDd>
            <DtDd label="Success rate">{stats.successRate.toFixed(1)}%</DtDd>
            {stats.avgDuration && <DtDd label="Avg duration">{formatTimeSpan(stats.avgDuration)}</DtDd>}
            {stats.lastRunAt && <DtDd label="Last run">{formatDate(stats.lastRunAt)}</DtDd>}
          </>
        )}
      </dl>

      {job.isPlan && job.planGraph && (
        <PlanGraphView planGraph={job.planGraph} stepRuns={[]} />
      )}

      <DataTable
        columns={runColumns}
        data={runs?.items ?? []}
        manualPagination
        pageCount={Math.ceil((runs?.totalCount ?? 0) / pagination.pageSize)}
        totalCount={runs?.totalCount ?? 0}
        pagination={pagination}
        onPaginationChange={setPagination}
        defaultPageSize={15}
        header={<span className="text-sm text-muted-foreground">Runs ({runs?.totalCount ?? 0})</span>}
      />
    </div>
  );
}
