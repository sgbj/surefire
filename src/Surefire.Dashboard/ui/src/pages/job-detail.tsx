import { useQuery, useMutation, useQueryClient, keepPreviousData } from '@tanstack/react-query';
import { type ColumnDef, type PaginationState } from '@tanstack/react-table';
import { useParams, Link } from 'react-router';
import { useState, useMemo } from 'react';
import { Play, Pause, CirclePlay, CircleAlert } from 'lucide-react';
import { toast } from 'sonner';
import { api, type JobRun } from '@/lib/api';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogTrigger } from '@/components/ui/dialog';
import { DataTable } from '@/components/data-table';
import { StatusBadge } from '@/components/status-badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { formatDate, formatDuration, formatTimeSpan } from '@/lib/format';
import { DtDd } from '@/components/dt-dd';

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
  const [open, setOpen] = useState(false);
  const [argsText, setArgsText] = useState('');
  const [notBeforeText, setNotBeforeText] = useState('');

  const [pagination, setPagination] = useState<PaginationState>({ pageIndex: 0, pageSize: 15 });

  const { data: job, isError } = useQuery({
    queryKey: ['job', name],
    queryFn: () => api.getJob(name!),
    refetchInterval: (query) => query.state.error ? false : 5000,
  });

  const runsQueryParams = useMemo(() => ({
    jobName: name!,
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
    mutationFn: (opts?: { args?: unknown; notBefore?: string }) => api.triggerJob(name!, opts),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['runs', 'job', name] });
      setOpen(false);
      setArgsText('');
      setNotBeforeText('');
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
      <div className="flex items-center justify-between">
        <Skeleton className="h-7 w-48" />
        <div className="flex gap-2">
          <Skeleton className="h-9 w-24" />
          <Skeleton className="h-9 w-16" />
        </div>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
        {Array.from({ length: 3 }).map((_, i) => (
          <div key={i}>
            <Skeleton className="h-3 w-16 mb-1.5" />
            <Skeleton className="h-4 w-24" />
          </div>
        ))}
      </div>
      <Skeleton className="h-64 w-full" />
    </div>
  );

  const handleRun = () => {
    let parsedArgs: unknown = undefined;
    if (argsText.trim()) {
      try {
        parsedArgs = JSON.parse(argsText);
      } catch {
        toast.error('Invalid JSON in arguments');
        return;
      }
    }
    const opts: { args?: unknown; notBefore?: string } = {};
    if (parsedArgs !== undefined) opts.args = parsedArgs;
    if (notBeforeText) opts.notBefore = new Date(notBeforeText).toISOString();
    trigger.mutate(Object.keys(opts).length > 0 ? opts : undefined);
  };

  return (
    <div className="space-y-6">
      <div>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3 min-w-0">
            <h2 className="text-xl font-semibold tracking-tight truncate">{job.name}</h2>
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
            <Dialog open={open} onOpenChange={setOpen}>
              <DialogTrigger asChild>
                <Button variant="outline" className="cursor-pointer">
                  <Play className="size-3.5" />
                  Run
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Run {job.name}</DialogTitle>
                </DialogHeader>
                <div className="space-y-4 py-4">
                  <div>
                    <label htmlFor="trigger-args" className="text-sm font-medium">Arguments (JSON)</label>
                    <textarea
                      id="trigger-args"
                      className="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[100px] outline-none focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px]"
                      placeholder='{"key": "value"}'
                      value={argsText}
                      onChange={(e) => setArgsText(e.target.value)}
                    />
                  </div>
                  <div>
                    <label htmlFor="trigger-not-before" className="text-sm font-medium">Run at (optional)</label>
                    <input
                      id="trigger-not-before"
                      type="datetime-local"
                      className="mt-1 w-full rounded-md border bg-background px-3 py-2 text-sm outline-none focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px]"
                      value={notBeforeText}
                      onChange={(e) => setNotBeforeText(e.target.value)}
                    />
                  </div>
                </div>
                <DialogFooter>
                  <Button variant="outline" onClick={() => setOpen(false)}>Cancel</Button>
                  <Button onClick={handleRun} disabled={trigger.isPending}>Run</Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </div>
        </div>
        {job.description && <p className="mt-1 text-muted-foreground">{job.description}</p>}
      </div>

      <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
        <DtDd label="Schedule">{job.isContinuous ? 'Continuous' : job.cronExpression ?? 'Manual'}</DtDd>
        {job.maxConcurrency != null && <DtDd label="Max concurrency">{job.maxConcurrency}</DtDd>}
        {job.retryPolicy.maxAttempts > 1 && (
          <DtDd label="Retries">
            {`${job.retryPolicy.maxAttempts} attempts, ${job.retryPolicy.backoffType === 1 ? 'exponential' : 'fixed'} ${formatTimeSpan(job.retryPolicy.initialDelay)}–${formatTimeSpan(job.retryPolicy.maxDelay)}`}
          </DtDd>
        )}
        {job.timeout && <DtDd label="Timeout">{formatTimeSpan(job.timeout)}</DtDd>}
        {job.tags.length > 0 && (
          <DtDd label="Tags">
            <div className="flex flex-wrap gap-1">{job.tags.map(t => <Badge key={t} variant="secondary">{t}</Badge>)}</div>
          </DtDd>
        )}
      </dl>

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
