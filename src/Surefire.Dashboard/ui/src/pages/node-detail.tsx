import { useMemo, useState } from 'react';
import { useQuery, keepPreviousData } from '@tanstack/react-query';
import { type ColumnDef, type PaginationState } from '@tanstack/react-table';
import { useParams, Link } from 'react-router';
import { CircleAlert } from 'lucide-react';
import { api, type JobRun } from '@/lib/api';
import { DataTable } from '@/components/data-table';
import { StatusBadge } from '@/components/status-badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Skeleton } from '@/components/ui/skeleton';
import { formatDate, formatRelative } from '@/lib/format';
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
    accessorKey: "jobName",
    header: "Job",
    cell: ({ row }) => (
      <Link to={`/jobs/${encodeURIComponent(row.original.jobName)}`} className="text-sm text-primary hover:underline truncate max-w-[200px] inline-block" title={row.original.jobName}>
        {row.original.jobName}
      </Link>
    ),
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ row }) => <StatusBadge status={row.original.status} />,
  },
  {
    accessorKey: "startedAt",
    header: "Started",
    cell: ({ row }) => <span className="text-sm">{formatDate(row.original.startedAt)}</span>,
  },
];

export function NodeDetailPage() {
  const { name } = useParams();
  const { data: node, isError } = useQuery({
    queryKey: ['node', name],
    queryFn: () => api.getNode(name!),
    refetchInterval: (query) => query.state.error ? false : 10000,
  });

  const [pagination, setPagination] = useState<PaginationState>({ pageIndex: 0, pageSize: 15 });

  const runsQueryParams = useMemo(() => ({
    nodeName: name!,
    skip: pagination.pageIndex * pagination.pageSize,
    take: pagination.pageSize,
  }), [name, pagination]);

  const { data: runs } = useQuery({
    queryKey: ['runs', 'node', name, runsQueryParams],
    queryFn: () => api.getRuns(runsQueryParams),
    refetchInterval: 5000,
    placeholderData: keepPreviousData,
  });

  if (isError) return <Alert variant="destructive"><CircleAlert /><AlertDescription>Failed to load node</AlertDescription></Alert>;

  if (!node) return (
    <div className="space-y-6">
      <Skeleton className="h-7 w-48" />
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
      <h2 className="text-xl font-semibold tracking-tight truncate">{node.name}</h2>

      <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
        <DtDd label="Started">{formatDate(node.startedAt)}</DtDd>
        <DtDd label="Last heartbeat">{formatRelative(node.lastHeartbeatAt)}</DtDd>
        <DtDd label="Running jobs">{node.runningCount}</DtDd>
        <DtDd label="Queues">{node.registeredQueueNames.join(', ')}</DtDd>
      </dl>

      {node.registeredJobNames.length > 0 && (
        <div className="rounded-lg border overflow-hidden">
          <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
            <span className="text-sm text-muted-foreground">Jobs ({node.registeredJobNames.length})</span>
          </div>
          <ul className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-1 text-sm p-3">
            {node.registeredJobNames.map(jobName => (
              <li key={jobName} className="truncate">
                <Link to={`/jobs/${encodeURIComponent(jobName)}`} className="text-primary hover:underline" title={jobName}>{jobName}</Link>
              </li>
            ))}
          </ul>
        </div>
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
