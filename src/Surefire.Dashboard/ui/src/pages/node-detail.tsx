import { useMemo, useState } from 'react';
import { useQuery, keepPreviousData } from '@tanstack/react-query';
import { type ColumnDef, type PaginationState } from '@tanstack/react-table';
import { useParams, Link } from 'react-router';
import { CircleAlert } from 'lucide-react';
import { api, type JobRun } from '@/lib/api';
import { DataTable } from '@/components/data-table';
import { StatusBadge } from '@/components/status-badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Spinner } from '@/components/ui/spinner';
import { formatDate, formatRelative } from '@/lib/format';
import { DtDd } from '@/components/dt-dd';

const runColumns: ColumnDef<JobRun>[] = [
  {
    accessorKey: "id",
    header: "ID",
    cell: ({ row }) => (
      <Link to={`/runs/${row.original.id}`} className="text-sm text-primary hover:underline">
        {row.original.id}
      </Link>
    ),
  },
  {
    accessorKey: "jobName",
    header: "Job",
    cell: ({ row }) => (
      <Link to={`/jobs/${encodeURIComponent(row.original.jobName)}`} className="text-sm text-primary hover:underline">
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

  const [pagination, setPagination] = useState<PaginationState>({ pageIndex: 0, pageSize: 20 });

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

  if (!node) return <div className="flex items-center gap-2 text-muted-foreground"><Spinner className="size-4" />Loading...</div>;

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold tracking-tight truncate">{node.name}</h2>

      <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
        <DtDd label="Started">{formatDate(node.startedAt)}</DtDd>
        <DtDd label="Last heartbeat">{formatRelative(node.lastHeartbeatAt)}</DtDd>
        <DtDd label="Running jobs">{node.runningCount}</DtDd>
      </dl>

      {node.registeredJobNames.length > 0 && (
        <div>
          <h3 className="text-lg font-semibold mb-2">Jobs</h3>
          <ul className="text-sm space-y-1">
            {node.registeredJobNames.map(jobName => (
              <li key={jobName}>
                <Link to={`/jobs/${encodeURIComponent(jobName)}`} className="text-primary hover:underline">{jobName}</Link>
              </li>
            ))}
          </ul>
        </div>
      )}

      <div>
        <h3 className="text-lg font-semibold mb-2">Runs</h3>
        <DataTable
          columns={runColumns}
          data={runs?.items ?? []}
          manualPagination
          pageCount={Math.ceil((runs?.totalCount ?? 0) / pagination.pageSize)}
          totalCount={runs?.totalCount ?? 0}
          pagination={pagination}
          onPaginationChange={setPagination}
          defaultPageSize={20}
        />
      </div>
    </div>
  );
}
