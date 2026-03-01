import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { type ColumnDef } from '@tanstack/react-table';
import { useParams, Link } from 'react-router';
import { CircleAlert } from 'lucide-react';
import { api, type JobRun } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { DataTable } from '@/components/data-table';
import { SortableHeader } from '@/components/sortable-header';
import { StatusBadge } from '@/components/status-badge';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Spinner } from '@/components/ui/spinner';
import { Input } from '@/components/ui/input';
import { formatDate } from '@/lib/format';

function DtDd({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <dt className="text-sm text-muted-foreground">{label}</dt>
      <dd className="text-sm">{children}</dd>
    </div>
  );
}

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
    header: ({ column }) => <SortableHeader column={column}>Job</SortableHeader>,
    cell: ({ row }) => (
      <Link to={`/jobs/${encodeURIComponent(row.original.jobName)}`} className="text-sm text-primary hover:underline">
        {row.original.jobName}
      </Link>
    ),
  },
  {
    accessorKey: "status",
    header: ({ column }) => <SortableHeader column={column}>Status</SortableHeader>,
    cell: ({ row }) => <StatusBadge status={row.original.status} />,
  },
  {
    accessorKey: "startedAt",
    header: ({ column }) => <SortableHeader column={column}>Started</SortableHeader>,
    cell: ({ row }) => <span className="text-sm">{formatDate(row.original.startedAt)}</span>,
  },
];

export function NodeDetailPage() {
  const { id } = useParams();
  const { data: node, isError } = useQuery({
    queryKey: ['node', id],
    queryFn: () => api.getNode(id!),
    refetchInterval: (query) => query.state.error ? false : 10000,
  });
  const { data: runs } = useQuery({
    queryKey: ['runs', 'node', id],
    queryFn: () => api.getRuns({ nodeName: id!, take: 100 }),
    refetchInterval: 5000,
  });

  const [runFilter, setRunFilter] = useState('');
  const filteredRuns = useMemo(() => {
    if (!runs) return [];
    if (!runFilter) return runs.items;
    const lower = runFilter.toLowerCase();
    return runs.items.filter(r => r.jobName.toLowerCase().includes(lower));
  }, [runs, runFilter]);

  if (isError) return <Alert variant="destructive"><CircleAlert /><AlertDescription>Failed to load node</AlertDescription></Alert>;

  if (!node) return <div className="flex items-center gap-2 text-muted-foreground"><Spinner className="size-4" />Loading...</div>;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3 min-w-0">
        <h2 className="text-2xl font-bold tracking-tight truncate">{node.name}</h2>
        <Badge variant={node.status === 0 ? 'default' : 'secondary'} className="shrink-0">
          {node.status === 0 ? 'Online' : 'Offline'}
        </Badge>
      </div>

      <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
        <DtDd label="Started">{formatDate(node.startedAt)}</DtDd>
        <DtDd label="Running jobs">{node.runningCount}</DtDd>
      </dl>

      {node.registeredJobNames.length > 0 && (
        <div>
          <h3 className="text-lg font-semibold mb-2">Jobs</h3>
          <ul className="text-sm space-y-1">
            {node.registeredJobNames.map(name => (
              <li key={name}>
                <Link to={`/jobs/${encodeURIComponent(name)}`} className="text-primary hover:underline">{name}</Link>
              </li>
            ))}
          </ul>
        </div>
      )}

      <div>
        <h3 className="text-lg font-semibold mb-2">Runs</h3>
        <DataTable
          columns={runColumns}
          data={filteredRuns}
          toolbar={
            <Input
              placeholder="Filter by job..."
              value={runFilter}
              onChange={(e) => setRunFilter(e.target.value)}
              className="max-w-sm"
            />
          }
        />
      </div>
    </div>
  );
}
