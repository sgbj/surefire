import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { type ColumnDef } from '@tanstack/react-table';
import { api, type NodeInfo } from '@/lib/api';
import { DataTable } from '@/components/data-table';
import { SortableHeader } from '@/components/sortable-header';
import { Input } from '@/components/ui/input';
import { formatRelative } from '@/lib/format';
import { Link } from 'react-router';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { CircleAlert } from 'lucide-react';

const columns: ColumnDef<NodeInfo>[] = [
  {
    accessorKey: "name",
    header: ({ column }) => <SortableHeader column={column}>Name</SortableHeader>,
    cell: ({ row }) => (
      <Link to={`/nodes/${encodeURIComponent(row.original.name)}`} className="font-medium text-primary hover:underline">
        {row.original.name}
      </Link>
    ),
  },
  {
    accessorKey: "lastHeartbeatAt",
    header: ({ column }) => <SortableHeader column={column}>Last heartbeat</SortableHeader>,
    cell: ({ row }) => <span className="text-sm">{formatRelative(row.original.lastHeartbeatAt)}</span>,
  },
  {
    accessorKey: "runningCount",
    header: ({ column }) => <SortableHeader column={column}>Running</SortableHeader>,
  },
  {
    id: "registeredJobs",
    header: "Jobs",
    cell: ({ row }) => <span className="text-sm">{row.original.registeredJobNames.length}</span>,
  },
];

export function NodesPage() {
  const { data: nodes, isError } = useQuery({ queryKey: ['nodes'], queryFn: api.getNodes, refetchInterval: 10000 });
  const [filter, setFilter] = useState('');

  const filtered = useMemo(() => {
    if (!nodes) return [];
    if (!filter) return nodes;
    const lower = filter.toLowerCase();
    return nodes.filter(n => n.name.toLowerCase().includes(lower));
  }, [nodes, filter]);

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold tracking-tight">Nodes</h2>
      {isError && <Alert variant="destructive"><CircleAlert /><AlertDescription>Failed to load nodes</AlertDescription></Alert>}
      <DataTable
        columns={columns}
        data={filtered}
        toolbar={
          <Input
            placeholder="Filter nodes..."
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="max-w-sm"
          />
        }
      />
    </div>
  );
}
