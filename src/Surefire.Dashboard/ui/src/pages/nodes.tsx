import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { type ColumnDef } from '@tanstack/react-table';
import { api, type NodeResponse } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { DataTable } from '@/components/data-table';
import { DropdownMenu, DropdownMenuCheckboxItem, DropdownMenuContent, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { SortableHeader } from '@/components/sortable-header';
import { Input } from '@/components/ui/input';
import { formatRelative } from '@/lib/format';
import { Link } from 'react-router';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { CircleAlert, ListFilter, Search } from 'lucide-react';

const columns: ColumnDef<NodeResponse>[] = [
  {
    accessorKey: "name",
    header: ({ column }) => <SortableHeader column={column}>Name</SortableHeader>,
    cell: ({ row }) => (
      <Link to={`/nodes/${encodeURIComponent(row.original.name)}`} className={`font-medium text-primary hover:underline truncate max-w-[200px] inline-block ${!row.original.isActive ? 'opacity-50' : ''}`} title={row.original.name}>
        {row.original.name}
      </Link>
    ),
  },
  {
    accessorKey: "lastHeartbeatAt",
    header: ({ column }) => <SortableHeader column={column}>Last heartbeat</SortableHeader>,
    cell: ({ row }) => <span className={`text-sm ${!row.original.isActive ? 'opacity-50' : ''}`}>{formatRelative(row.original.lastHeartbeatAt)}</span>,
  },
  {
    accessorKey: "runningCount",
    header: ({ column }) => <SortableHeader column={column}>Running</SortableHeader>,
    cell: ({ row }) => <span className={!row.original.isActive ? 'opacity-50' : ''}>{row.original.runningCount}</span>,
  },
  {
    id: "status",
    header: "Status",
    cell: ({ row }) => row.original.isActive
      ? <Badge variant="outline" className="text-emerald-700 dark:text-emerald-400">Active</Badge>
      : <Badge variant="outline" className="text-muted-foreground opacity-50">Inactive</Badge>,
  },
  {
    id: "queues",
    header: "Queues",
    cell: ({ row }) => (
      <span className={`text-sm ${!row.original.isActive ? 'opacity-50' : ''}`}>
        {row.original.registeredQueueNames.join(', ')}
      </span>
    ),
  },
  {
    id: "registeredJobs",
    header: "Jobs",
    cell: ({ row }) => <span className={`text-sm ${!row.original.isActive ? 'opacity-50' : ''}`}>{row.original.registeredJobNames.length}</span>,
  },
];

export function NodesPage() {
  const [showInactive, setShowInactive] = useState(false);
  const { data: nodes, isError } = useQuery({
    queryKey: ['nodes', showInactive],
    queryFn: () => api.getNodes(showInactive ? { includeInactive: true } : undefined),
    refetchInterval: 10000,
  });
  const [filter, setFilter] = useState('');

  const filtered = useMemo(() => {
    if (!nodes) return [];
    if (!filter) return nodes;
    const lower = filter.toLowerCase();
    return nodes.filter(n => n.name.toLowerCase().includes(lower));
  }, [nodes, filter]);

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold tracking-tight">Nodes</h2>
      {isError && <Alert variant="destructive"><CircleAlert /><AlertDescription>Failed to load nodes</AlertDescription></Alert>}
      <DataTable
        columns={columns}
        data={filtered}
        toolbar={
          <>
            <div className="relative max-w-sm">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-4 text-muted-foreground/60" />
              <Input
                placeholder="Search..."
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                className="pl-8"
              />
            </div>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" className={showInactive ? 'border-primary/50' : ''}>
                  <ListFilter className="size-4" />
                  Filter
                  {showInactive && <Badge variant="secondary" className="ml-1 px-1 py-0 text-[10px]">1</Badge>}
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuCheckboxItem checked={showInactive} onCheckedChange={setShowInactive}>
                  Inactive nodes
                </DropdownMenuCheckboxItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </>
        }
      />
    </div>
  );
}
