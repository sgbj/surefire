import { useMemo, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { type ColumnDef } from '@tanstack/react-table';
import { api, type QueueResponse } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { DataTable } from '@/components/data-table';
import { SortableHeader } from '@/components/sortable-header';
import { Input } from '@/components/ui/input';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { CircleAlert, Pause, Play, Search } from 'lucide-react';
import { toast } from 'sonner';

export function QueuesPage() {
  const queryClient = useQueryClient();
  const { data: queues, isError } = useQuery({
    queryKey: ['queues'],
    queryFn: () => api.getQueues(),
    refetchInterval: 10000,
  });
  const [filter, setFilter] = useState('');

  const togglePause = useMutation({
    mutationFn: ({ name, isPaused }: { name: string; isPaused: boolean }) =>
      api.updateQueue(name, { isPaused }),
    onSuccess: (_data, { isPaused }) => {
      queryClient.invalidateQueries({ queryKey: ['queues'] });
      toast.success(isPaused ? 'Queue paused' : 'Queue resumed');
    },
    onError: () => toast.error('Failed to update queue'),
  });

  const columns: ColumnDef<QueueResponse>[] = [
    {
      accessorKey: "name",
      header: ({ column }) => <SortableHeader column={column}>Name</SortableHeader>,
      cell: ({ row }) => <span className="font-medium">{row.original.name}</span>,
    },
    {
      accessorKey: "priority",
      header: ({ column }) => <SortableHeader column={column}>Priority</SortableHeader>,
    },
    {
      accessorKey: "pendingCount",
      header: ({ column }) => <SortableHeader column={column}>Pending</SortableHeader>,
      cell: ({ row }) => (
        <span className={row.original.pendingCount > 0 ? 'font-medium' : 'text-muted-foreground'}>
          {row.original.pendingCount}
        </span>
      ),
    },
    {
      accessorKey: "runningCount",
      header: ({ column }) => <SortableHeader column={column}>Running</SortableHeader>,
      cell: ({ row }) => (
        <span className={row.original.runningCount > 0 ? 'font-medium' : 'text-muted-foreground'}>
          {row.original.runningCount}
        </span>
      ),
    },
    {
      id: "maxConcurrency",
      header: "Concurrency",
      cell: ({ row }) => (
        <span className="text-sm text-muted-foreground">
          {row.original.maxConcurrency != null
            ? `${row.original.runningCount} / ${row.original.maxConcurrency}`
            : 'Unlimited'}
        </span>
      ),
    },
    {
      id: "status",
      header: "Status",
      cell: ({ row }) => row.original.isPaused
        ? <Badge variant="outline" className="text-amber-600 dark:text-amber-400">Paused</Badge>
        : <Badge variant="outline" className="text-emerald-700 dark:text-emerald-400">Active</Badge>,
    },
    {
      id: "processingNodes",
      header: "Nodes",
      cell: ({ row }) => (
        <span className="text-sm text-muted-foreground">
          {row.original.processingNodes.length > 0
            ? row.original.processingNodes.join(', ')
            : <span className="italic">None</span>}
        </span>
      ),
    },
    {
      id: "actions",
      cell: ({ row }) => (
        <div className="flex justify-end">
          <Button
            variant="ghost"
            size="icon"
            className="size-8 cursor-pointer"
            onClick={() => togglePause.mutate({ name: row.original.name, isPaused: !row.original.isPaused })}
            disabled={togglePause.isPending}
          >
            {row.original.isPaused
              ? <Play className="size-3.5" />
              : <Pause className="size-3.5" />}
          </Button>
        </div>
      ),
    },
  ];

  const filtered = useMemo(() => {
    if (!queues) return [];
    if (!filter) return queues;
    const lower = filter.toLowerCase();
    return queues.filter(q => q.name.toLowerCase().includes(lower));
  }, [queues, filter]);

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold tracking-tight">Queues</h2>
      {isError && <Alert variant="destructive"><CircleAlert /><AlertDescription>Failed to load queues</AlertDescription></Alert>}
      <DataTable
        columns={columns}
        data={filtered}
        toolbar={
          <div className="relative max-w-sm">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-4 text-muted-foreground/60" />
            <Input
              placeholder="Search..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              className="pl-8"
            />
          </div>
        }
      />
    </div>
  );
}
