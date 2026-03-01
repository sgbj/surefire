import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { type ColumnDef } from '@tanstack/react-table';
import { api, type JobDefinition } from '@/lib/api';
import { Badge } from '@/components/ui/badge';
import { DataTable } from '@/components/data-table';
import { SortableHeader } from '@/components/sortable-header';
import { Input } from '@/components/ui/input';
import { Link } from 'react-router';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { CircleAlert } from 'lucide-react';

const columns: ColumnDef<JobDefinition>[] = [
  {
    accessorKey: "name",
    header: ({ column }) => <SortableHeader column={column}>Name</SortableHeader>,
    cell: ({ row }) => (
      <Link to={`/jobs/${encodeURIComponent(row.original.name)}`} className="font-medium text-primary hover:underline">
        {row.original.name}
      </Link>
    ),
  },
  {
    accessorKey: "description",
    header: "Description",
    cell: ({ row }) => <span className="text-muted-foreground">{row.original.description ?? '-'}</span>,
  },
  {
    accessorKey: "cronExpression",
    header: "Cron",
    cell: ({ row }) => <span className="text-sm">{row.original.cronExpression ?? '-'}</span>,
  },
  {
    accessorKey: "isEnabled",
    header: "Status",
    cell: ({ row }) => row.original.isEnabled
      ? <Badge variant="outline" className="text-emerald-700 dark:text-emerald-400">Enabled</Badge>
      : <Badge variant="outline" className="text-muted-foreground">Disabled</Badge>,
  },
  {
    accessorKey: "tags",
    header: "Tags",
    cell: ({ row }) => (
      <div className="flex gap-1">
        {row.original.tags.map(tag => <Badge key={tag} variant="outline">{tag}</Badge>)}
      </div>
    ),
  },
];

export function JobsPage() {
  const { data: jobs, isError } = useQuery({ queryKey: ['jobs'], queryFn: api.getJobs });
  const [filter, setFilter] = useState('');

  const filtered = useMemo(() => {
    if (!jobs) return [];
    if (!filter) return jobs;
    const lower = filter.toLowerCase();
    return jobs.filter(j =>
      j.name.toLowerCase().includes(lower) ||
      j.description?.toLowerCase().includes(lower) ||
      j.tags.some(t => t.toLowerCase().includes(lower))
    );
  }, [jobs, filter]);

  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold tracking-tight">Jobs</h2>
      {isError && <Alert variant="destructive"><CircleAlert /><AlertDescription>Failed to load jobs</AlertDescription></Alert>}
      <DataTable
        columns={columns}
        data={filtered}
        toolbar={
          <Input
            placeholder="Filter jobs..."
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="max-w-sm"
          />
        }
      />
    </div>
  );
}
