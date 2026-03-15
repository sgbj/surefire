import { useState, useMemo, useEffect, useRef } from 'react';
import { useQuery, keepPreviousData } from '@tanstack/react-query';
import { type ColumnDef, type PaginationState } from '@tanstack/react-table';
import { api, JobStatusLabels, type JobRun } from '@/lib/api';
import { StatusBadge } from '@/components/status-badge';
import { DataTable } from '@/components/data-table';
import { formatDate, formatDuration } from '@/lib/format';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Link } from 'react-router';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { CircleAlert, Search } from 'lucide-react';

function useDebouncedValue<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);
  const isFirst = useRef(true);
  useEffect(() => {
    if (isFirst.current) { isFirst.current = false; return; }
    const id = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(id);
  }, [value, delay]);
  return debounced;
}

const columns: ColumnDef<JobRun>[] = [
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
    accessorKey: "nodeName",
    header: "Node",
    cell: ({ row }) => row.original.nodeName
      ? <Link to={`/nodes/${encodeURIComponent(row.original.nodeName)}`} className="text-sm text-primary hover:underline truncate max-w-[160px] inline-block" title={row.original.nodeName}>{row.original.nodeName}</Link>
      : null,
  },
];

const DATE_PRESETS: { label: string; value: string; getAfter: () => string | undefined }[] = [
  { label: 'All time', value: 'all', getAfter: () => undefined },
  { label: 'Last hour', value: '1h', getAfter: () => new Date(Date.now() - 3600_000).toISOString() },
  { label: 'Last 24 hours', value: '24h', getAfter: () => new Date(Date.now() - 86400_000).toISOString() },
  { label: 'Last 7 days', value: '7d', getAfter: () => new Date(Date.now() - 604800_000).toISOString() },
  { label: 'Last 30 days', value: '30d', getAfter: () => new Date(Date.now() - 2592000_000).toISOString() },
];

export function RunsPage() {
  const [pagination, setPagination] = useState<PaginationState>({ pageIndex: 0, pageSize: 15 });
  const [jobNameInput, setJobNameInput] = useState('');
  const [statusFilter, setStatusFilter] = useState<string>('all');
  const [datePreset, setDatePreset] = useState('all');

  const debouncedJobName = useDebouncedValue(jobNameInput, 300);

  // Reset to page 1 when debounced filter changes
  const prevJobName = useRef(debouncedJobName);
  useEffect(() => {
    if (prevJobName.current !== debouncedJobName) {
      prevJobName.current = debouncedJobName;
      setPagination(prev => ({ ...prev, pageIndex: 0 }));
    }
  }, [debouncedJobName]);

  // Use datePreset (not a computed date) in the query key so the cache key is stable.
  // Compute the actual createdAfter date inside queryFn so each refetch uses a fresh timestamp.
  const queryKey = useMemo(() => ({
    jobName: debouncedJobName || undefined,
    status: statusFilter !== 'all' ? Number(statusFilter) : undefined,
    datePreset,
    skip: pagination.pageIndex * pagination.pageSize,
    take: pagination.pageSize,
  }), [debouncedJobName, statusFilter, datePreset, pagination]);

  const { data, isError } = useQuery({
    queryKey: ['runs', queryKey],
    queryFn: () => {
      const preset = DATE_PRESETS.find(p => p.value === datePreset);
      return api.getRuns({
        jobName: queryKey.jobName,
        status: queryKey.status,
        createdAfter: preset?.getAfter(),
        skip: queryKey.skip,
        take: queryKey.take,
      });
    },
    refetchInterval: 5000,
    placeholderData: keepPreviousData,
  });

  const totalCount = data?.totalCount ?? 0;
  const pageCount = Math.ceil(totalCount / pagination.pageSize);

  const resetPage = () => setPagination(prev => ({ ...prev, pageIndex: 0 }));

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold tracking-tight">Runs</h2>
      {isError && <Alert variant="destructive"><CircleAlert /><AlertDescription>Failed to load runs</AlertDescription></Alert>}
      <DataTable
        columns={columns}
        data={data?.items ?? []}
        showColumnVisibility
        manualPagination
        pageCount={pageCount}
        totalCount={totalCount}
        pagination={pagination}
        onPaginationChange={setPagination}
        defaultPageSize={15}
        toolbar={
          <>
            <div className="relative max-w-sm">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-4 text-muted-foreground/60" />
              <Input
                placeholder="Search..."
                value={jobNameInput}
                onChange={(e) => setJobNameInput(e.target.value)}
                className="pl-8"
              />
            </div>
            <Select value={statusFilter} onValueChange={(v) => { setStatusFilter(v); resetPage(); }}>
              <SelectTrigger size="sm" className="w-[140px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent position="popper">
                <SelectItem value="all">All statuses</SelectItem>
                {Object.entries(JobStatusLabels).map(([val, label]) => (
                  <SelectItem key={val} value={val}>{label}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={datePreset} onValueChange={(v) => { setDatePreset(v); resetPage(); }}>
              <SelectTrigger size="sm" className="w-[140px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent position="popper">
                {DATE_PRESETS.map((p) => (
                  <SelectItem key={p.value} value={p.value}>{p.label}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </>
        }
      />
    </div>
  );
}
