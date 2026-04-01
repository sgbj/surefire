import { useMemo, useState } from "react";
import { useQuery, keepPreviousData } from "@tanstack/react-query";
import { type ColumnDef, type PaginationState } from "@tanstack/react-table";
import { useParams } from "react-router";
import { CircleAlert, ListFilter, Search } from "lucide-react";
import { api, JobStatusLabels, type JobResponse, type JobRun } from "@/lib/api";
import { DataTable } from "@/components/data-table";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { formatDate, formatRelative } from "@/lib/format";
import { DtDd } from "@/components/dt-dd";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { buildRunColumns } from "@/components/run-columns";
import { buildJobColumns } from "@/components/job-columns";
import { RUN_DATE_PRESETS } from "@/lib/run-date-presets";
import { useDebouncedValue } from "@/hooks/use-debounced-value";

const runColumns: ColumnDef<JobRun>[] = buildRunColumns({ showStarted: true });
const jobColumns: ColumnDef<JobResponse>[] = buildJobColumns();

export function NodeDetailPage() {
  const { name } = useParams();
  const { data: node, isError } = useQuery({
    queryKey: ["node", name],
    queryFn: () => api.getNode(name!),
    refetchInterval: (query) => (query.state.error ? false : 10000),
  });

  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: 15,
  });
  const [jobNameInput, setJobNameInput] = useState("");
  const [statusFilter, setStatusFilter] = useState<string>("all");
  const [datePreset, setDatePreset] = useState("all");
  const [jobSearch, setJobSearch] = useState("");
  const [showInactiveJobs, setShowInactiveJobs] = useState(false);

  const debouncedJobName = useDebouncedValue(jobNameInput, 300);

  const runsQueryKey = useMemo(
    () => ({
      nodeName: name!,
      jobName: debouncedJobName || undefined,
      status: statusFilter !== "all" ? Number(statusFilter) : undefined,
      datePreset,
      skip: pagination.pageIndex * pagination.pageSize,
      take: pagination.pageSize,
    }),
    [name, debouncedJobName, statusFilter, datePreset, pagination],
  );

  const { data: runs } = useQuery({
    queryKey: ["runs", "node", name, runsQueryKey],
    queryFn: () => {
      const preset = RUN_DATE_PRESETS.find((p) => p.value === datePreset);
      return api.getRuns({
        nodeName: runsQueryKey.nodeName,
        jobName: runsQueryKey.jobName,
        status: runsQueryKey.status,
        createdAfter: preset?.getAfter(),
        skip: runsQueryKey.skip,
        take: runsQueryKey.take,
      });
    },
    refetchInterval: 5000,
    placeholderData: keepPreviousData,
  });

  const { data: jobs } = useQuery({
    queryKey: ["jobs", "node", name, showInactiveJobs],
    queryFn: () =>
      api.getJobs({ includeInactive: showInactiveJobs || undefined }),
  });

  const nodeScopedJobs = useMemo(() => {
    const registered = new Set(node?.registeredJobNames ?? []);
    const source = jobs ?? [];
    const filteredByNode = source.filter((j) => registered.has(j.name));
    if (!jobSearch) return filteredByNode;

    const lower = jobSearch.toLowerCase();
    return filteredByNode.filter(
      (j) =>
        j.name.toLowerCase().includes(lower) ||
        j.description?.toLowerCase().includes(lower) ||
        j.tags.some((t) => t.toLowerCase().includes(lower)),
    );
  }, [jobs, node?.registeredJobNames, jobSearch]);

  if (isError)
    return (
      <Alert variant="destructive">
        <CircleAlert />
        <AlertDescription>Failed to load node</AlertDescription>
      </Alert>
    );

  if (!node)
    return (
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
      <h2 className="text-xl font-semibold tracking-tight truncate">
        {node.name}
      </h2>

      <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
        <DtDd label="Started">{formatDate(node.startedAt)}</DtDd>
        <DtDd label="Last heartbeat">
          {formatRelative(node.lastHeartbeatAt)}
        </DtDd>
        <DtDd label="Running jobs">{node.runningCount}</DtDd>
        <DtDd label="Queues">{node.registeredQueueNames.join(", ")}</DtDd>
      </dl>

      <Tabs defaultValue="runs" className="space-y-3">
        <TabsList>
          <TabsTrigger value="runs">Runs</TabsTrigger>
          <TabsTrigger value="jobs">Jobs</TabsTrigger>
        </TabsList>

        <TabsContent value="runs">
          <DataTable
            columns={runColumns}
            data={runs?.items ?? []}
            manualPagination
            pageCount={Math.ceil((runs?.totalCount ?? 0) / pagination.pageSize)}
            totalCount={runs?.totalCount ?? 0}
            pagination={pagination}
            onPaginationChange={setPagination}
            defaultPageSize={15}
            toolbar={
              <>
                <div className="relative max-w-sm">
                  <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-4 text-muted-foreground/60" />
                  <Input
                    aria-label="Search node runs"
                    placeholder="Search..."
                    value={jobNameInput}
                    onChange={(e) => {
                      const next = e.target.value;
                      setJobNameInput(next);
                      if (pagination.pageIndex !== 0) {
                        setPagination((prev) => ({ ...prev, pageIndex: 0 }));
                      }
                    }}
                    className="pl-8"
                  />
                </div>
                <Select
                  value={statusFilter}
                  onValueChange={(v) => {
                    setStatusFilter(v);
                    setPagination((prev) => ({ ...prev, pageIndex: 0 }));
                  }}
                >
                  <SelectTrigger size="sm" className="w-[140px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent position="popper">
                    <SelectItem value="all">All statuses</SelectItem>
                    {Object.entries(JobStatusLabels).map(([val, label]) => (
                      <SelectItem key={val} value={val}>
                        {label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Select
                  value={datePreset}
                  onValueChange={(v) => {
                    setDatePreset(v);
                    setPagination((prev) => ({ ...prev, pageIndex: 0 }));
                  }}
                >
                  <SelectTrigger size="sm" className="w-[140px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent position="popper">
                    {RUN_DATE_PRESETS.map((p) => (
                      <SelectItem key={p.value} value={p.value}>
                        {p.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </>
            }
            header={
              <span className="text-sm text-muted-foreground">
                Runs ({runs?.totalCount ?? 0})
              </span>
            }
          />
        </TabsContent>

        <TabsContent value="jobs">
          <DataTable
            columns={jobColumns}
            data={nodeScopedJobs}
            toolbar={
              <>
                <div className="relative max-w-sm">
                  <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-4 text-muted-foreground/60" />
                  <Input
                    aria-label="Search node jobs"
                    placeholder="Search..."
                    value={jobSearch}
                    onChange={(e) => setJobSearch(e.target.value)}
                    className="pl-8"
                  />
                </div>
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button
                      variant="outline"
                      size="sm"
                      className={showInactiveJobs ? "border-primary/50" : ""}
                    >
                      <ListFilter className="size-4" />
                      Filter
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <DropdownMenuCheckboxItem
                      checked={showInactiveJobs}
                      onCheckedChange={setShowInactiveJobs}
                    >
                      Inactive jobs
                    </DropdownMenuCheckboxItem>
                  </DropdownMenuContent>
                </DropdownMenu>
              </>
            }
            header={
              <span className="text-sm text-muted-foreground">
                Jobs ({nodeScopedJobs.length})
              </span>
            }
          />
        </TabsContent>
      </Tabs>
    </div>
  );
}
