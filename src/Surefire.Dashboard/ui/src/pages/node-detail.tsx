import {useMemo, useState} from "react";
import {keepPreviousData, useQuery} from "@tanstack/react-query";
import {type ColumnDef, type PaginationState} from "@tanstack/react-table";
import {useParams} from "react-router";
import {Search} from "lucide-react";
import {api, type JobResponse, type JobRun, JobStatusLabels} from "@/lib/api";
import {DataTable} from "@/components/data-table";
import {Switch} from "@/components/ui/switch";
import {Input} from "@/components/ui/input";
import {Skeleton} from "@/components/ui/skeleton";
import {Tabs, TabsContent} from "@/components/ui/tabs";
import {TabBar, TabBarTrigger, ToolBar} from "@/components/tab-bar";
import {formatDate, formatRelative} from "@/lib/format";
import {metadataGridClass} from "@/components/dt-dd";
import {MetadataStrip, type MetadataItem} from "@/components/metadata-strip";
import {Select, SelectContent, SelectItem, SelectTrigger, SelectValue,} from "@/components/ui/select";
import {buildRunColumns} from "@/components/run-columns";
import {buildJobColumns} from "@/components/job-columns";
import {RUN_DATE_PRESETS} from "@/lib/run-date-presets";
import {useDebouncedValue} from "@/hooks/use-debounced-value";
import {PageShell, PageBody} from "@/components/page-shell";
import {PageErrorBanner} from "@/components/page-error-banner";
import {StatePill} from "@/components/status-badge";
import {TopBarBadge} from "@/components/topbar-slot";

const runColumns: ColumnDef<JobRun>[] = buildRunColumns({showStarted: true});
const jobColumns: ColumnDef<JobResponse>[] = buildJobColumns();

export function NodeDetailPage() {
  const {name} = useParams();
  const {data: node, isError} = useQuery({
    queryKey: ["node", name],
    queryFn: () => api.getNode(name!),
    refetchInterval: (query) => (query.state.error ? false : 10000),
  });

  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: 15,
  });
  const [activeTab, setActiveTab] = useState("runs");
  const [jobNameInput, setJobNameInput] = useState("");
  const [statusFilter, setStatusFilter] = useState<string>("all");
  const [datePreset, setDatePreset] = useState("all");
  const [jobSearch, setJobSearch] = useState("");
  const [showInactiveJobs, setShowInactiveJobs] = useState(false);

  const debouncedJobName = useDebouncedValue(jobNameInput, 300);

  const runsQueryKey = useMemo(
    () => ({
      nodeName: name!,
      jobNameContains: debouncedJobName || undefined,
      status: statusFilter !== "all" ? Number(statusFilter) : undefined,
      datePreset,
      skip: pagination.pageIndex * pagination.pageSize,
      take: pagination.pageSize,
    }),
    [name, debouncedJobName, statusFilter, datePreset, pagination],
  );

  const {data: runs} = useQuery({
    queryKey: ["runs", "node", name, runsQueryKey],
    queryFn: () => {
      const preset = RUN_DATE_PRESETS.find((p) => p.value === datePreset);
      return api.getRuns({
        nodeName: runsQueryKey.nodeName,
        jobNameContains: runsQueryKey.jobNameContains,
        status: runsQueryKey.status,
        createdAfter: preset?.getAfter(),
        skip: runsQueryKey.skip,
        take: runsQueryKey.take,
      });
    },
    refetchInterval: 5000,
    placeholderData: keepPreviousData,
  });

  const {data: jobs} = useQuery({
    queryKey: ["jobs", "node", name, showInactiveJobs],
    queryFn: () =>
      api.getJobs({includeInactive: showInactiveJobs || undefined}),
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
      <PageShell>
        <PageErrorBanner message="Failed to load node" />
      </PageShell>
    );

  if (!node)
    return (
      <PageShell>
        <div className={metadataGridClass}>
          {Array.from({length: 4}).map((_, i) => (
            <div key={i}>
              <Skeleton className="h-3 w-16 mb-1.5 rounded-sm"/>
              <Skeleton className="h-4 w-24 rounded-sm"/>
            </div>
          ))}
        </div>
        <PageBody>
          <Skeleton className="h-64 w-full rounded-sm"/>
        </PageBody>
      </PageShell>
    );

  return (
    <PageShell>
      <TopBarBadge>
        {node.isActive ? (
          <StatePill tone="success">
            <span className="inline-block size-1.5 rounded-full bg-current"/>
            Active
          </StatePill>
        ) : (
          <StatePill tone="muted">Inactive</StatePill>
        )}
      </TopBarBadge>

      <MetadataStrip
        items={[
          {key: "started", label: "Started", align: "mono", children: formatDate(node.startedAt)},
          {key: "heartbeat", label: "Last heartbeat", align: "mono", children: formatRelative(node.lastHeartbeatAt)},
          {key: "runningJobs", label: "Running jobs", align: "mono", children: node.runningCount},
          {
            key: "queues",
            label: "Queues",
            children: (
              <span className="font-mono text-sm text-foreground/85">
                {node.registeredQueueNames.join(", ")}
              </span>
            ),
          },
        ] satisfies MetadataItem[]}
      />

      <Tabs value={activeTab} onValueChange={setActiveTab} className="gap-0">
        <TabBar>
          <TabBarTrigger value="runs">Runs</TabBarTrigger>
          <TabBarTrigger value="jobs">Jobs</TabBarTrigger>
        </TabBar>

        <ToolBar>
          {activeTab === "runs" ? (
            <>
              <div className="relative max-w-sm">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground/60"/>
                <Input
                  aria-label="Search node runs"
                  placeholder="Filter runs…"
                  value={jobNameInput}
                  onChange={(e) => {
                    const next = e.target.value;
                    setJobNameInput(next);
                    if (pagination.pageIndex !== 0) {
                      setPagination((prev) => ({...prev, pageIndex: 0}));
                    }
                  }}
                  className="h-8 pl-8"
                />
              </div>
              <Select
                value={statusFilter}
                onValueChange={(v) => {
                  setStatusFilter(v);
                  setPagination((prev) => ({...prev, pageIndex: 0}));
                }}
              >
                <SelectTrigger size="sm" className="w-32">
                  <SelectValue/>
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
                  setPagination((prev) => ({...prev, pageIndex: 0}));
                }}
              >
                <SelectTrigger size="sm" className="w-32">
                  <SelectValue/>
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
          ) : (
            <>
              <div className="relative max-w-sm">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground/60"/>
                <Input
                  aria-label="Search node jobs"
                  placeholder="Filter jobs…"
                  value={jobSearch}
                  onChange={(e) => setJobSearch(e.target.value)}
                  className="h-8 pl-8"
                />
              </div>
              <label className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors cursor-pointer select-none">
                <Switch
                  size="sm"
                  checked={showInactiveJobs}
                  onCheckedChange={setShowInactiveJobs}
                />
                Show inactive
              </label>
            </>
          )}
        </ToolBar>

        <TabsContent value="runs" className="mt-0">
          <DataTable
            columns={runColumns}
            data={runs?.items ?? []}
            manualPagination
            pageCount={Math.ceil((runs?.totalCount ?? 0) / pagination.pageSize)}
            totalCount={runs?.totalCount ?? 0}
            pagination={pagination}
            onPaginationChange={setPagination}
            defaultPageSize={15}
            getRowHref={(r) => `/runs/${r.id}`}
            getRowLinkLabel={(r) => `Open run ${r.id}`}
          />
        </TabsContent>

        <TabsContent value="jobs" className="mt-0">
          <DataTable
            columns={jobColumns}
            data={nodeScopedJobs}
            getRowHref={(r) => `/jobs/${encodeURIComponent(r.name)}`}
            getRowLinkLabel={(r) => `Open job ${r.name}`}
          />
        </TabsContent>
      </Tabs>
    </PageShell>
  );
}
