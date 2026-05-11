import {keepPreviousData, useMutation, useQuery, useQueryClient,} from "@tanstack/react-query";
import {type PaginationState} from "@tanstack/react-table";
import {useParams} from "react-router";
import {useMemo, useState} from "react";
import {CircleAlert, CirclePlay, Pause} from "lucide-react";
import {Alert, AlertDescription} from "@/components/ui/alert";
import {toast} from "sonner";
import {api, JobStatusLabels} from "@/lib/api";
import {Button} from "@/components/ui/button";
import {Skeleton} from "@/components/ui/skeleton";
import {DataTable} from "@/components/data-table";
import {formatDate, formatTimeSpan} from "@/lib/format";
import {DtDd} from "@/components/dt-dd";
import {TriggerDialog} from "@/components/trigger-dialog";
import {Select, SelectContent, SelectItem, SelectTrigger, SelectValue,} from "@/components/ui/select";
import {buildRunColumns} from "@/components/run-columns";
import {RUN_DATE_PRESETS} from "@/lib/run-date-presets";
import {PageShell, PageBody} from "@/components/page-shell";
import {StatePill} from "@/components/status-badge";
import {TopBarActions, TopBarBadge} from "@/components/topbar-slot";

const runColumns = buildRunColumns({showJob: false, showAttempt: true});

export function JobDetailPage() {
  const {name} = useParams();
  const queryClient = useQueryClient();

  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: 15,
  });
  const [statusFilter, setStatusFilter] = useState<string>("all");
  const [datePreset, setDatePreset] = useState("all");

  const {data: job, isError} = useQuery({
    queryKey: ["job", name],
    queryFn: () => api.getJob(name!),
    refetchInterval: (query) => (query.state.error ? false : 5000),
  });

  const {data: stats} = useQuery({
    queryKey: ["job-stats", name],
    queryFn: () => api.getJobStats(name!),
    refetchInterval: 10000,
  });

  const runsQueryParams = useMemo(
    () => ({
      jobName: name!,
      status: statusFilter !== "all" ? Number(statusFilter) : undefined,
      datePreset,
      skip: pagination.pageIndex * pagination.pageSize,
      take: pagination.pageSize,
    }),
    [name, statusFilter, datePreset, pagination],
  );

  const {data: runs} = useQuery({
    queryKey: ["runs", "job", name, runsQueryParams],
    queryFn: () => {
      const preset = RUN_DATE_PRESETS.find((p) => p.value === datePreset);
      return api.getRuns({
        ...runsQueryParams,
        createdAfter: preset?.getAfter(),
      });
    },
    refetchInterval: 5000,
    placeholderData: keepPreviousData,
  });

  const resetPage = () => setPagination((prev) => ({...prev, pageIndex: 0}));

  const trigger = useMutation({
    mutationFn: (opts?: {
      args?: unknown;
      notBefore?: string;
      notAfter?: string;
      priority?: number;
      deduplicationId?: string;
    }) => api.triggerJob(name!, opts),
    onSuccess: () => {
      queryClient.invalidateQueries({queryKey: ["runs", "job", name]});
      toast.success("Job triggered");
    },
    onError: () => toast.error("Failed to trigger job"),
  });

  const toggleEnabled = useMutation({
    mutationFn: (isEnabled: boolean) => api.updateJob(name!, {isEnabled}),
    onSuccess: (_data, isEnabled) => {
      queryClient.invalidateQueries({queryKey: ["job", name]});
      queryClient.invalidateQueries({queryKey: ["jobs"]});
      toast.success(isEnabled ? "Job enabled" : "Job disabled");
    },
    onError: () => toast.error("Failed to update job"),
  });

  if (isError)
    return (
      <PageShell>
        <PageBody>
          <Alert variant="destructive">
            <CircleAlert/>
            <AlertDescription>Failed to load job</AlertDescription>
          </Alert>
        </PageBody>
      </PageShell>
    );

  if (!job)
    return (
      <PageShell>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-x-6 gap-y-5 border-b border-border px-6 py-5">
          {Array.from({length: 6}).map((_, i) => (
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

  const renderStatePill = () => {
    if (!job.isActive) return <StatePill tone="muted">Inactive</StatePill>;
    return job.isEnabled ? (
      <StatePill tone="success">
        <span className="inline-block size-1.5 rounded-full bg-current"/>
        Enabled
      </StatePill>
    ) : (
      <StatePill tone="muted">Disabled</StatePill>
    );
  };

  return (
    <PageShell>
      <TopBarBadge>
        {renderStatePill()}
      </TopBarBadge>
      <TopBarActions>
        <Button
          variant="outline"
          size="sm"
          onClick={() => toggleEnabled.mutate(!job.isEnabled)}
          disabled={toggleEnabled.isPending}
        >
          {job.isEnabled ? (
            <Pause className="size-3.5"/>
          ) : (
            <CirclePlay className="size-3.5"/>
          )}
          {job.isEnabled ? "Disable" : "Enable"}
        </Button>
        <TriggerDialog
          jobName={job.name}
          argumentsSchema={job.argumentsSchema}
          isPending={trigger.isPending}
          onTrigger={(opts) => trigger.mutate(opts)}
        />
      </TopBarActions>

      <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-x-6 gap-y-5 border-b border-border px-6 py-5">
        {job.description && (
          <DtDd label="Description" className="col-span-full">
            <span className="text-sm text-foreground/85">{job.description}</span>
          </DtDd>
        )}
        <DtDd label="Schedule" align="mono">
          {job.isContinuous
            ? "continuous"
            : job.cronExpression
              ? `${job.cronExpression}${job.timeZoneId ? ` (${job.timeZoneId})` : ""}`
              : "manual"}
        </DtDd>
        <DtDd label="Queue" align="mono">{job.queue ?? "default"}</DtDd>
        {job.maxConcurrency != null && (
          <DtDd label="Max concurrency" align="mono">{job.maxConcurrency}</DtDd>
        )}
        {job.timeout && (
          <DtDd label="Timeout" align="mono">{formatTimeSpan(job.timeout)}</DtDd>
        )}
        {job.retryPolicy.maxAttempts > 1 && (
          <DtDd label="Retries" align="mono">
            {job.retryPolicy.maxAttempts}× ·{" "}
            {job.retryPolicy.backoffType === 1 ? "exp" : "fixed"} ·{" "}
            {formatTimeSpan(job.retryPolicy.initialDelay)}–{formatTimeSpan(job.retryPolicy.maxDelay)}
          </DtDd>
        )}
        {job.nextRunAt && (
          <DtDd label="Next run" align="mono">{formatDate(job.nextRunAt)}</DtDd>
        )}
        {stats && stats.totalRuns > 0 && (
          <>
            <DtDd label="Total runs" align="mono">{stats.totalRuns}</DtDd>
            <DtDd label="Success rate" align="mono">{stats.successRate.toFixed(1)}%</DtDd>
            {stats.avgDuration && (
              <DtDd label="Avg duration" align="mono">{formatTimeSpan(stats.avgDuration)}</DtDd>
            )}
            {stats.lastRunAt && (
              <DtDd label="Last run" align="mono">{formatDate(stats.lastRunAt)}</DtDd>
            )}
          </>
        )}
        {job.tags.length > 0 && (
          <DtDd label="Tags" className="col-span-2">
            <div className="flex flex-wrap gap-1.5">
              {job.tags.map((t) => (
                <span
                  key={t}
                  className="inline-flex h-5 items-center rounded-sm border border-border bg-muted/40 px-1.5 text-xs text-muted-foreground"
                >
                  {t}
                </span>
              ))}
            </div>
          </DtDd>
        )}
      </dl>

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
        toolbar={
          <>
            <Select
              value={statusFilter}
              onValueChange={(v) => {
                setStatusFilter(v);
                resetPage();
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
                resetPage();
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
        }
      />
    </PageShell>
  );
}
