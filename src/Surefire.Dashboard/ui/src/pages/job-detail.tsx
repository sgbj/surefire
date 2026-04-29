import {keepPreviousData, useMutation, useQuery, useQueryClient,} from "@tanstack/react-query";
import {type PaginationState} from "@tanstack/react-table";
import {useParams} from "react-router";
import {useMemo, useState} from "react";
import {CircleAlert, CirclePlay, Pause} from "lucide-react";
import {Alert, AlertDescription} from "@/components/ui/alert";
import {toast} from "sonner";
import {api, JobStatusLabels} from "@/lib/api";
import {Button} from "@/components/ui/button";
import {Badge} from "@/components/ui/badge";
import {Skeleton} from "@/components/ui/skeleton";
import {DataTable} from "@/components/data-table";
import {formatDate, formatTimeSpan} from "@/lib/format";
import {DtDd} from "@/components/dt-dd";
import {TriggerDialog} from "@/components/trigger-dialog";
import {Select, SelectContent, SelectItem, SelectTrigger, SelectValue,} from "@/components/ui/select";
import {buildRunColumns} from "@/components/run-columns";
import {RUN_DATE_PRESETS} from "@/lib/run-date-presets";

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
      <div className="space-y-6">
        <h2 className="text-xl font-semibold tracking-tight truncate">
          {name}
        </h2>
        <Alert variant="destructive">
          <CircleAlert/>
          <AlertDescription>Failed to load job</AlertDescription>
        </Alert>
      </div>
    );

  if (!job)
    return (
      <div className="space-y-6">
        <div>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <Skeleton className="h-7 w-48"/>
            </div>
            <div className="flex items-center gap-2">
              <Skeleton className="h-9 w-[5.5rem]"/>
              <Skeleton className="h-9 w-16"/>
            </div>
          </div>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
          {Array.from({length: 4}).map((_, i) => (
            <div key={i}>
              <Skeleton className="h-3 w-16 mb-1.5"/>
              <Skeleton className="h-4 w-24"/>
            </div>
          ))}
        </div>
        <Skeleton className="h-64 w-full rounded-lg"/>
      </div>
    );

  return (
    <div className="space-y-6">
      <div>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3 min-w-0">
            <h2 className="text-xl font-semibold tracking-tight truncate">
              {job.name}
            </h2>
            {!job.isEnabled && (
              <Badge
                variant="outline"
                className="text-muted-foreground shrink-0"
              >
                Disabled
              </Badge>
            )}
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              className="cursor-pointer"
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
          </div>
        </div>
        {job.description && (
          <p className="mt-1 text-muted-foreground">{job.description}</p>
        )}
      </div>

      <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
        <DtDd label="Schedule">
          {job.isContinuous
            ? "Continuous"
            : job.cronExpression
              ? `${job.cronExpression}${job.timeZoneId ? ` (${job.timeZoneId})` : ""}`
              : "Manual"}
        </DtDd>
        {job.maxConcurrency != null && (
          <DtDd label="Max concurrency">{job.maxConcurrency}</DtDd>
        )}
        {job.retryPolicy.maxAttempts > 1 && (
          <DtDd label="Retries">
            {`${job.retryPolicy.maxAttempts} attempts, ${job.retryPolicy.backoffType === 1 ? "exponential" : "fixed"} ${formatTimeSpan(job.retryPolicy.initialDelay)}–${formatTimeSpan(job.retryPolicy.maxDelay)}`}
          </DtDd>
        )}
        <DtDd label="Queue">{job.queue ?? "default"}</DtDd>
        {job.timeout && (
          <DtDd label="Timeout">{formatTimeSpan(job.timeout)}</DtDd>
        )}
        {job.nextRunAt && (
          <DtDd label="Next run">{formatDate(job.nextRunAt)}</DtDd>
        )}
        {job.tags.length > 0 && (
          <DtDd label="Tags">
            <div className="flex flex-wrap gap-1">
              {job.tags.map((t) => (
                <Badge key={t} variant="secondary">
                  {t}
                </Badge>
              ))}
            </div>
          </DtDd>
        )}
        {stats && stats.totalRuns > 0 && (
          <>
            <DtDd label="Total runs">{stats.totalRuns}</DtDd>
            <DtDd label="Success rate">{stats.successRate.toFixed(1)}%</DtDd>
            {stats.avgDuration && (
              <DtDd label="Avg duration">
                {formatTimeSpan(stats.avgDuration)}
              </DtDd>
            )}
            {stats.lastRunAt && (
              <DtDd label="Last run">{formatDate(stats.lastRunAt)}</DtDd>
            )}
          </>
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
        toolbar={
          <>
            <Select
              value={statusFilter}
              onValueChange={(v) => {
                setStatusFilter(v);
                resetPage();
              }}
            >
              <SelectTrigger size="sm" className="w-[140px]">
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
              <SelectTrigger size="sm" className="w-[140px]">
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
        header={
          <span className="text-sm text-muted-foreground">
            Runs ({runs?.totalCount ?? 0})
          </span>
        }
      />
    </div>
  );
}
