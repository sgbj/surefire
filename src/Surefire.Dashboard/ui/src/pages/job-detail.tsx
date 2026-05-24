import {
  keepPreviousData,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { type PaginationState } from "@tanstack/react-table";
import { useParams } from "react-router";
import { useEffect, useMemo, useState } from "react";
import { CirclePlay, Pause } from "lucide-react";
import { toast } from "sonner";
import { api, JobStatusLabels } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { DataTable } from "@/components/data-table";
import { formatDate, formatTimeSpan } from "@/lib/format";
import { metadataGridClass } from "@/components/dt-dd";
import { MetadataStrip, type MetadataItem } from "@/components/metadata-strip";
import { TriggerDialog } from "@/components/trigger-dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { buildRunColumns } from "@/components/run-columns";
import { RUN_DATE_PRESETS } from "@/lib/run-date-presets";
import { PageShell, PageBody } from "@/components/page-shell";
import { PageErrorBanner } from "@/components/page-error-banner";
import { StatePill } from "@/components/status-badge";
import { TopBarActions, TopBarBadge } from "@/components/topbar-slot";
import { Tabs, TabsContent } from "@/components/ui/tabs";
import { TabBar, TabBarTrigger, ToolBar } from "@/components/tab-bar";

const runColumns = buildRunColumns({ showJob: false, showAttempt: true });

export function JobDetailPage() {
  const { name } = useParams();
  const queryClient = useQueryClient();

  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: 15,
  });
  const [statusFilter, setStatusFilter] = useState<string>("all");
  const [datePreset, setDatePreset] = useState("all");
  const [activeTab, setActiveTab] = useState("runs");

  const { data: job, isError } = useQuery({
    queryKey: ["job", name],
    queryFn: () => api.getJob(name!),
    refetchInterval: (query) => (query.state.error ? false : 5000),
  });

  const { data: stats } = useQuery({
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

  const { data: runs } = useQuery({
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

  const resetPage = () => setPagination((prev) => ({ ...prev, pageIndex: 0 }));

  const trigger = useMutation({
    mutationFn: (opts?: {
      args?: unknown;
      notBefore?: string;
      notAfter?: string;
      expiresAt?: string;
      priority?: number;
      deduplicationId?: string;
    }) => api.triggerJob(name!, opts),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["runs", "job", name] });
      toast.success("Job triggered");
    },
    onError: () => toast.error("Failed to trigger job"),
  });

  const toggleEnabled = useMutation({
    mutationFn: (isEnabled: boolean) => api.updateJob(name!, { isEnabled }),
    onSuccess: (_data, isEnabled) => {
      queryClient.invalidateQueries({ queryKey: ["job", name] });
      queryClient.invalidateQueries({ queryKey: ["jobs"] });
      toast.success(isEnabled ? "Job enabled" : "Job disabled");
    },
    onError: () => toast.error("Failed to update job"),
  });

  if (isError)
    return (
      <PageShell>
        <PageErrorBanner message="Failed to load job" />
      </PageShell>
    );

  if (!job)
    return (
      <PageShell>
        <div className={metadataGridClass}>
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i}>
              <Skeleton className="h-3 w-16 mb-1.5 rounded-sm" />
              <Skeleton className="h-4 w-24 rounded-sm" />
            </div>
          ))}
        </div>
        <PageBody>
          <Skeleton className="h-64 w-full rounded-sm" />
        </PageBody>
      </PageShell>
    );

  const renderStatePill = () => {
    if (!job.isActive) return <StatePill tone="muted">Inactive</StatePill>;
    return job.isEnabled ? (
      <StatePill tone="success">
        <span className="inline-block size-1.5 rounded-full bg-current" />
        Enabled
      </StatePill>
    ) : (
      <StatePill tone="muted">Disabled</StatePill>
    );
  };

  const metadataItems: MetadataItem[] = [
    ...(job.description
      ? [
          {
            key: "description",
            fullWidth: true,
            children: (
              <span className="text-sm text-foreground/85">
                {job.description}
              </span>
            ),
          },
        ]
      : []),
    {
      key: "schedule",
      label: "Schedule",
      align: "mono",
      children: job.isContinuous
        ? "continuous"
        : job.cronExpression
          ? `${job.cronExpression}${job.timeZoneId ? ` (${job.timeZoneId})` : ""}`
          : "manual",
    },
    {
      key: "queue",
      label: "Queue",
      align: "mono",
      children: job.queue ?? "default",
    },
    ...(job.nextRunAt
      ? [
          {
            key: "nextRun",
            label: "Next run",
            align: "mono" as const,
            children: formatDate(job.nextRunAt),
          },
        ]
      : []),
    ...(stats && stats.totalRuns > 0
      ? [
          {
            key: "totalRuns",
            label: "Total runs",
            align: "mono" as const,
            children: stats.totalRuns,
          },
          {
            key: "successRate",
            label: "Success rate",
            align: "mono" as const,
            children: `${stats.successRate.toFixed(1)}%`,
          },
        ]
      : []),
    ...(job.maxConcurrency != null
      ? [
          {
            key: "maxConcurrency",
            label: "Max concurrency",
            align: "mono" as const,
            children: job.maxConcurrency,
          },
        ]
      : []),
    ...(job.timeout
      ? [
          {
            key: "timeout",
            label: "Timeout",
            align: "mono" as const,
            children: formatTimeSpan(job.timeout),
          },
        ]
      : []),
    ...(job.retryPolicy.maxAttempts > 1
      ? [
          {
            key: "retries",
            label: "Retries",
            align: "mono" as const,
            children: (
              <>
                {job.retryPolicy.maxAttempts}× ·{" "}
                {job.retryPolicy.backoffType === 1 ? "exp" : "fixed"} ·{" "}
                {formatTimeSpan(job.retryPolicy.initialDelay)}–
                {formatTimeSpan(job.retryPolicy.maxDelay)}
              </>
            ),
          },
        ]
      : []),
    ...(stats?.avgDuration
      ? [
          {
            key: "avgDuration",
            label: "Avg duration",
            align: "mono" as const,
            children: formatTimeSpan(stats.avgDuration),
          },
        ]
      : []),
    ...(stats?.lastRunAt
      ? [
          {
            key: "lastRun",
            label: "Last run",
            align: "mono" as const,
            children: formatDate(stats.lastRunAt),
          },
        ]
      : []),
    ...(job.tags.length > 0
      ? [
          {
            key: "tags",
            label: "Tags",
            children: (
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
            ),
          },
        ]
      : []),
  ];

  return (
    <PageShell>
      <TopBarBadge>{renderStatePill()}</TopBarBadge>
      <TopBarActions>
        <Button
          variant="outline"
          size="sm"
          onClick={() => toggleEnabled.mutate(!job.isEnabled)}
          disabled={toggleEnabled.isPending}
        >
          {job.isEnabled ? (
            <Pause className="size-3.5" />
          ) : (
            <CirclePlay className="size-3.5" />
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

      <MetadataStrip items={metadataItems} />

      <Tabs value={activeTab} onValueChange={setActiveTab} className="gap-0">
        <TabBar>
          <TabBarTrigger value="runs">Runs</TabBarTrigger>
          {job.sourceCode && (
            <TabBarTrigger value="source">Code</TabBarTrigger>
          )}
        </TabBar>

        {activeTab === "runs" && (
          <ToolBar>
            <Select
              value={statusFilter}
              onValueChange={(v) => {
                setStatusFilter(v);
                resetPage();
              }}
            >
              <SelectTrigger size="sm" className="ml-auto w-32">
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
                resetPage();
              }}
            >
              <SelectTrigger size="sm" className="w-32">
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
          </ToolBar>
        )}

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

        {job.sourceCode && (
          <TabsContent value="source" className="mt-0">
            {activeTab === "source" && <JobSourcePanel code={job.sourceCode} />}
          </TabsContent>
        )}
      </Tabs>
    </PageShell>
  );
}

function JobSourcePanel({ code }: { code: string }) {
  return (
    <div className="relative border-b border-border">
      <HighlightedSource code={code} />
    </div>
  );
}

function HighlightedSource({ code }: { code: string }) {
  const [html, setHtml] = useState<string | null>(null);

  useEffect(() => {
    let canceled = false;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- clear stale highlight while the dynamic import loads for the new code
    setHtml(null);
    void Promise.all([
      import("highlight.js/lib/core"),
      import("highlight.js/lib/languages/csharp"),
    ])
      .then(([hljsModule, csharpModule]) => {
        const hljs = hljsModule.default;
        if (!hljs.getLanguage("csharp"))
          hljs.registerLanguage("csharp", csharpModule.default);
        return hljs.highlight(code, {
          language: "csharp",
          ignoreIllegals: true,
        }).value;
      })
      .then((nextHtml) => {
        if (!canceled) setHtml(nextHtml);
      })
      .catch(() => {
        if (!canceled) setHtml(null);
      });

    return () => {
      canceled = true;
    };
  }, [code]);

  if (html) {
    return (
      <pre className="job-source overflow-auto whitespace-pre bg-transparent px-6 py-4 text-xs leading-[1.55] font-mono">
        <code
          className="hljs language-csharp bg-transparent!"
          dangerouslySetInnerHTML={{ __html: html }}
        />
      </pre>
    );
  }

  return (
    <pre className="overflow-auto whitespace-pre px-6 py-4 text-xs leading-[1.55] font-mono text-foreground/85">
      <code>{code}</code>
    </pre>
  );
}
