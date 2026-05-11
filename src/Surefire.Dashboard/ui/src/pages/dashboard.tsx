import {useMemo, useState} from "react";
import {useQuery} from "@tanstack/react-query";
import {api, JobStatus, type DashboardStats} from "@/lib/api";
import {Skeleton} from "@/components/ui/skeleton";
import {StatusBadge} from "@/components/status-badge";
import {LiveDuration} from "@/components/live-duration";
import {formatRelative} from "@/lib/format";
import {Link} from "react-router";
import {Area, AreaChart, CartesianGrid, XAxis, YAxis} from "recharts";
import {
  type ChartConfig,
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
} from "@/components/ui/chart";
import {Alert, AlertDescription} from "@/components/ui/alert";
import {ArrowUpRight, CircleAlert} from "lucide-react";
import {PageShell, PageBody} from "@/components/page-shell";
import {SectionHeader} from "@/components/section-header";
import {TopBarActions} from "@/components/topbar-slot";
import {cn} from "@/lib/utils";

const chartConfig = {
  pending: {label: "Pending", color: "var(--status-pending)"},
  running: {label: "Running", color: "var(--status-running)"},
  succeeded: {label: "Succeeded", color: "var(--status-succeeded)"},
  canceled: {label: "Canceled", color: "var(--status-canceled)"},
  failed: {label: "Failed", color: "var(--status-failed)"},
} satisfies ChartConfig;

const PERIODS: Record<string, { hours: number; bucketMinutes: number }> = {
  "1h": {hours: 1, bucketMinutes: 5},
  "24h": {hours: 24, bucketMinutes: 60},
  "7d": {hours: 168, bucketMinutes: 1440},
  "30d": {hours: 720, bucketMinutes: 1440},
};

function formatBucketLabel(timestamp: string, period: string) {
  const d = new Date(timestamp);
  if (Number.isNaN(d.getTime())) return "";
  if (period === "1h" || period === "24h") {
    return d.toLocaleTimeString(undefined, {hour: "2-digit", minute: "2-digit"});
  }
  return d.toLocaleDateString(undefined, {month: "short", day: "numeric"});
}

export function DashboardPage() {
  const [period, setPeriod] = useState("1h");
  const {data: stats, isError} = useQuery({
    queryKey: ["stats", period],
    queryFn: () => {
      const p = PERIODS[period];
      const since = new Date(Date.now() - p.hours * 3600_000).toISOString();
      return api.getStats({since, bucketMinutes: p.bucketMinutes});
    },
    refetchInterval: 5000,
  });

  const timeline = useMemo(() => stats?.timeline ?? [], [stats?.timeline]);

  return (
    <PageShell>
      <TopBarActions>
        <PeriodTabs value={period} onChange={setPeriod}/>
      </TopBarActions>

      {isError && (
        <PageBody>
          <Alert variant="destructive">
            <CircleAlert/>
            <AlertDescription>Failed to load dashboard</AlertDescription>
          </Alert>
        </PageBody>
      )}

      {!stats && !isError ? (
        <DashboardSkeleton/>
      ) : stats ? (
        <>
          <KPIStrip stats={stats}/>
          <PageBody>
            <section className="grid gap-x-12 gap-y-12 xl:grid-cols-[minmax(0,7fr)_minmax(0,5fr)]">
              <ThroughputPanel stats={stats} timeline={timeline} period={period}/>
              <RecentRunsPanel stats={stats}/>
            </section>
          </PageBody>
        </>
      ) : null}
    </PageShell>
  );
}

function PeriodTabs({value, onChange}: { value: string; onChange: (v: string) => void }) {
  return (
    <div className="inline-flex rounded-md border border-border bg-card p-0.5">
      {Object.entries(PERIODS).map(([key]) => {
        const active = key === value;
        return (
          <button
            key={key}
            type="button"
            onClick={() => onChange(key)}
            className={cn(
              "h-7 min-w-10 px-2.5 text-xs tnum rounded-sm transition-colors inline-flex items-center justify-center leading-none",
              active
                ? "bg-foreground/[0.07] text-foreground"
                : "text-muted-foreground hover:text-foreground",
            )}
          >
            {key}
          </button>
        );
      })}
    </div>
  );
}

interface KPIStripProps {
  stats: DashboardStats;
}

function KPIStrip({stats}: KPIStripProps) {
  const items = [
    {label: "Total jobs", value: stats.totalJobs, accent: false},
    {label: "Total runs", value: stats.totalRuns, accent: false},
    {label: "Active runs", value: stats.activeRuns, accent: stats.activeRuns > 0},
    {label: "Success rate", value: `${(stats.successRate ?? 0).toFixed(1)}`, suffix: "%", accent: false},
    {label: "Nodes", value: stats.nodeCount, accent: false},
  ];
  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 border-b border-border">
      {items.map((item, i) => (
        <div
          key={item.label}
          className={cn(
            "relative px-5 py-5",
            i === 0 && "pl-6",
            i === items.length - 1 && "pr-6",
          )}
          style={{animation: `lift-in 460ms cubic-bezier(0.22,1,0.36,1) both ${i * 50}ms`}}
        >
          <div className="eyebrow">{item.label}</div>
          <div
            className={cn(
              "mt-2.5 font-semibold tracking-[-0.02em] tnum text-[2rem] leading-[1] sm:text-[2.5rem]",
              item.accent ? "text-accent-brand" : "text-foreground",
            )}
          >
            {item.value}
            {item.suffix && (
              <span className="ml-1 text-base text-muted-foreground font-medium">{item.suffix}</span>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

interface ThroughputPanelProps {
  stats: DashboardStats;
  timeline: DashboardStats["timeline"];
  period: string;
}

function ThroughputPanel({stats, timeline, period}: ThroughputPanelProps) {
  const buckets = useMemo(() => buildDistribution(stats), [stats]);
  const total = buckets.reduce((sum, b) => sum + b.count, 0);

  return (
    <div className="min-w-0 space-y-12">
      <div>
        <SectionHeader title="Status"/>
        <div className="mt-3 flex h-[6px] overflow-hidden rounded-sm bg-muted/40">
          {total === 0 ? (
            <div className="w-full bg-muted/40"/>
          ) : (
            buckets.map((b) => {
              const pct = (b.count / total) * 100;
              if (pct <= 0) return null;
              return (
                <div
                  key={b.key}
                  style={{width: `${pct}%`, background: b.color}}
                  className="transition-[width] duration-300"
                  title={`${b.label}: ${b.count}`}
                />
              );
            })
          )}
        </div>
        <div className="mt-3 grid grid-cols-2 sm:grid-cols-5 gap-x-5 gap-y-3">
          {buckets.map((b) => (
            <div key={b.key} className="flex items-center gap-3 min-w-0">
              <span
                className="inline-block h-3 w-[3px] rounded-sm shrink-0"
                style={{background: b.color}}
                aria-hidden
              />
              <div className="min-w-0">
                <div className="eyebrow">{b.label}</div>
                <div className="font-mono text-base tnum text-foreground/85 leading-tight">
                  {b.count}
                  {total > 0 && (
                    <span className="ml-1.5 text-[10.5px] text-muted-foreground/60">
                      {((b.count / total) * 100).toFixed(0)}%
                    </span>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      <div>
        <SectionHeader title="Activity"/>
        <div className="-mx-1">
        {timeline.length > 0 ? (
          <ChartContainer
            config={chartConfig}
            className="aspect-auto h-[320px] w-full"
          >
            <AreaChart data={timeline} margin={{left: 0, right: 8, top: 12}}>
              <defs>
                {Object.entries(chartConfig).map(([key, {color}]) => (
                  <linearGradient
                    key={key}
                    id={`gradient-${key}`}
                    x1="0" y1="0" x2="0" y2="1"
                  >
                    <stop offset="0%" stopColor={color} stopOpacity={0.5}/>
                    <stop offset="100%" stopColor={color} stopOpacity={0.02}/>
                  </linearGradient>
                ))}
              </defs>
              <XAxis
                dataKey="timestamp"
                tickLine={false}
                axisLine={false}
                tick={({x, y, index, visibleTicksCount, payload}) => {
                  const anchor =
                    index === 0 ? "start" : index === visibleTicksCount - 1 ? "end" : "middle";
                  return (
                    <text
                      x={x}
                      y={Number(y) + 14}
                      textAnchor={anchor}
                      className="fill-muted-foreground text-[11px] tnum"
                    >
                      {formatBucketLabel(payload.value, period)}
                    </text>
                  );
                }}
              />
              <CartesianGrid
                horizontal vertical={false}
                strokeDasharray="2 4"
                className="stroke-border/60"
              />
              <YAxis
                allowDecimals={false}
                tickLine={false}
                axisLine={false}
                mirror width={1}
                className="text-[10px]"
              />
              <ChartTooltip
                content={<ChartTooltipContent/>}
                labelFormatter={(v) => formatBucketLabel(v, period)}
              />
              <Area type="monotone" dataKey="pending" stackId="1" stroke="var(--color-pending)" fill="url(#gradient-pending)"/>
              <Area type="monotone" dataKey="running" stackId="1" stroke="var(--color-running)" fill="url(#gradient-running)"/>
              <Area type="monotone" dataKey="succeeded" stackId="1" stroke="var(--color-succeeded)" fill="url(#gradient-succeeded)"/>
              <Area type="monotone" dataKey="canceled" stackId="1" stroke="var(--color-canceled)" fill="url(#gradient-canceled)"/>
              <Area type="monotone" dataKey="failed" stackId="1" stroke="var(--color-failed)" fill="url(#gradient-failed)"/>
            </AreaChart>
          </ChartContainer>
        ) : (
          <div className="flex h-[260px] items-center justify-center text-xs text-muted-foreground">
            no telemetry for this window
          </div>
        )}
        </div>
      </div>
    </div>
  );
}

interface RecentRunsPanelProps {
  stats: DashboardStats;
}

function RecentRunsPanel({stats}: RecentRunsPanelProps) {
  return (
    <div className="min-w-0">
      <SectionHeader
        title="Latest runs"
        actions={
          <Link
            to="/runs"
            className="eyebrow inline-flex items-center gap-1 hover:text-foreground transition-colors"
          >
            All runs
            <ArrowUpRight className="size-3"/>
          </Link>
        }
      />
      {stats.recentRuns.length === 0 ? (
        <p className="eyebrow py-8 text-center">no runs yet</p>
      ) : (
        <ul className="-mx-2.5">
          {stats.recentRuns.slice(0, 10).map((run, idx) => (
            <li key={run.id}>
              <Link
                to={`/runs/${run.id}`}
                className={cn(
                  "group flex items-center gap-3 p-2.5 transition-colors",
                  "border-b border-border/60 last:border-b-0",
                  "hover:bg-accent/40",
                )}
                style={{animation: `lift-in 380ms cubic-bezier(0.22,1,0.36,1) both ${24 * idx}ms`}}
              >
                <div className="flex-1 min-w-0 space-y-1">
                  <div className="truncate text-sm font-medium text-foreground group-hover:text-accent-brand transition-colors" title={run.jobName}>
                    {run.jobName}
                  </div>
                  {run.nodeName && (
                    <div className="text-xs text-muted-foreground truncate" title={run.nodeName}>
                      {run.nodeName}
                    </div>
                  )}
                </div>
                <StatusBadge status={run.status}/>
                <div className="flex flex-col items-end shrink-0 w-20">
                  <span className="font-mono text-[11px] tnum text-muted-foreground">
                    {formatRelative(run.createdAt)}
                  </span>
                  {run.startedAt && (
                    <LiveDuration
                      startedAt={run.startedAt}
                      completedAt={run.completedAt}
                      className="text-[11px] text-muted-foreground/70"
                    />
                  )}
                </div>
              </Link>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

interface DistBucket {
  key: string;
  label: string;
  color: string;
  count: number;
  status: number;
}

function buildDistribution(stats: DashboardStats): DistBucket[] {
  // The dashboard API returns this dictionary keyed by JobStatus *name*,
  // not by integer enum value.
  const lookup = (name: string) =>
    stats.runsByStatus[name] ?? stats.runsByStatus[name.toLowerCase()] ?? 0;
  return [
    {key: "succeeded", label: "Succeeded", color: "var(--status-succeeded)", count: lookup("Succeeded"), status: JobStatus.Succeeded},
    {key: "running", label: "Running", color: "var(--status-running)", count: lookup("Running"), status: JobStatus.Running},
    {key: "pending", label: "Pending", color: "var(--status-pending)", count: lookup("Pending"), status: JobStatus.Pending},
    {key: "canceled", label: "Canceled", color: "var(--status-canceled)", count: lookup("Canceled"), status: JobStatus.Canceled},
    {key: "failed", label: "Failed", color: "var(--status-failed)", count: lookup("Failed"), status: JobStatus.Failed},
  ];
}

function DashboardSkeleton() {
  return (
    <>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 border-b border-border">
        {Array.from({length: 5}).map((_, i) => (
          <div
            key={i}
            className={cn(
              "px-5 py-5",
              i === 0 && "pl-6",
              i === 4 && "pr-6",
            )}
          >
            <Skeleton className="h-3 w-20 rounded-sm"/>
            <Skeleton className="mt-3 h-9 w-24 rounded-sm"/>
          </div>
        ))}
      </div>
      <PageBody>
        <div className="grid gap-x-12 gap-y-12 xl:grid-cols-[minmax(0,7fr)_minmax(0,5fr)]">
          <Skeleton className="h-80 rounded-sm"/>
          <div className="space-y-2">
            {Array.from({length: 10}).map((_, i) => (
              <Skeleton key={i} className="h-9 w-full rounded-sm"/>
            ))}
          </div>
        </div>
      </PageBody>
    </>
  );
}
