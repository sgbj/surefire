import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { StatusBadge } from "@/components/status-badge";
import { formatRelative } from "@/lib/format";
import { Link } from "react-router";
import { AreaChart, Area, XAxis, YAxis, CartesianGrid } from "recharts";
import {
  type ChartConfig,
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  ChartLegend,
  ChartLegendContent,
} from "@/components/ui/chart";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Alert, AlertDescription } from "@/components/ui/alert";
import {
  CircleAlert,
  Workflow,
  Play,
  Activity,
  TrendingUp,
  type LucideIcon,
} from "lucide-react";

const chartConfig = {
  pending: { label: "Pending", color: "var(--status-pending)" },
  running: { label: "Running", color: "var(--status-running)" },
  completed: { label: "Completed", color: "var(--status-completed)" },
  retrying: { label: "Retrying", color: "var(--status-retrying)" },
  cancelled: { label: "Cancelled", color: "var(--status-cancelled)" },
  deadLetter: { label: "Dead letter", color: "var(--status-dead-letter)" },
} satisfies ChartConfig;

const PERIODS: Record<string, { hours: number; bucketMinutes: number }> = {
  "1h": { hours: 1, bucketMinutes: 5 },
  "24h": { hours: 24, bucketMinutes: 60 },
  "7d": { hours: 168, bucketMinutes: 1440 },
  "30d": { hours: 720, bucketMinutes: 1440 },
};

function formatBucketLabel(timestamp: string, period: string) {
  const d = new Date(timestamp);
  if (Number.isNaN(d.getTime())) return "";
  if (period === "1h" || period === "24h") {
    return d.toLocaleTimeString(undefined, {
      hour: "2-digit",
      minute: "2-digit",
    });
  }
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

export function DashboardPage() {
  const [period, setPeriod] = useState("1h");
  const { data: stats, isError } = useQuery({
    queryKey: ["stats", period],
    queryFn: () => {
      const p = PERIODS[period];
      const since = new Date(Date.now() - p.hours * 3600_000).toISOString();
      return api.getStats({ since, bucketMinutes: p.bucketMinutes });
    },
    refetchInterval: 5000,
  });

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold tracking-tight">Dashboard</h2>
        <Tabs value={period} onValueChange={setPeriod}>
          <TabsList>
            <TabsTrigger value="1h">1h</TabsTrigger>
            <TabsTrigger value="24h">24h</TabsTrigger>
            <TabsTrigger value="7d">7d</TabsTrigger>
            <TabsTrigger value="30d">30d</TabsTrigger>
          </TabsList>
        </Tabs>
      </div>

      {isError && (
        <Alert variant="destructive">
          <CircleAlert />
          <AlertDescription>Failed to load dashboard</AlertDescription>
        </Alert>
      )}

      {!stats && !isError && (
        <>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <Card key={i} className="gap-3 py-5">
                <CardHeader>
                  <div className="flex items-center gap-2">
                    <Skeleton className="size-4 rounded" />
                    <Skeleton className="h-4 w-20" />
                  </div>
                </CardHeader>
                <CardContent>
                  <Skeleton className="h-8 w-16" />
                </CardContent>
              </Card>
            ))}
          </div>
          <div className="space-y-6">
            <Card className="pt-0 gap-0">
              <CardHeader className="border-b py-3! gap-0! grid-rows-none! rounded-t-xl bg-muted/30 backdrop-blur-sm">
                <Skeleton className="h-4 w-28" />
              </CardHeader>
              <CardContent className="pt-4 h-[300px]" />
            </Card>
            <Card className="pt-0 gap-0 pb-0 bg-transparent shadow-none">
              <CardHeader className="py-3! gap-0! grid-rows-none! rounded-t-xl bg-muted/30 backdrop-blur-sm">
                <Skeleton className="h-4 w-24" />
              </CardHeader>
              <CardContent className="px-0">
                {Array.from({ length: 5 }).map((_, i) => (
                  <div
                    key={i}
                    className="flex items-center justify-between gap-4 px-6 py-2.5 border-t"
                  >
                    <Skeleton className="h-4 w-32" />
                    <div className="flex items-center gap-3 shrink-0">
                      <Skeleton className="h-5 w-[4.5rem] rounded-full" />
                      <Skeleton className="h-3 w-14" />
                    </div>
                  </div>
                ))}
              </CardContent>
            </Card>
          </div>
        </>
      )}

      {stats && (
        <>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            <StatCard
              title="Total jobs"
              value={stats.totalJobs}
              icon={Workflow}
            />
            <StatCard title="Total runs" value={stats.totalRuns} icon={Play} />
            <StatCard
              title="Active runs"
              value={stats.activeRuns}
              icon={Activity}
            />
            <StatCard
              title="Success rate"
              value={`${(stats.successRate ?? 0).toFixed(1)}%`}
              icon={TrendingUp}
            />
          </div>

          <div className="space-y-6">
            <Card className="pt-0 gap-0">
              <CardHeader className="border-b py-3! gap-0! grid-rows-none! rounded-t-xl bg-muted/30 backdrop-blur-sm">
                <CardTitle className="text-sm font-medium">
                  Runs over time
                </CardTitle>
              </CardHeader>
              <CardContent className="pt-4 px-0">
                {stats.timeline.length > 0 ? (
                  <ChartContainer
                    config={chartConfig}
                    className="aspect-auto h-[300px] w-full"
                  >
                    <AreaChart
                      data={stats.timeline}
                      margin={{ left: 0, right: 0 }}
                    >
                      <defs>
                        {Object.entries(chartConfig).map(([key, { color }]) => (
                          <linearGradient
                            key={key}
                            id={`gradient-${key}`}
                            x1="0"
                            y1="0"
                            x2="0"
                            y2="1"
                          >
                            <stop
                              offset="0%"
                              stopColor={color}
                              stopOpacity={0.6}
                            />
                            <stop
                              offset="100%"
                              stopColor={color}
                              stopOpacity={0.02}
                            />
                          </linearGradient>
                        ))}
                      </defs>
                      <XAxis
                        dataKey="timestamp"
                        tickLine={false}
                        axisLine={false}
                        tick={({ x, y, index, visibleTicksCount, payload }) => {
                          const anchor =
                            index === 0
                              ? "start"
                              : index === visibleTicksCount - 1
                                ? "end"
                                : "middle";
                          return (
                            <text
                              x={x}
                              y={y + 12}
                              textAnchor={anchor}
                              className="fill-muted-foreground text-xs"
                            >
                              {formatBucketLabel(payload.value, period)}
                            </text>
                          );
                        }}
                      />
                      <CartesianGrid
                        horizontal
                        vertical={false}
                        strokeDasharray="3 3"
                        className="stroke-border/50"
                      />
                      <YAxis
                        allowDecimals={false}
                        tickLine={false}
                        axisLine={false}
                        mirror
                        width={1}
                      />
                      <ChartTooltip
                        content={<ChartTooltipContent />}
                        labelFormatter={(v) => formatBucketLabel(v, period)}
                      />
                      <ChartLegend
                        content={<ChartLegendContent className="flex-wrap" />}
                      />
                      <Area
                        type="monotone"
                        dataKey="pending"
                        stackId="1"
                        stroke="var(--color-pending)"
                        fill="url(#gradient-pending)"
                      />
                      <Area
                        type="monotone"
                        dataKey="running"
                        stackId="1"
                        stroke="var(--color-running)"
                        fill="url(#gradient-running)"
                      />
                      <Area
                        type="monotone"
                        dataKey="completed"
                        stackId="1"
                        stroke="var(--color-completed)"
                        fill="url(#gradient-completed)"
                      />
                      <Area
                        type="monotone"
                        dataKey="retrying"
                        stackId="1"
                        stroke="var(--color-retrying)"
                        fill="url(#gradient-retrying)"
                      />
                      <Area
                        type="monotone"
                        dataKey="cancelled"
                        stackId="1"
                        stroke="var(--color-cancelled)"
                        fill="url(#gradient-cancelled)"
                      />
                      <Area
                        type="monotone"
                        dataKey="deadLetter"
                        stackId="1"
                        stroke="var(--color-deadLetter)"
                        fill="url(#gradient-deadLetter)"
                      />
                    </AreaChart>
                  </ChartContainer>
                ) : (
                  <p className="text-sm text-muted-foreground">
                    No data for this period
                  </p>
                )}
              </CardContent>
            </Card>

            <Card className="pt-0 gap-0 pb-0 bg-transparent shadow-none">
              <CardHeader className="py-3! gap-0! grid-rows-none! rounded-t-xl bg-muted/30 backdrop-blur-sm">
                <CardTitle className="text-sm font-medium">
                  Recent runs
                </CardTitle>
              </CardHeader>
              <CardContent className="px-0">
                {stats.recentRuns.length > 0 ? (
                  <div>
                    {stats.recentRuns.slice(0, 15).map((run, i, rows) => (
                      <Link
                        key={run.id}
                        to={`/runs/${run.id}`}
                        className={`flex items-center justify-between gap-4 px-6 py-2.5 border-t hover:bg-muted/50 transition-colors text-sm${i === rows.length - 1 ? " rounded-b-xl" : ""}`}
                      >
                        <span
                          className="font-medium truncate"
                          title={run.jobName}
                        >
                          {run.jobName}
                        </span>
                        <div className="flex items-center gap-3 shrink-0">
                          <StatusBadge status={run.status} />
                          <span className="text-xs text-muted-foreground tabular-nums w-14 text-right">
                            {formatRelative(run.createdAt)}
                          </span>
                        </div>
                      </Link>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground px-6 py-4">
                    No runs yet
                  </p>
                )}
              </CardContent>
            </Card>
          </div>
        </>
      )}
    </div>
  );
}

function StatCard({
  title,
  value,
  icon: Icon,
}: {
  title: string;
  value: string | number;
  icon: LucideIcon;
}) {
  return (
    <Card className="gap-3 py-5">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-sm font-medium text-muted-foreground">
          <Icon className="size-4" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-semibold tabular-nums">{value}</div>
      </CardContent>
    </Card>
  );
}
