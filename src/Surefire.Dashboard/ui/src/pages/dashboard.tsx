import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { StatusBadge } from '@/components/status-badge';
import { formatRelative } from '@/lib/format';
import { Link } from 'react-router';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid } from 'recharts';
import { type ChartConfig, ChartContainer, ChartTooltip, ChartTooltipContent, ChartLegend, ChartLegendContent } from '@/components/ui/chart';
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { CircleAlert, Workflow, Play, Activity, TrendingUp, type LucideIcon } from 'lucide-react';

const chartConfig = {
  completed: { label: 'Completed', color: 'var(--status-completed)' },
  failed: { label: 'Failed', color: 'var(--status-failed)' },
  pending: { label: 'Pending', color: 'var(--status-pending)' },
  running: { label: 'Running', color: 'var(--status-running)' },
  cancelled: { label: 'Cancelled', color: 'var(--status-cancelled)' },
  deadLetter: { label: 'Dead letter', color: 'var(--status-dead-letter)' },
} satisfies ChartConfig;

const PERIODS: Record<string, { hours: number; bucketMinutes: number }> = {
  '1h': { hours: 1, bucketMinutes: 5 },
  '24h': { hours: 24, bucketMinutes: 60 },
  '7d': { hours: 168, bucketMinutes: 1440 },
  '30d': { hours: 720, bucketMinutes: 1440 },
};

function formatBucketLabel(timestamp: string, period: string) {
  const d = new Date(timestamp);
  if (period === '1h' || period === '24h') {
    return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
  }
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

export function DashboardPage() {
  const [period, setPeriod] = useState('1h');
  const { data: stats, isError } = useQuery({
    queryKey: ['stats', period],
    queryFn: () => {
      const p = PERIODS[period];
      const since = new Date(Date.now() - p.hours * 3600_000).toISOString();
      return api.getStats({ since, bucketMinutes: p.bucketMinutes });
    },
    refetchInterval: 5000,
  });

  return (
    <div className="relative space-y-6">
      <div className="absolute left-1/2 -translate-x-1/2 w-screen -top-8 h-[400px] pointer-events-none" aria-hidden="true">
        <div className="absolute inset-0 animate-glow-drift will-change-transform" style={{ background: 'radial-gradient(ellipse 60% 60% at 30% 20%, oklch(from var(--primary) l c h / 0.06), transparent)' }} />
        <div className="absolute inset-0 animate-glow-drift will-change-transform [animation-delay:-5s] [animation-direction:reverse]" style={{ background: 'radial-gradient(ellipse 50% 55% at 70% 30%, oklch(from var(--ring) l c h / 0.04), transparent)' }} />
      </div>
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

      {isError && <Alert variant="destructive"><CircleAlert /><AlertDescription>Failed to load dashboard</AlertDescription></Alert>}

      {!stats && !isError && (
        <>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <Card key={i} className="gap-2 py-5">
                <CardHeader><Skeleton className="h-4 w-24" /></CardHeader>
                <CardContent><Skeleton className="h-7 w-16" /></CardContent>
              </Card>
            ))}
          </div>
          <Card>
            <CardHeader><Skeleton className="h-5 w-32" /></CardHeader>
            <CardContent><Skeleton className="h-[300px] w-full" /></CardContent>
          </Card>
          <Card className="gap-0 pb-0">
            <CardHeader className="pb-3"><Skeleton className="h-5 w-28" /></CardHeader>
            <CardContent className="px-0">
              {Array.from({ length: 5 }).map((_, i) => (
                <div key={i} className="flex items-center justify-between px-6 py-3 border-t">
                  <Skeleton className="h-4 w-32" />
                  <Skeleton className="h-5 w-20" />
                </div>
              ))}
            </CardContent>
          </Card>
        </>
      )}

      {stats && <>
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <StatCard title="Total jobs" value={stats.totalJobs} icon={Workflow} />
        <StatCard title="Total runs" value={stats.totalRuns} icon={Play} />
        <StatCard title="Active runs" value={stats.activeRuns} icon={Activity} />
        <StatCard title="Success rate" value={`${(stats.successRate ?? 0).toFixed(1)}%`} icon={TrendingUp} />
      </div>

      <div className="space-y-6">
      <Card className="pt-0 gap-0">
        <CardHeader className="border-b py-3! gap-0! grid-rows-none! rounded-t-xl bg-background/80 backdrop-blur-sm"><CardTitle className="text-sm font-medium">Runs over time</CardTitle></CardHeader>
        <CardContent className="pt-4 px-0">
          {stats.timeline.length > 0 ? (
            <ChartContainer config={chartConfig} className="aspect-auto h-[300px] w-full">
              <AreaChart data={stats.timeline} margin={{ left: 0, right: 0 }}>
                <XAxis
                  dataKey="timestamp"
                  tickLine={false}
                  axisLine={false}
                  tick={({ x, y, index, visibleTicksCount, payload }) => {
                    const anchor = index === 0 ? 'start' : index === visibleTicksCount - 1 ? 'end' : 'middle';
                    return (
                      <text x={x} y={y + 12} textAnchor={anchor} className="fill-muted-foreground text-xs">
                        {formatBucketLabel(payload.value, period)}
                      </text>
                    );
                  }}
                />
                <CartesianGrid horizontal vertical={false} strokeDasharray="3 3" className="stroke-border/50" />
                <YAxis allowDecimals={false} tickLine={false} axisLine={false} mirror width={1} />
                <ChartTooltip
                  content={<ChartTooltipContent />}
                  labelFormatter={(v) => formatBucketLabel(v, period)}
                />
                <ChartLegend content={<ChartLegendContent className="flex-wrap" />} />
                <Area type="monotone" dataKey="completed" stackId="1" stroke="var(--color-completed)" fill="var(--color-completed)" fillOpacity={0.25} />
                <Area type="monotone" dataKey="failed" stackId="1" stroke="var(--color-failed)" fill="var(--color-failed)" fillOpacity={0.25} />
                <Area type="monotone" dataKey="pending" stackId="1" stroke="var(--color-pending)" fill="var(--color-pending)" fillOpacity={0.25} />
                <Area type="monotone" dataKey="running" stackId="1" stroke="var(--color-running)" fill="var(--color-running)" fillOpacity={0.25} />
                <Area type="monotone" dataKey="cancelled" stackId="1" stroke="var(--color-cancelled)" fill="var(--color-cancelled)" fillOpacity={0.25} />
                <Area type="monotone" dataKey="deadLetter" stackId="1" stroke="var(--color-deadLetter)" fill="var(--color-deadLetter)" fillOpacity={0.25} />
              </AreaChart>
            </ChartContainer>
          ) : (
            <p className="text-sm text-muted-foreground">No data for this period</p>
          )}
        </CardContent>
      </Card>

      <Card className="pt-0 gap-0 pb-0">
        <CardHeader className="py-3! gap-0! grid-rows-none! rounded-t-xl bg-background/80 backdrop-blur-sm"><CardTitle className="text-sm font-medium">Recent runs</CardTitle></CardHeader>
        <CardContent className="px-0">
          {stats.recentRuns.length > 0 ? (
            <div>
              {stats.recentRuns.map((run, i) => (
                <Link
                  key={run.id}
                  to={`/runs/${run.id}`}
                  className={`flex items-center justify-between gap-4 px-6 py-2.5 border-t hover:bg-muted/50 transition-colors text-sm${i === stats.recentRuns.length - 1 ? ' rounded-b-xl' : ''}`}
                >
                  <span className="font-medium truncate" title={run.jobName}>{run.jobName}</span>
                  <div className="flex items-center gap-3 shrink-0">
                    <StatusBadge status={run.status} />
                    <span className="text-xs text-muted-foreground tabular-nums w-14 text-right">{formatRelative(run.createdAt)}</span>
                  </div>
                </Link>
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground px-6 py-4">No runs yet</p>
          )}
        </CardContent>
      </Card>
      </div>
      </>}
    </div>
  );
}

function StatCard({ title, value, icon: Icon }: { title: string; value: string | number; icon: LucideIcon }) {
  return (
    <Card className="gap-3 py-5 border-t-2 border-t-primary/10">
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-sm font-medium text-muted-foreground">
          <Icon className="size-4" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent><div className="text-2xl font-semibold tabular-nums">{value}</div></CardContent>
    </Card>
  );
}
