import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { StatusBadge } from '@/components/status-badge';
import { formatRelative } from '@/lib/format';
import { Link } from 'react-router';
import { AreaChart, Area, XAxis, YAxis } from 'recharts';
import { type ChartConfig, ChartContainer, ChartTooltip, ChartTooltipContent, ChartLegend, ChartLegendContent } from '@/components/ui/chart';
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { CircleAlert } from 'lucide-react';
import { Spinner } from '@/components/ui/spinner';

const chartConfig = {
  completed: { label: 'Completed', color: 'var(--status-completed)' },
  failed: { label: 'Failed', color: 'var(--status-failed)' },
  pending: { label: 'Pending', color: 'var(--status-pending)' },
  running: { label: 'Running', color: 'var(--status-running)' },
  cancelled: { label: 'Cancelled', color: 'var(--status-cancelled)' },
  deadLetter: { label: 'Dead Letter', color: 'var(--status-dead-letter)' },
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
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold tracking-tight">Dashboard</h2>
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
      {!stats && !isError && <div className="flex items-center gap-2 text-muted-foreground"><Spinner className="size-4" />Loading...</div>}

      {stats && <>
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <StatCard title="Total jobs" value={stats.totalJobs} />
        <StatCard title="Total runs" value={stats.totalRuns} />
        <StatCard title="Active runs" value={stats.activeRuns} />
        <StatCard title="Success rate" value={`${stats.successRate.toFixed(1)}%`} />
      </div>

      <div className="space-y-4">
      <Card>
        <CardHeader><CardTitle>Runs over time</CardTitle></CardHeader>
        <CardContent>
          {stats.timeline.length > 0 ? (
            <ChartContainer config={chartConfig} className="aspect-auto h-[280px] w-full">
              <AreaChart data={stats.timeline} margin={{ left: -20 }}>
                <XAxis
                  dataKey="timestamp"
                  tickFormatter={(v) => formatBucketLabel(v, period)}
                  tickLine={false}
                  axisLine={false}
                />
                <YAxis allowDecimals={false} tickLine={false} axisLine={false} />
                <ChartTooltip
                  content={<ChartTooltipContent />}
                  labelFormatter={(v) => formatBucketLabel(v, period)}
                />
                <ChartLegend content={<ChartLegendContent />} />
                <Area type="monotone" dataKey="completed" stackId="1" stroke="var(--color-completed)" fill="var(--color-completed)" fillOpacity={0.4} />
                <Area type="monotone" dataKey="failed" stackId="1" stroke="var(--color-failed)" fill="var(--color-failed)" fillOpacity={0.4} />
                <Area type="monotone" dataKey="pending" stackId="1" stroke="var(--color-pending)" fill="var(--color-pending)" fillOpacity={0.4} />
                <Area type="monotone" dataKey="running" stackId="1" stroke="var(--color-running)" fill="var(--color-running)" fillOpacity={0.4} />
                <Area type="monotone" dataKey="cancelled" stackId="1" stroke="var(--color-cancelled)" fill="var(--color-cancelled)" fillOpacity={0.4} />
                <Area type="monotone" dataKey="deadLetter" stackId="1" stroke="var(--color-deadLetter)" fill="var(--color-deadLetter)" fillOpacity={0.4} />
              </AreaChart>
            </ChartContainer>
          ) : (
            <p className="text-sm text-muted-foreground">No data for this period</p>
          )}
        </CardContent>
      </Card>

      <Card className="gap-0 pb-0">
        <CardHeader className="pb-3"><CardTitle>Recent runs</CardTitle></CardHeader>
        <CardContent className="px-0">
          {stats.recentRuns.length > 0 ? (
            <div>
              {stats.recentRuns.map((run, i) => (
                <Link
                  key={run.id}
                  to={`/runs/${run.id}`}
                  className={`flex items-center justify-between gap-4 px-6 py-2.5 border-t hover:bg-muted/50 text-sm${i === stats.recentRuns.length - 1 ? ' rounded-b-xl' : ''}`}
                >
                  <span>{run.jobName}</span>
                  <div className="flex items-center gap-3 shrink-0">
                    <StatusBadge status={run.status} />
                    <span className="text-xs text-muted-foreground tabular-nums w-14 text-right">{formatRelative(run.createdAt)}</span>
                  </div>
                </Link>
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground px-6 pb-4">No runs yet</p>
          )}
        </CardContent>
      </Card>
      </div>
      </>}
    </div>
  );
}

function StatCard({ title, value }: { title: string; value: string | number }) {
  return (
    <Card className="gap-2 py-4">
      <CardHeader><CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle></CardHeader>
      <CardContent><div className="text-2xl font-bold">{value}</div></CardContent>
    </Card>
  );
}
