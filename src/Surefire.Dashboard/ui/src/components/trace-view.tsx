import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router";
import type { JobRun } from "@/lib/api";
import { formatMs } from "@/lib/format";

interface TreeNode {
  run: JobRun;
  children: TreeNode[];
  depth: number;
}

const barColors: Record<number, string> = {
  0: "bg-amber-400 dark:bg-amber-500",
  1: "bg-sky-400 dark:bg-sky-500",
  2: "bg-emerald-400 dark:bg-emerald-500",
  3: "bg-rose-400 dark:bg-rose-500",
  4: "bg-slate-400 dark:bg-slate-500",
  5: "bg-violet-400 dark:bg-violet-500",
  6: "bg-gray-400 dark:bg-gray-500",
};

function buildTree(runs: JobRun[]): TreeNode[] {
  const byId = new Map<string, TreeNode>();
  for (const run of runs) {
    byId.set(run.id, { run, children: [], depth: 0 });
  }

  const roots: TreeNode[] = [];
  for (const node of byId.values()) {
    if (node.run.parentRunId && byId.has(node.run.parentRunId)) {
      byId.get(node.run.parentRunId)!.children.push(node);
    } else {
      roots.push(node);
    }
  }

  for (const node of byId.values()) {
    node.children.sort(
      (a, b) =>
        new Date(a.run.createdAt).getTime() -
        new Date(b.run.createdAt).getTime(),
    );
  }

  return roots;
}

function flattenDfs(nodes: TreeNode[], depth = 0): TreeNode[] {
  const result: TreeNode[] = [];
  for (const node of nodes) {
    node.depth = depth;
    result.push(node);
    result.push(...flattenDfs(node.children, depth + 1));
  }
  return result;
}

function computeTicks(rangeMs: number): number[] {
  if (rangeMs <= 0) return [0];
  const intervals = [
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000, 60000,
    120000, 300000, 600000, 1800000, 3600000,
  ];
  const target = rangeMs / 5;
  const interval =
    intervals.find((n) => n >= target) ?? Math.ceil(target / 1000) * 1000;
  const ticks: number[] = [0];
  for (let t = interval; t < rangeMs; t += interval) {
    ticks.push(t);
  }
  return ticks;
}

// Bars and ticks occupy 0–SCALE of the timeline width,
// leaving room for trailing duration labels.
const SCALE = 0.9;

export function TraceView({
  runs,
  currentRunId,
}: {
  runs: JobRun[];
  currentRunId: string;
}) {
  const hasActiveRuns = useMemo(
    () => runs.some((run) => run.status === 0 || run.status === 1),
    [runs],
  );
  const [nowMs, setNowMs] = useState(() => Date.now());

  useEffect(() => {
    if (!hasActiveRuns) return;
    const timer = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [hasActiveRuns]);

  const { flatNodes, timeStart, timeRange, ticks } = useMemo(() => {
    const roots = buildTree(runs);
    const flat = flattenDfs(roots);

    let earliest = Infinity;
    let latest = -Infinity;

    for (const { run } of flat) {
      const created = new Date(run.createdAt).getTime();
      if (created < earliest) earliest = created;

      const end = run.completedAt
        ? new Date(run.completedAt).getTime()
        : run.startedAt
          ? nowMs
          : created;
      if (end > latest) latest = end;
    }

    const range = Math.max(latest - earliest, 1);
    return {
      flatNodes: flat,
      timeStart: earliest,
      timeRange: range,
      ticks: computeTicks(range),
    };
  }, [runs, nowMs]);

  const currentRef = useRef<HTMLAnchorElement>(null);
  const hasScrolled = useRef(false);

  // Only scroll once on mount or when navigating to a different run
  useEffect(() => {
    hasScrolled.current = false;
  }, [currentRunId]);

  useEffect(() => {
    if (currentRef.current && !hasScrolled.current) {
      currentRef.current.scrollIntoView({ block: "center" });
      hasScrolled.current = true;
    }
  }, [flatNodes]);

  if (flatNodes.length === 0) return null;

  const pct = (ms: number) => (ms / timeRange) * 100 * SCALE;

  return (
    <div className="w-full min-h-0">
      <div className="grid grid-cols-[auto_1fr]">
        {/* Time axis header */}
        <div className="col-span-2 grid grid-cols-subgrid items-stretch sticky top-10 z-10 h-10 border-b bg-muted/30 backdrop-blur-sm px-2">
          <span className="pr-3" />
          <div className="relative overflow-visible">
            {ticks.map((t, i) => (
              <span
                key={i}
                className="absolute top-1/2 text-[11px] text-muted-foreground/60 tabular-nums -translate-x-1/2 -translate-y-1/2"
                style={{ left: `${pct(t)}%` }}
              >
                {formatMs(t)}
              </span>
            ))}
          </div>
        </div>

        {/* Trace rows */}
        {flatNodes.map(({ run, depth }) => {
          const created = new Date(run.createdAt).getTime();
          const started = run.startedAt
            ? new Date(run.startedAt).getTime()
            : created;
          const end = run.completedAt
            ? new Date(run.completedAt).getTime()
            : run.startedAt
              ? nowMs
              : created;

          const leftPct = pct(started - timeStart);
          const widthPct = Math.max(
            Math.min(pct(end - started), 100 * SCALE - leftPct),
            0.3,
          );
          const durationMs = end - started;
          const isCurrent = run.id === currentRunId;

          return (
            <Link
              key={run.id}
              ref={isCurrent ? currentRef : undefined}
              to={`/runs/${run.id}`}
              className={`col-span-2 grid grid-cols-subgrid items-center transition-colors border-b border-border/20 last:border-b-0 ${
                isCurrent
                  ? "bg-primary/5 hover:bg-primary/10"
                  : "hover:bg-muted/50"
              }`}
            >
              {/* Name */}
              <div
                className="flex items-center gap-1.5 min-w-0 max-w-[200px] pr-3 py-1.5"
                style={{
                  paddingLeft: `calc(0.75rem + ${depth * 0.875}rem)`,
                }}
              >
                <span
                  className={`size-1.5 rounded-full shrink-0 ${isCurrent ? "bg-primary" : ""}`}
                />
                <span className="text-[13px] truncate" title={run.jobName}>
                  {run.jobName}
                </span>
              </div>

              {/* Timeline */}
              <div className="relative h-7 border-l border-border/40">
                {/* Gridlines (skip first — border-l covers position 0) */}
                {ticks.slice(1).map((t, i) => (
                  <div
                    key={i}
                    className="absolute top-0 bottom-0 w-px bg-border/10"
                    style={{ left: `${pct(t)}%` }}
                  />
                ))}
                {/* Bar */}
                <div
                  className={`absolute top-1/2 -translate-y-1/2 h-[10px] rounded-sm ${
                    barColors[run.status] ?? "bg-slate-400"
                  } ${isCurrent ? "ring-1 ring-primary" : ""}`}
                  style={{
                    left: `${leftPct}%`,
                    width: `${widthPct}%`,
                    minWidth: "3px",
                  }}
                />
                {/* Duration trailing the bar */}
                {durationMs > 0 && (
                  <span
                    className="absolute top-1/2 -translate-y-1/2 text-[11px] text-muted-foreground/70 tabular-nums whitespace-nowrap"
                    style={{ left: `calc(${leftPct + widthPct}% + 6px)` }}
                  >
                    {formatMs(durationMs)}
                  </span>
                )}
              </div>
            </Link>
          );
        })}
      </div>
    </div>
  );
}
