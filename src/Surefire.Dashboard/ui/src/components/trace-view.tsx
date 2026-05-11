import {
  type RefObject,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {useNavigate} from "react-router";
import {useVirtualizer} from "@tanstack/react-virtual";
import {type JobRun, JobStatus} from "@/lib/api";
import {formatMs} from "@/lib/format";

const statusColorVar: Record<number, string> = {
  [JobStatus.Pending]: "var(--status-pending)",
  [JobStatus.Running]: "var(--status-running)",
  [JobStatus.Succeeded]: "var(--status-succeeded)",
  [JobStatus.Canceled]: "var(--status-canceled)",
  [JobStatus.Failed]: "var(--status-failed)",
};

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

// Bars and ticks occupy 0 to SCALE of the timeline width, leaving 10% for trailing
// duration labels.
const SCALE = 0.9;

export const TRACE_ROW_HEIGHT = 32;
const ROW_HEIGHT = TRACE_ROW_HEIGHT;

export type TraceItem = { kind: "run" } & JobRun;

export function TraceView({
  items,
  currentRunId,
  scrollContainerRef,
  headerSticky = true,
  header,
  onVisibleRunIdsChange,
}: {
  items: TraceItem[];
  currentRunId: string;
  scrollContainerRef: RefObject<HTMLDivElement | null>;
  /** When false, the timeline-tick header flows inline instead of pinning to
   *  the top of the scroll container. Use false when the trace shares a scroll
   *  container with other content that has its own sticky headers. */
  headerSticky?: boolean;
  header?: React.ReactNode;
  onVisibleRunIdsChange?: (runIds: string[]) => void;
}) {
  const navigate = useNavigate();
  const hasActiveRuns = useMemo(
    () =>
      items.some(
        (run) =>
          run.status === JobStatus.Pending || run.status === JobStatus.Running,
      ),
    [items],
  );
  const [nowMs, setNowMs] = useState(() => Date.now());

  useEffect(() => {
    if (!hasActiveRuns) return;
    const timer = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [hasActiveRuns]);

  const {timeStart, timeRange, ticks} = useMemo(() => {
    let earliest = Infinity;
    let latest = -Infinity;
    for (const run of items) {
      const created = new Date(run.createdAt).getTime();
      if (created < earliest) earliest = created;
      const end = run.completedAt
        ? new Date(run.completedAt).getTime()
        : run.startedAt
          ? nowMs
          : created;
      if (end > latest) latest = end;
    }
    if (!Number.isFinite(earliest)) earliest = nowMs;
    if (!Number.isFinite(latest)) latest = earliest;
    const range = Math.max(latest - earliest, 1);
    return {
      timeStart: earliest,
      timeRange: range,
      ticks: computeTicks(range),
    };
  }, [items, nowMs]);

  const reportVisibleRunIds = useCallback(
    (indices: number[]) => {
      if (!onVisibleRunIdsChange) return;
      onVisibleRunIdsChange(
        indices
          .map((index) => items[index]?.id)
          .filter((id): id is string => Boolean(id)),
      );
    },
    [items, onVisibleRunIdsChange],
  );

  // Virtualizer needs scrollMargin = rows-container offset within the scroll container.
  // Otherwise it renders the wrong index range when the trace shares scroll with other
  // content above it (mobile single-scroll layout, sticky desktop header).
  const rowsContainerRef = useRef<HTMLDivElement>(null);
  const [rowsScrollMargin, setRowsScrollMargin] = useState(0);
  const updateRowsScrollMargin = useCallback(() => {
    const scrollEl = scrollContainerRef.current;
    const rowsEl = rowsContainerRef.current;
    if (!scrollEl || !rowsEl) return;
    const scrollRect = scrollEl.getBoundingClientRect();
    const rowsRect = rowsEl.getBoundingClientRect();
    const offset = rowsRect.top - scrollRect.top + scrollEl.scrollTop;
    setRowsScrollMargin((prev) => (prev === offset ? prev : offset));
  }, [scrollContainerRef]);
  useLayoutEffect(() => {
    updateRowsScrollMargin();
  });

  // eslint-disable-next-line react-hooks/incompatible-library -- useVirtualizer manages its own state; React Compiler memoization is unnecessary.
  const rowVirtualizer = useVirtualizer({
    count: items.length,
    getScrollElement: () => scrollContainerRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
    scrollMargin: rowsScrollMargin,
    onChange: (instance) => {
      reportVisibleRunIds(
        instance.getVirtualItems().map((item) => item.index),
      );
    },
  });

  useEffect(() => {
    reportVisibleRunIds(
      rowVirtualizer.getVirtualItems().map((item) => item.index),
    );
  }, [items, reportVisibleRunIds, rowVirtualizer]);

  const focusIdx = useMemo(
    () => items.findIndex((item) => item.id === currentRunId),
    [items, currentRunId],
  );

  // scrollIntoView would walk ancestors and disturb the page's main scroll, so center
  // via an invisible sentinel + manual scrollTop. rAF lets virtualizer measurements
  // settle first. Centers once per currentRunId.
  const hasCenteredRef = useRef(false);
  const focusSentinelRef = useRef<HTMLDivElement>(null);
  const lastCenteredRunIdRef = useRef<string>("");

  useEffect(() => {
    const el = scrollContainerRef.current;
    if (!el) return;

    if (lastCenteredRunIdRef.current !== currentRunId) {
      hasCenteredRef.current = false;
      lastCenteredRunIdRef.current = currentRunId;
    }

    if (hasCenteredRef.current) return;
    if (focusIdx < 0) return;

    const rafId = requestAnimationFrame(() => {
      const sentinel = focusSentinelRef.current;
      const scrollEl = scrollContainerRef.current;
      if (!sentinel || !scrollEl) return;
      if (scrollEl.clientHeight === 0) return;

      const sentinelRect = sentinel.getBoundingClientRect();
      const containerRect = scrollEl.getBoundingClientRect();
      const sentinelTopInScroll =
        sentinelRect.top - containerRect.top + scrollEl.scrollTop;
      const sentinelCenter = sentinelTopInScroll + ROW_HEIGHT / 2;

      // Sticky header (h-10 = 40px) plus a small buffer so the focus row
      // doesn't kiss the bottom edge of the header.
      const HEADER_OFFSET = 48;
      const viewportCenter =
        HEADER_OFFSET + (scrollEl.clientHeight - HEADER_OFFSET) / 2;
      const target = sentinelCenter - viewportCenter;

      scrollEl.scrollTop = Math.max(0, target);
      hasCenteredRef.current = true;
    });

    return () => cancelAnimationFrame(rafId);
  }, [focusIdx, currentRunId, scrollContainerRef]);

  if (items.length === 0) return null;

  const pct = (ms: number) => (ms / timeRange) * 100 * SCALE;

  return (
    <div className="[--trace-name-col:13rem]">
      <div
        className={`${headerSticky ? "sticky top-0 z-10 " : ""}h-10 border-b border-border bg-card/95 backdrop-blur-sm`}
        style={{
          display: "grid",
          gridTemplateColumns: "var(--trace-name-col) 1fr 1.5rem",
        }}
      >
        <div className="flex items-center gap-2 pl-6 pr-3 text-xs text-muted-foreground">
          {header}
        </div>
        <div className="relative h-full overflow-hidden">
          {ticks.map((t, i) => (
            <span
              key={i}
              className="absolute top-1/2 -translate-y-1/2 font-mono text-[11px] text-muted-foreground/70 tnum"
              style={{left: `${pct(t)}%`}}
            >
                {formatMs(t)}
              </span>
          ))}
        </div>
      </div>

      <div
        ref={rowsContainerRef}
        className="relative w-full"
        style={{height: `${rowVirtualizer.getTotalSize()}px`}}
      >
        {focusIdx >= 0 && (
          <div
            ref={focusSentinelRef}
            aria-hidden="true"
            className="pointer-events-none invisible absolute left-0 w-px"
            style={{
              top: `${focusIdx * ROW_HEIGHT}px`,
              height: `${ROW_HEIGHT}px`,
            }}
          />
        )}
        {rowVirtualizer.getVirtualItems().map((virtualItem) => {
          const run = items[virtualItem.index];
          const rowStyle = {
            height: `${virtualItem.size}px`,
            transform: `translateY(${virtualItem.start - rowsScrollMargin}px)`,
          };
          const depth = run.depth ?? 0;
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
          const barColor =
            statusColorVar[run.status] ?? "var(--muted-foreground)";

          const tooltipParts = [
            run.jobName,
            durationMs > 0 ? `· ${formatMs(durationMs)}` : null,
            run.nodeName ? `on ${run.nodeName}` : null,
            `attempt ${run.attempt}`,
          ].filter(Boolean).join(" ");

          const goToRun = () => navigate(`/runs/${run.id}`);

          return (
            <div
              key={run.id}
              data-run-id={run.id}
              role="link"
              tabIndex={0}
              aria-current={isCurrent ? "page" : undefined}
              title={tooltipParts}
              onClick={goToRun}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  goToRun();
                }
              }}
              className={`group/trace-row absolute top-0 left-0 w-full items-stretch transition-colors border-b border-border/40 cursor-pointer focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent-brand/40 ${
                isCurrent
                  ? "bg-accent-brand-soft/40"
                  : "hover:bg-accent/40"
              }`}
              style={{
                display: "grid",
                gridTemplateColumns: "var(--trace-name-col) 1fr 1.5rem",
                ...rowStyle,
              }}
            >
              {isCurrent && (
                <span
                  aria-hidden
                  className="absolute left-0 top-0 bottom-0 w-[2px] bg-accent-brand"
                />
              )}
              <div
                className="flex items-center min-w-0 pr-3"
                style={{paddingLeft: `calc(1.5rem + ${depth * 1.25}rem)`}}
              >
                <span
                  className={`text-sm leading-none truncate transition-colors ${
                    isCurrent
                      ? "font-semibold text-foreground"
                      : "text-foreground/85 group-hover/trace-row:text-accent-brand"
                  }`}
                >
                  {run.jobName}
                </span>
              </div>

              <div className="relative h-full border-l border-border/40 overflow-hidden">
                {ticks.slice(1).map((t, i) => (
                  <div
                    key={i}
                    className="absolute top-0 bottom-0 w-px bg-border/15"
                    style={{left: `${pct(t)}%`}}
                  />
                ))}
                <div
                  className="absolute top-0 bottom-0"
                  style={{
                    left: `${leftPct}%`,
                    width: `${widthPct}%`,
                    minWidth: "3px",
                    backgroundColor: barColor,
                    opacity: 0.78,
                  }}
                />
                {durationMs > 0 && (
                  <span
                    className="absolute top-1/2 -translate-y-1/2 font-mono text-[11px] text-muted-foreground/70 tnum whitespace-nowrap"
                    style={{left: `calc(${leftPct + widthPct}% + 8px)`}}
                  >
                      {formatMs(durationMs)}
                    </span>
                )}
              </div>
            </div>
          );
        })}
      </div>

    </div>
  );
}
