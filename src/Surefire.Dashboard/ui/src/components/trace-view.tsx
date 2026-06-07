import {
  type RefObject,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useNavigate } from "react-router";
import { useVirtualizer } from "@tanstack/react-virtual";
import { type JobRun, JobStatus } from "@/lib/api";
import { formatMs } from "@/lib/format";

const statusColorVar: Record<number, string> = {
  [JobStatus.Pending]: "var(--status-pending)",
  [JobStatus.Running]: "var(--status-running)",
  [JobStatus.Succeeded]: "var(--status-succeeded)",
  [JobStatus.Suspended]: "var(--status-suspended)",
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

export const TRACE_ROW_HEIGHT = 36;
const ROW_HEIGHT = TRACE_ROW_HEIGHT;

// Canvas-based text measurement. Single shared canvas, font set per call.
// Used to size the trace name column to fit the widest indented name exactly.
let measureCanvas: HTMLCanvasElement | null = null;
let measureCtx: CanvasRenderingContext2D | null = null;

function measureNameWidth(name: string, weight: number): number {
  if (typeof document === "undefined") return name.length * 8;
  if (!measureCanvas) {
    measureCanvas = document.createElement("canvas");
    measureCtx = measureCanvas.getContext("2d");
  }
  if (!measureCtx) return name.length * 8;
  measureCtx.font = `${weight} 14px "Geist Sans", ui-sans-serif, system-ui, sans-serif`;
  return measureCtx.measureText(name).width;
}

export type TraceItem = { kind: "run" } & JobRun;

export type TraceScrollState = {
  rootId: string | null;
  scrollTop: number;
};

export function TraceView({
  items,
  currentRunId,
  rootId,
  scrollContainerRef,
  scrollStateRef,
  headerSticky = true,
  manageFocusScroll = true,
  header,
  onHeaderClick,
  headerClassName,
}: {
  items: TraceItem[];
  currentRunId: string;
  /** Stable identity for the trace. Used to decide whether navigation stays
   *  inside the same trace (preserve scroll) or moves to a different trace
   *  (scroll the focus row into view). Comes from the server's RunTreeResponse
   *  since items[0] isn't stable across truncation. */
  rootId?: string;
  scrollContainerRef: RefObject<HTMLDivElement | null>;
  scrollStateRef?: { current: TraceScrollState };
  /** When false, the timeline-tick header flows inline instead of pinning to
   *  the top of the scroll container. Use false when the trace shares a scroll
   *  container with other content that has its own sticky headers. */
  headerSticky?: boolean;
  /** When false, the trace will never adjust the scroll container itself. Use
   *  false when the trace shares its scroll container with the rest of the page
   *  (mobile single-scroll layout), so navigation doesn't yank the whole page. */
  manageFocusScroll?: boolean;
  header?: React.ReactNode;
  /** When provided, the entire header bar (title cell + time ticks) becomes a
   *  clickable region, used to collapse/expand the trace section. */
  onHeaderClick?: () => void;
  headerClassName?: string;
}) {
  const navigate = useNavigate();
  const hasActiveRuns = useMemo(
    () =>
      items.some(
        (run) =>
          run.status === JobStatus.Pending
          || run.status === JobStatus.Running
          || run.status === JobStatus.Suspended,
      ),
    [items],
  );
  const [nowMs, setNowMs] = useState(() => Date.now());

  useEffect(() => {
    if (!hasActiveRuns) return;
    const timer = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [hasActiveRuns]);

  const { timeStart, timeRange, ticks } = useMemo(() => {
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
  });

  const focusIdx = useMemo(
    () => items.findIndex((item) => item.id === currentRunId),
    [items, currentRunId],
  );

  // Scroll the focus row into view only when the trace itself changes. Intra-trace
  // navigation leaves scroll alone so the user keeps their place while clicking
  // around siblings/children.
  const lastRootIdRef = useRef<string | null>(null);

  useEffect(() => {
    if (!manageFocusScroll) return;
    if (focusIdx < 0) return;
    if (!rootId) return;
    if (lastRootIdRef.current === rootId) return;

    const savedRootId = scrollStateRef?.current.rootId ?? null;
    const savedScrollTop = scrollStateRef?.current.scrollTop ?? 0;

    const rafId = requestAnimationFrame(() => {
      const scrollEl = scrollContainerRef.current;
      if (!scrollEl) return;
      if (scrollEl.clientHeight === 0) return;

      if (savedRootId === rootId) {
        scrollEl.scrollTop = savedScrollTop;
      } else {
        const HEADER_OFFSET = 48;
        const availableHeight = Math.max(
          scrollEl.clientHeight - HEADER_OFFSET,
          ROW_HEIGHT,
        );
        const focusCenterInScroll =
          rowsScrollMargin + focusIdx * ROW_HEIGHT + ROW_HEIGHT / 2;
        scrollEl.scrollTop = Math.max(
          0,
          focusCenterInScroll - HEADER_OFFSET - availableHeight / 2,
        );
      }

      lastRootIdRef.current = rootId;
      if (scrollStateRef) {
        scrollStateRef.current = { rootId, scrollTop: scrollEl.scrollTop };
      }
    });

    return () => cancelAnimationFrame(rafId);
  }, [
    focusIdx,
    rootId,
    manageFocusScroll,
    rowsScrollMargin,
    scrollContainerRef,
    scrollStateRef,
  ]);

  useEffect(() => {
    if (!manageFocusScroll) return;
    if (!rootId) return;
    if (!scrollStateRef) return;
    const scrollEl = scrollContainerRef.current;
    if (!scrollEl) return;

    const saveScroll = () => {
      scrollStateRef.current = { rootId, scrollTop: scrollEl.scrollTop };
    };

    scrollEl.addEventListener("scroll", saveScroll, { passive: true });
    return () => {
      saveScroll();
      scrollEl.removeEventListener("scroll", saveScroll);
    };
  }, [rootId, manageFocusScroll, scrollContainerRef, scrollStateRef]);

  // Size the name column to exactly fit the widest (depth-indented) job name.
  // Always measured at weight 600 (the bolder current-run weight) so the column
  // stays stable when the current run changes within the same trace, at the
  // cost of a few px of over-estimation for non-current rows. Capped at a
  // ceiling so an absurd outlier name doesn't blow out the timeline area.
  const nameColPx = useMemo(() => {
    const BASE_PX = 24;     // pl-6 base padding
    const INDENT_PX = 20;   // per-depth indent (1.25rem)
    const RIGHT_PX = 12;    // pr-3 trailing padding
    const EXTRA_PX = 16;    // breathing room between name and bar column
    const MAX_PX = 448;     // 28rem ceiling

    let maxPx = 0;
    for (const item of items) {
      const depth = item.depth ?? 0;
      const nameWidth = measureNameWidth(item.jobName, 600);
      const total = BASE_PX + depth * INDENT_PX + nameWidth + RIGHT_PX + EXTRA_PX;
      if (total > maxPx) maxPx = total;
    }
    return Math.min(maxPx, MAX_PX);
  }, [items]);

  if (items.length === 0) return null;

  const pct = (ms: number) => (ms / timeRange) * 100 * SCALE;

  const traceHeaderClassName = headerClassName ?? "bg-background";

  return (
    <div style={{ ["--trace-name-col" as string]: `${nameColPx}px` }}>
      <div
        role={onHeaderClick ? "button" : undefined}
        tabIndex={onHeaderClick ? 0 : undefined}
        aria-expanded={onHeaderClick ? true : undefined}
        onClick={onHeaderClick}
        onKeyDown={
          onHeaderClick
            ? (e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onHeaderClick();
                }
              }
            : undefined
        }
        className={`${headerSticky ? "sticky top-0 z-10 " : ""}border-b border-border ${traceHeaderClassName}${onHeaderClick ? " cursor-pointer hover:bg-accent/30 transition-colors" : ""}`}
      >
        <div
          className="grid h-[2.625rem]"
          style={{
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
                style={{ left: `${pct(t)}%` }}
              >
                {formatMs(t)}
              </span>
            ))}
          </div>
        </div>
      </div>

      <div
        ref={rowsContainerRef}
        className="relative w-full"
        style={{ height: `${rowVirtualizer.getTotalSize()}px` }}
      >
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
            run.replayCount > 0 ? `(${run.replayCount} replay${run.replayCount === 1 ? "" : "s"})` : null,
          ]
            .filter(Boolean)
            .join(" ");

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
                isCurrent ? "bg-accent-brand-soft/40" : "hover:bg-accent/40"
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
                  className="absolute left-0 top-0 bottom-0 w-0.5 bg-accent-brand"
                />
              )}
              <div
                className="flex items-center min-w-0 pr-3"
                style={{ paddingLeft: `calc(1.5rem + ${depth * 1.25}rem)` }}
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
                    style={{ left: `${pct(t)}%` }}
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
                    style={{ left: `calc(${leftPct + widthPct}% + 8px)` }}
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
