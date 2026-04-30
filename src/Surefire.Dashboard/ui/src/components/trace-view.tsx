import {type RefObject, useEffect, useMemo, useRef, useState,} from "react";
import {Link} from "react-router";
import {useVirtualizer} from "@tanstack/react-virtual";
import {ChevronDown, ChevronRight, ChevronUp} from "lucide-react";
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

export const TRACE_ROW_HEIGHT = 28;
const ROW_HEIGHT = TRACE_ROW_HEIGHT;

export type TraceItem = { kind: "run" } & JobRun;

export function TraceView({
                            items,
                            currentRunId,
                            ancestorIds,
                            expandedNodes,
                            knownEmptyNodes,
                            loadingNodes,
                            onToggle,
                            scrollContainerRef,
                            header,
                            loadEarlierSiblings,
                            loadMoreLaterSiblings,
                            extraSiblingsBeforeCount = 0,
                          }: {
  items: TraceItem[];
  currentRunId: string;
  ancestorIds: Set<string>;
  expandedNodes: Set<string>;
  /** Nodes we've expanded and confirmed to have zero children; hide their caret. */
  knownEmptyNodes: Set<string>;
  /** Nodes with an in-flight children fetch (first page or load-more). */
  loadingNodes: Set<string>;
  /** Toggles a node's expansion; no-op for focus and ancestors. */
  onToggle: (nodeId: string) => void;
  scrollContainerRef: RefObject<HTMLDivElement | null>;
  header?: React.ReactNode;
  /** Count-pill row at top: click to load the next chunk of earlier siblings. */
  loadEarlierSiblings?: { onClick: () => void; isLoading: boolean };
  /** Count-pill row at bottom: click to load the next chunk of later siblings. */
  loadMoreLaterSiblings?: { onClick: () => void; isLoading: boolean };
  /**
   * Number of extra earlier-siblings the parent has loaded so far. An increase
   * signals a user-initiated prepend (load-more-earlier click). At that point
   * we stop trying to auto-center, so the user's scroll stays at the top where
   * the newly-loaded rows are visible.
   */
  extraSiblingsBeforeCount?: number;
}) {
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

  // eslint-disable-next-line react-hooks/incompatible-library -- useVirtualizer manages its own state; React Compiler memoization is unnecessary.
  const rowVirtualizer = useVirtualizer({
    count: items.length,
    getScrollElement: () => scrollContainerRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });

  const focusIdx = useMemo(
    () => items.findIndex((item) => item.id === currentRunId),
    [items, currentRunId],
  );

  // Center via invisible sentinel + scrollTop in useEffect+rAF so virtualizer measurements
  // settle first; scrollIntoView would walk ancestors and disturb the page's main scroll.
  // Centers once per currentRunId; user-initiated prepend shifts scrollTop by inserted rows.
  const hasCenteredRef = useRef(false);
  const prevExtraCountRef = useRef(extraSiblingsBeforeCount);
  const focusSentinelRef = useRef<HTMLDivElement>(null);
  const lastCenteredRunIdRef = useRef<string>("");

  useEffect(() => {
    const prevCount = prevExtraCountRef.current;
    const userPrepended = extraSiblingsBeforeCount > prevCount;
    prevExtraCountRef.current = extraSiblingsBeforeCount;

    const el = scrollContainerRef.current;
    if (!el) return;

    if (lastCenteredRunIdRef.current !== currentRunId) {
      hasCenteredRef.current = false;
      lastCenteredRunIdRef.current = currentRunId;
    }

    if (userPrepended) {
      hasCenteredRef.current = true;
      const delta = extraSiblingsBeforeCount - prevCount;
      el.scrollTop += delta * ROW_HEIGHT;
      return;
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

      // Sticky header (h-2.75rem) hides the top of the scroll viewport.
      const HEADER_OFFSET = 44;
      const viewportCenter =
        HEADER_OFFSET + (scrollEl.clientHeight - HEADER_OFFSET) / 2;
      const target = sentinelCenter - viewportCenter;

      scrollEl.scrollTop = Math.max(0, target);
      hasCenteredRef.current = true;
    });

    return () => cancelAnimationFrame(rafId);
  }, [focusIdx, currentRunId, extraSiblingsBeforeCount, scrollContainerRef]);

  if (items.length === 0) return null;

  const pct = (ms: number) => (ms / timeRange) * 100 * SCALE;

  return (
    // min-w forces horizontal overflow on narrow viewports so the timeline stays usable.
    <div className="min-w-3xl [--trace-name-col:13.75rem]">
      <div
        className="sticky top-0 z-10 py-2.5 border-b bg-muted/40 backdrop-blur-sm px-2"
        style={{
          display: "grid",
          gridTemplateColumns: "var(--trace-name-col) 1fr",
        }}
      >
        <div className="flex items-center text-sm text-muted-foreground pr-3">
          {header}
        </div>
        <div className="relative h-full overflow-hidden">
          {ticks.map((t, i) => (
            <span
              key={i}
              className="absolute top-1/2 -translate-y-1/2 text-[11px] text-muted-foreground/60 tabular-nums"
              style={{left: `${pct(t)}%`}}
            >
                {formatMs(t)}
              </span>
          ))}
        </div>
      </div>

      {loadEarlierSiblings && (
        <SiblingPillRow
          direction="earlier"
          onClick={loadEarlierSiblings.onClick}
          isLoading={loadEarlierSiblings.isLoading}
        />
      )}

      <div
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
            transform: `translateY(${virtualItem.start}px)`,
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

          // No caret on ancestors, focus, or confirmed-leaf nodes.
          const isAncestor = ancestorIds.has(run.id);
          const isFocus = run.id === currentRunId;
          const canToggle =
            !isAncestor && !isFocus && !knownEmptyNodes.has(run.id);
          const isExpanded = expandedNodes.has(run.id);
          const isLoading = loadingNodes.has(run.id);

          return (
            <div
              key={run.id}
              data-run-id={run.id}
              className={`absolute top-0 left-0 w-full items-center transition-colors border-b border-border/50 ${
                isCurrent
                  ? "bg-primary/5 hover:bg-primary/10"
                  : "hover:bg-muted/50"
              }`}
              style={{
                display: "grid",
                gridTemplateColumns: "var(--trace-name-col) 1fr",
                ...rowStyle,
              }}
            >
              <div
                className="flex items-center gap-1 min-w-0 pr-3 py-1.5"
                style={{
                  paddingLeft: `calc(0.25rem + ${depth * 0.875}rem)`,
                }}
              >
                {canToggle ? (
                  <button
                    type="button"
                    aria-label={isExpanded ? "Collapse" : "Expand"}
                    aria-expanded={isExpanded}
                    disabled={isLoading}
                    onClick={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      onToggle(run.id);
                    }}
                    className="inline-flex items-center justify-center size-4 shrink-0 rounded hover:bg-muted disabled:opacity-50 disabled:cursor-wait cursor-pointer text-muted-foreground"
                  >
                    {isExpanded ? (
                      <ChevronDown className="size-3"/>
                    ) : (
                      <ChevronRight className="size-3"/>
                    )}
                  </button>
                ) : (
                  <span className="inline-block size-4 shrink-0"/>
                )}
                <span
                  className={`size-1.5 rounded-full shrink-0 ${
                    isCurrent ? "bg-primary" : ""
                  }`}
                />
                <Link
                  to={`/runs/${run.id}`}
                  className="text-xs leading-none truncate hover:underline"
                  title={run.jobName}
                >
                  {run.jobName}
                </Link>
              </div>

              <div className="relative h-7 border-l border-border/40 overflow-hidden">
                {ticks.slice(1).map((t, i) => (
                  <div
                    key={i}
                    className="absolute top-0 bottom-0 w-px bg-border/10"
                    style={{left: `${pct(t)}%`}}
                  />
                ))}
                <div
                  className="absolute top-1/2 -translate-y-1/2 h-2.5 rounded-sm"
                  style={{
                    left: `${leftPct}%`,
                    width: `${widthPct}%`,
                    minWidth: "3px",
                    backgroundColor: barColor,
                  }}
                />
                {durationMs > 0 && (
                  <span
                    className="absolute top-1/2 -translate-y-1/2 text-[11px] text-muted-foreground/70 tabular-nums whitespace-nowrap"
                    style={{left: `calc(${leftPct + widthPct}% + 6px)`}}
                  >
                      {formatMs(durationMs)}
                    </span>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {loadMoreLaterSiblings && (
        <SiblingPillRow
          direction="later"
          onClick={loadMoreLaterSiblings.onClick}
          isLoading={loadMoreLaterSiblings.isLoading}
        />
      )}
    </div>
  );
}

function SiblingPillRow({
                          direction,
                          onClick,
                          isLoading,
                        }: {
  direction: "earlier" | "later";
  onClick: () => void;
  isLoading: boolean;
}) {
  const Icon = direction === "earlier" ? ChevronUp : ChevronDown;
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={isLoading}
      className="flex items-center justify-center gap-1.5 w-full border-b border-border/50 bg-muted/40 backdrop-blur-sm px-3 py-1.5 text-xs text-muted-foreground hover:bg-muted/60 hover:text-foreground disabled:opacity-50 disabled:cursor-wait cursor-pointer transition-colors"
    >
      <Icon className="size-3.5 shrink-0"/>
      <span>{isLoading ? "Loading…" : "Load more"}</span>
    </button>
  );
}
