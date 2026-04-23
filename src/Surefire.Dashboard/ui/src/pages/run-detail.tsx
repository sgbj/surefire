import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { useParams, useNavigate, Link } from "react-router";
import { api, JobStatus, type JobRun, type RunLogEntry, LogLevelLabels } from "@/lib/api";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { StatusBadge } from "@/components/status-badge";
import { Progress } from "@/components/ui/progress";
import { formatDate, formatDuration, formatLogTime } from "@/lib/format";
import { useLiveDuration } from "@/hooks/use-live-duration";
import { useStickToBottom } from "@/hooks/use-stick-to-bottom";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Ban, CircleAlert, RotateCcw } from "lucide-react";
import { DtDd } from "@/components/dt-dd";
import {
  Fragment,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { toast } from "sonner";
import { TraceView, type TraceItem } from "@/components/trace-view";
import { useVirtualizer } from "@tanstack/react-virtual";
import { useInfiniteScroll } from "@/hooks/use-infinite-scroll";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

function formatJsonDisplay(json: string): string {
  try {
    return JSON.stringify(JSON.parse(json), null, 2);
  } catch {
    return json;
  }
}

const EMPTY_LOGS: RunLogEntry[] = [];
const EMPTY_OUTPUT_ITEMS: unknown[] = [];
const EMPTY_INPUT_ITEMS: { param: string; value: unknown }[] = [];
const EMPTY_ATTEMPT_FAILURES: AttemptFailureItem[] = [];
const CHILD_RUN_PAGE_SIZE = 500;
// Trace: tree-aware — ancestors + siblings window + direct children of focus.
// Siblings-before uses a bounded window (typical workflows have few siblings);
// children paginate on demand through /runs/{id}/children.
const TRACE_SIBLING_WINDOW = 200;
const TRACE_CHILDREN_TAKE = 200;

interface AttemptFailureItem {
  attempt: number;
  occurredAt?: string;
  exceptionType?: string;
  message?: string;
  stackTrace?: string;
}

function attemptFailureKey(item: AttemptFailureItem): string {
  return [
    item.attempt,
    item.occurredAt ?? "",
    item.exceptionType ?? "",
    item.message ?? "",
    item.stackTrace ?? "",
  ].join("|");
}

function parseInputStreamItem(
  raw: string,
): { param: string; value: unknown } | null {
  const parsed = JSON.parse(raw) as Record<string, unknown>;

  // Support both direct { param, value } payloads and InputEnvelope
  // payloads shaped like { argument, payload } or { Argument, Payload }.
  const paramCandidate = (parsed.param ??
    parsed.argument ??
    parsed.Argument) as string | undefined;
  const payloadCandidate = parsed.value ?? parsed.payload ?? parsed.Payload;

  if (!paramCandidate) {
    return null;
  }

  if (typeof payloadCandidate === "string") {
    try {
      return { param: paramCandidate, value: JSON.parse(payloadCandidate) };
    } catch {
      return { param: paramCandidate, value: payloadCandidate };
    }
  }

  return { param: paramCandidate, value: payloadCandidate ?? null };
}

function parseAttemptFailureItem(raw: string): AttemptFailureItem | null {
  const parsed = JSON.parse(raw) as Record<string, unknown>;

  const attempt = Number(parsed.attempt ?? parsed.Attempt);
  if (!Number.isFinite(attempt) || attempt < 0) {
    return null;
  }

  return {
    attempt,
    occurredAt: (parsed.occurredAt ?? parsed.OccurredAt) as string | undefined,
    exceptionType: (parsed.exceptionType ?? parsed.ExceptionType) as
      | string
      | undefined,
    message: (parsed.message ?? parsed.Message) as string | undefined,
    stackTrace: (parsed.stackTrace ?? parsed.StackTrace) as string | undefined,
  };
}

function pruneRunMap<T>(source: Record<string, T>, allowedRunIds: Set<string>) {
  const entries = Object.entries(source).filter(([runId]) =>
    allowedRunIds.has(runId),
  );
  return Object.fromEntries(entries) as Record<string, T>;
}

function dedupeRuns(runs: JobRun[]): JobRun[] {
  const seen = new Set<string>();
  const result: JobRun[] = [];
  for (const run of runs) {
    if (seen.has(run.id)) {
      continue;
    }

    seen.add(run.id);
    result.push(run);
  }

  return result;
}

export function RunDetailPage() {
  const { id } = useParams();
  const runKey = id ?? "";
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { data: run, isError } = useQuery({
    queryKey: ["run", id],
    queryFn: () => api.getRun(id!),
    refetchInterval: (query) => {
      if (query.state.error) return false;
      const s = query.state.data?.status;
      return s === JobStatus.Pending || s === JobStatus.Running ? 5000 : false;
    },
  });
  const isActive = run?.status === JobStatus.Pending || run?.status === JobStatus.Running;
  const [childRunsSnapshotCreatedBefore, setChildRunsSnapshotCreatedBefore] =
    useState(() => new Date(Date.now() + 1).toISOString());

  useEffect(() => {
    setChildRunsSnapshotCreatedBefore(new Date(Date.now() + 1).toISOString());
  }, [id]);

  const {
    data: childRunsData,
    hasNextPage: hasNextChildRunsPage,
    fetchNextPage: fetchNextChildRunsPage,
    isFetchingNextPage: isLoadingMoreChildren,
  } = useInfiniteQuery({
    queryKey: ["runs", "children", id, childRunsSnapshotCreatedBefore],
    queryFn: ({ pageParam }) =>
      api.getRuns({
        parentRunId: id!,
        skip: pageParam * CHILD_RUN_PAGE_SIZE,
        take: CHILD_RUN_PAGE_SIZE,
        createdBefore: childRunsSnapshotCreatedBefore,
      }),
    initialPageParam: 0,
    getNextPageParam: (lastPage, pages) => {
      const loaded = pages.reduce((sum, page) => sum + page.items.length, 0);
      return loaded < lastPage.totalCount ? pages.length : undefined;
    },
    refetchInterval: (query) => {
      const pages = query.state.data?.pages;
      if (!pages) return 5000;

      const items = dedupeRuns(pages.flatMap((page) => page.items));
      const hasActiveChild = items.some(
        (r) => r.status === JobStatus.Pending || r.status === JobStatus.Running,
      );
      return isActive || hasActiveChild ? 5000 : false;
    },
    enabled: !!id,
  });

  // Initial focused trace view. Single round-trip returns: ancestor chain + focus +
  // siblings window + first page of children. Lazy expansion via api.getRunChildren
  // for "load more" actions.
  const { data: traceData } = useQuery({
    queryKey: ["run-trace", id],
    queryFn: () =>
      api.getRunTrace(id!, {
        siblingWindow: TRACE_SIBLING_WINDOW,
        childrenTake: TRACE_CHILDREN_TAKE,
      }),
    enabled: !!id,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) return 5000;
      const allRuns = [
        ...data.ancestors,
        data.focus,
        ...data.siblingsBefore,
        ...data.siblingsAfter,
        ...data.children,
      ];
      return allRuns.some((r) => r.status === JobStatus.Pending || r.status === JobStatus.Running)
        ? 5000
        : false;
    },
  });

  // Lazy-expansion tree state. Clicking a node's disclosure caret populates
  // childrenByNode[nodeId] (paginated via childrenCursorByNode); collapsing
  // removes it from expandedNodes but preserves the cache for instant re-open.
  // Focus is always conceptually expanded: its children come from
  // traceData.children plus any paginated extras in childrenByNode[focusId].
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
  const [childrenByNode, setChildrenByNode] = useState<
    Record<string, JobRun[]>
  >({});
  const [childrenCursorByNode, setChildrenCursorByNode] = useState<
    Record<string, string | null>
  >({});
  const [loadingNodes, setLoadingNodes] = useState<Set<string>>(new Set());

  // Extended sibling windows when the user paginates past the initial bounds.
  // Cursor tri-state: `undefined` = user hasn't paginated in this direction yet
  // (fall back to traceData's initial cursor); `string` = next page available;
  // `null` = paginated to exhaustion, no more rows. Collapsing the last two into
  // a single nullable would hide the exhaustion signal behind the initial cursor
  // and keep the load-more button showing forever.
  const [extraSiblingsBefore, setExtraSiblingsBefore] = useState<JobRun[]>([]);
  const [extraSiblingsBeforeCursor, setExtraSiblingsBeforeCursor] = useState<
    string | null | undefined
  >(undefined);
  const [extraSiblingsAfter, setExtraSiblingsAfter] = useState<JobRun[]>([]);
  const [extraSiblingsAfterCursor, setExtraSiblingsAfterCursor] = useState<
    string | null | undefined
  >(undefined);
  const [isLoadingMoreSiblingsAfter, setIsLoadingMoreSiblingsAfter] =
    useState(false);
  const [isLoadingMoreSiblingsBefore, setIsLoadingMoreSiblingsBefore] =
    useState(false);

  // Reset all trace-local state when the focus run changes.
  useEffect(() => {
    setExpandedNodes(new Set());
    setChildrenByNode({});
    setChildrenCursorByNode({});
    setLoadingNodes(new Set());
    setExtraSiblingsBefore([]);
    setExtraSiblingsBeforeCursor(undefined);
    setExtraSiblingsAfter([]);
    setExtraSiblingsAfterCursor(undefined);
  }, [id]);
  const [logsByRun, setLogsByRun] = useState<Record<string, RunLogEntry[]>>({});
  const [logFilterByRun, setLogFilterByRun] = useState<
    Record<string, number | null>
  >({});
  const [sseProgressByRun, setSseProgressByRun] = useState<
    Record<string, number | null>
  >({});
  const [outputItemsByRun, setOutputItemsByRun] = useState<
    Record<string, unknown[]>
  >({});
  const [inputItemsByRun, setInputItemsByRun] = useState<
    Record<string, { param: string; value: unknown }[]>
  >({});
  const [attemptFailuresByRun, setAttemptFailuresByRun] = useState<
    Record<string, AttemptFailureItem[]>
  >({});
  const [expandedFailureRow, setExpandedFailureRow] = useState<string | null>(
    null,
  );

  // DFS-flatten the tree into ordered TraceItems. Focus is always expanded; its
  // children merge traceData.children (refreshed by the 5s poll — so status
  // changes and new first-page children flow live) with childrenByNode[focusId]
  // (the fully-paginated set from the auto-paginate effect). Non-focus expanded
  // nodes draw solely from childrenByNode (one-shot auto-paginate on expand).
  const traceItems = useMemo(() => {
    if (!traceData) return [] as TraceItem[];

    const focusId = traceData.focus.id;
    const focusDepth = traceData.focus.depth ?? traceData.ancestors.length;

    function childrenOf(node: JobRun): JobRun[] {
      if (node.id === focusId) {
        const paginated = childrenByNode[focusId];
        const fresh = traceData!.children;
        if (paginated === undefined || paginated.length === 0) return fresh;
        // Overlay fresh (polled) first-page statuses onto the paginated set;
        // append any brand-new items from fresh that aren't yet paginated.
        const byId = new Map<string, JobRun>();
        for (const r of paginated) byId.set(r.id, r);
        for (const r of fresh) byId.set(r.id, r);
        const merged: JobRun[] = [];
        const seen = new Set<string>();
        for (const r of paginated) {
          const v = byId.get(r.id)!;
          if (seen.has(v.id)) continue;
          seen.add(v.id);
          merged.push(v);
        }
        for (const r of fresh) {
          if (seen.has(r.id)) continue;
          seen.add(r.id);
          merged.push(r);
        }
        return merged;
      }
      if (expandedNodes.has(node.id)) {
        return childrenByNode[node.id] ?? [];
      }
      return [];
    }

    function flatten(node: JobRun, depth: number): TraceItem[] {
      const result: TraceItem[] = [{ kind: "run", ...node, depth }];
      const childDepth = depth + 1;
      const seen = new Set<string>();
      for (const child of childrenOf(node)) {
        if (seen.has(child.id)) continue;
        seen.add(child.id);
        result.push(...flatten(child, childDepth));
      }
      return result;
    }

    const siblingDepth = focusDepth;
    // Every node that can be expanded (siblings too, not just focus) must route
    // through flatten() so its children appear in the flat list when the user
    // toggles its caret. Ancestors are never expandable, so flattening them is a
    // no-op — but going through flatten keeps the shape uniform.
    const ordered: TraceItem[] = [
      ...traceData.ancestors.flatMap((a) => flatten(a, a.depth ?? 0)),
      ...extraSiblingsBefore.flatMap((s) => flatten(s, siblingDepth)),
      ...traceData.siblingsBefore.flatMap((s) =>
        flatten(s, s.depth ?? siblingDepth),
      ),
      ...flatten(traceData.focus, focusDepth),
      ...traceData.siblingsAfter.flatMap((s) =>
        flatten(s, s.depth ?? siblingDepth),
      ),
      ...extraSiblingsAfter.flatMap((s) => flatten(s, siblingDepth)),
    ];

    // Dedup runs by id across the ordered list.
    const seen = new Set<string>();
    const deduped: TraceItem[] = [];
    for (const item of ordered) {
      if (seen.has(item.id)) continue;
      seen.add(item.id);
      deduped.push(item);
    }
    return deduped;
  }, [
    traceData,
    childrenByNode,
    expandedNodes,
    extraSiblingsBefore,
    extraSiblingsAfter,
  ]);

  // Once expansion has loaded an empty first page and there's no cursor for more,
  // the node is known to have no children — hide its caret.
  const knownEmptyNodes = useMemo(() => {
    const empty = new Set<string>();
    for (const [id, items] of Object.entries(childrenByNode)) {
      if (items.length === 0 && childrenCursorByNode[id] == null) {
        empty.add(id);
      }
    }
    return empty;
  }, [childrenByNode, childrenCursorByNode]);

  // Ancestors render as a linear chain above the focus; they don't get a caret
  // (their one displayed "child" is the path down to focus).
  const ancestorIds = useMemo(() => {
    const ids = new Set<string>();
    for (const a of traceData?.ancestors ?? []) ids.add(a.id);
    return ids;
  }, [traceData]);

  // `undefined` means the user hasn't paginated in this direction yet — fall
  // back to the initial cursor from the trace endpoint. Any explicit value
  // (string OR null) is the authoritative state from the latest paginate call,
  // including the "exhausted" null that must suppress the load-more button.
  const siblingsAfterCursor =
    extraSiblingsAfterCursor !== undefined
      ? extraSiblingsAfterCursor
      : traceData?.siblingsCursor?.after ?? null;
  const siblingsBeforeCursor =
    extraSiblingsBeforeCursor !== undefined
      ? extraSiblingsBeforeCursor
      : traceData?.siblingsCursor?.before ?? null;

  const allowedRunIds = useMemo(() => {
    const ids = new Set<string>();
    if (runKey) {
      ids.add(runKey);
    }
    for (const item of traceItems) {
      if (item.kind === "run") ids.add(item.id);
    }
    return ids;
  }, [runKey, traceItems]);

  useEffect(() => {
    setLogsByRun((prev) => pruneRunMap(prev, allowedRunIds));
    setLogFilterByRun((prev) => pruneRunMap(prev, allowedRunIds));
    setSseProgressByRun((prev) => pruneRunMap(prev, allowedRunIds));
    setOutputItemsByRun((prev) => pruneRunMap(prev, allowedRunIds));
    setInputItemsByRun((prev) => pruneRunMap(prev, allowedRunIds));
    setAttemptFailuresByRun((prev) => pruneRunMap(prev, allowedRunIds));

    const nextSeen: Record<string, number> = {};
    for (const runId of allowedRunIds) {
      const seenEventId = lastSeenEventIdByRun.current[runId];
      if (seenEventId) {
        nextSeen[runId] = seenEventId;
      }
    }

    lastSeenEventIdByRun.current = nextSeen;
  }, [allowedRunIds]);

  const logs = logsByRun[runKey] ?? EMPTY_LOGS;
  const logFilter = logFilterByRun[runKey] ?? null;
  const sseProgress = sseProgressByRun[runKey] ?? null;
  const outputItems = outputItemsByRun[runKey] ?? EMPTY_OUTPUT_ITEMS;
  const inputItems = inputItemsByRun[runKey] ?? EMPTY_INPUT_ITEMS;
  const attemptFailures =
    attemptFailuresByRun[runKey] ?? EMPTY_ATTEMPT_FAILURES;

  const sortedAttemptFailures = useMemo(
    () =>
      [...attemptFailures].sort((a, b) => {
        if ((a.attempt ?? 0) !== (b.attempt ?? 0)) {
          return (a.attempt ?? 0) - (b.attempt ?? 0);
        }

        const aTime = a.occurredAt ? new Date(a.occurredAt).getTime() : 0;
        const bTime = b.occurredAt ? new Date(b.occurredAt).getTime() : 0;
        return aTime - bTime;
      }),
    [attemptFailures],
  );

  const failureRows = useMemo(
    () =>
      sortedAttemptFailures.map((failure, index) => ({
        failure,
        key: `${failure.attempt}-${failure.occurredAt ?? "na"}-${index}`,
      })),
    [sortedAttemptFailures],
  );

  const isDeadLetter = run?.status === JobStatus.Failed;

  useEffect(() => {
    if (isDeadLetter && failureRows.length > 0) {
      setExpandedFailureRow(failureRows[failureRows.length - 1].key);
      return;
    }

    setExpandedFailureRow(null);
  }, [isDeadLetter, failureRows, runKey]);

  const setCurrentLogFilter = (value: number | null) => {
    setLogFilterByRun((prev) => ({ ...prev, [runKey]: value }));
  };

  const filteredLogs = useMemo(
    () =>
      logFilter === null ? logs : logs.filter((l) => l.level >= logFilter),
    [logs, logFilter],
  );

  // All three virtualizers wrap rows to full content; measureElement rounds up
  // to integer pixels via Math.ceil so sub-pixel drift doesn't jitter
  // getTotalSize() mid-scroll.
  const LOG_ROW_HEIGHT = 24;
  const LIST_ROW_HEIGHT = 24;
  const measureRow = (el: Element) =>
    Math.ceil(el.getBoundingClientRect().height);

  // Virtualized log viewer
  const logScrollContainerRef = useRef<HTMLDivElement>(null);
  const logVirtualizer = useVirtualizer({
    count: filteredLogs.length,
    getScrollElement: () => logScrollContainerRef.current,
    estimateSize: () => LOG_ROW_HEIGHT,
    overscan: 20,
    measureElement: measureRow,
  });

  // Virtualized input stream
  const inputScrollContainerRef = useRef<HTMLDivElement>(null);
  const inputVirtualizer = useVirtualizer({
    count: inputItems.length,
    getScrollElement: () => inputScrollContainerRef.current,
    estimateSize: () => LIST_ROW_HEIGHT,
    overscan: 20,
    measureElement: measureRow,
  });

  // Virtualized output stream
  const outputScrollContainerRef = useRef<HTMLDivElement>(null);
  const outputVirtualizer = useVirtualizer({
    count: outputItems.length,
    getScrollElement: () => outputScrollContainerRef.current,
    estimateSize: () => LIST_ROW_HEIGHT,
    overscan: 20,
    measureElement: measureRow,
  });

  useStickToBottom({ scrollElement: logScrollContainerRef.current, virtualizer: logVirtualizer, count: filteredLogs.length });
  useStickToBottom({ scrollElement: inputScrollContainerRef.current, virtualizer: inputVirtualizer, count: inputItems.length });
  useStickToBottom({ scrollElement: outputScrollContainerRef.current, virtualizer: outputVirtualizer, count: outputItems.length });

  const cancel = useMutation({
    mutationFn: () => api.cancelRun(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["run", id] });
      queryClient.invalidateQueries({ queryKey: ["run-trace", id] });
      toast.success("Run cancelled");
    },
    onError: () => toast.error("Failed to cancel run"),
  });

  const retry = useMutation({
    mutationFn: () => api.rerunRun(id!),
    onSuccess: (data) => {
      toast.success("New run created");
      navigate(`/runs/${data.runId}`);
    },
    onError: () => toast.error("Failed to rerun"),
  });

  const logBuf = useRef<RunLogEntry[]>([]);
  const outputBuf = useRef<unknown[]>([]);
  const inputBuf = useRef<{ param: string; value: unknown }[]>([]);
  const attemptFailureBuf = useRef<AttemptFailureItem[]>([]);
  const rafId = useRef<number>(0);
  const lastSeenEventIdByRun = useRef<Record<string, number>>({});

  const shouldProcessEvent = useCallback(
    (event: MessageEvent) => {
      const rawId = event.lastEventId;
      if (!rawId) return true;

      const eventId = Number(rawId);
      if (!Number.isFinite(eventId)) return true;

      const current = lastSeenEventIdByRun.current[runKey] ?? 0;
      if (eventId <= current) return false;

      lastSeenEventIdByRun.current[runKey] = eventId;
      return true;
    },
    [runKey],
  );

  const scheduleFlush = useCallback(() => {
    if (rafId.current) return;
    rafId.current = requestAnimationFrame(() => {
      rafId.current = 0;
      if (logBuf.current.length > 0) {
        const batch = logBuf.current;
        logBuf.current = [];
        setLogsByRun((prev) => ({
          ...prev,
          [runKey]: [...(prev[runKey] ?? []), ...batch],
        }));
      }
      if (outputBuf.current.length > 0) {
        const batch = outputBuf.current;
        outputBuf.current = [];
        setOutputItemsByRun((prev) => ({
          ...prev,
          [runKey]: [...(prev[runKey] ?? []), ...batch],
        }));
      }
      if (inputBuf.current.length > 0) {
        const batch = inputBuf.current;
        inputBuf.current = [];
        setInputItemsByRun((prev) => ({
          ...prev,
          [runKey]: [...(prev[runKey] ?? []), ...batch],
        }));
      }
      if (attemptFailureBuf.current.length > 0) {
        const batch = attemptFailureBuf.current;
        attemptFailureBuf.current = [];
        setAttemptFailuresByRun((prev) => ({
          ...prev,
          [runKey]: (() => {
            const existing = prev[runKey] ?? [];
            const seen = new Set(existing.map(attemptFailureKey));
            const merged = [...existing];
            for (const item of batch) {
              const key = attemptFailureKey(item);
              if (seen.has(key)) continue;
              seen.add(key);
              merged.push(item);
            }

            return merged;
          })(),
        }));
      }
    });
  }, [runKey]);

  useEffect(() => {
    if (!id) return;
    setExpandedFailureRow(null);
    logBuf.current = [];
    outputBuf.current = [];
    inputBuf.current = [];
    attemptFailureBuf.current = [];
    let stale = false;
    let doneReceived = false;
    const sinceEventId = lastSeenEventIdByRun.current[runKey] ?? 0;
    const es = api.streamRun(id, sinceEventId);
    es.onmessage = (e) => {
      try {
        if (!shouldProcessEvent(e)) return;
        const entry: RunLogEntry = JSON.parse(e.data);
        if (entry?.timestamp) {
          logBuf.current.push(entry);
          scheduleFlush();
        }
      } catch {
        /* ignore malformed messages */
      }
    };
    es.addEventListener("progress", (e: MessageEvent) => {
      try {
        if (!shouldProcessEvent(e)) return;
        const data = JSON.parse(e.data);
        setSseProgressByRun((prev) => ({
          ...prev,
          [runKey]: data.value ?? Number(e.data),
        }));
      } catch {
        setSseProgressByRun((prev) => ({
          ...prev,
          [runKey]: Number(e.data),
        }));
      }
    });
    es.addEventListener("output", (e: MessageEvent) => {
      try {
        if (!shouldProcessEvent(e)) return;
        const item = JSON.parse(e.data);
        outputBuf.current.push(item);
        scheduleFlush();
      } catch {
        /* ignore malformed */
      }
    });
    es.addEventListener("outputComplete", () => {
      /* stream ended */
    });
    es.addEventListener("input", (e: MessageEvent) => {
      try {
        if (!shouldProcessEvent(e)) return;
        const item = parseInputStreamItem(e.data);
        if (item) {
          inputBuf.current.push(item);
          scheduleFlush();
        }
      } catch {
        /* ignore */
      }
    });
    es.addEventListener("inputComplete", () => {
      /* no special display action */
    });
    es.addEventListener("attemptFailure", (e: MessageEvent) => {
      try {
        if (!shouldProcessEvent(e)) return;
        const item = parseAttemptFailureItem(e.data);
        if (item) {
          attemptFailureBuf.current.push(item);
          scheduleFlush();
        }
      } catch {
        /* ignore */
      }
    });
    es.addEventListener("status", () => {
      queryClient.invalidateQueries({ queryKey: ["run", id] });
      queryClient.invalidateQueries({ queryKey: ["run-trace", id] });
    });
    es.addEventListener("done", () => {
      doneReceived = true;
      es.close();
      queryClient.invalidateQueries({ queryKey: ["run", id] });
      queryClient.invalidateQueries({ queryKey: ["run-trace", id] });
      queryClient.invalidateQueries({ queryKey: ["runs", "job"] });
      queryClient.invalidateQueries({ queryKey: ["runs", "children", id] });
    });
    es.onerror = () => {
      if (doneReceived) {
        es.close();
        return;
      }
      // Don't close — let the browser's built-in EventSource reconnection handle
      // transient network errors. Only fetch logs as fallback if the connection
      // is fully dead (readyState === CLOSED).
      if (es.readyState === EventSource.CLOSED) {
        api
          .getRunLogs(id)
          .then((fetched) => {
            if (!stale && fetched.length > 0) {
              setLogsByRun((prev) => ({ ...prev, [runKey]: fetched }));
            }
          })
          .catch(() => {});
      }
    };
    return () => {
      stale = true;
      es.close();
      if (rafId.current) {
        cancelAnimationFrame(rafId.current);
        rafId.current = 0;
      }
    };
  }, [id, queryClient, runKey, scheduleFlush, shouldProcessEvent]);

  const inputHeader = useMemo(() => {
    if (inputItems.length === 0) return "";
    const counts = new Map<string, number>();
    for (const item of inputItems) {
      counts.set(item.param, (counts.get(item.param) ?? 0) + 1);
    }
    if (counts.size <= 1) return `Input stream (${inputItems.length} items)`;
    return `Input stream (${[...counts.entries()].map(([k, v]) => `${k}: ${v}`).join(", ")})`;
  }, [inputItems]);

  const sortedStepRuns = useMemo(() => {
    const items = dedupeRuns(
      (childRunsData?.pages ?? []).flatMap((page) => page.items),
    );
    if (!items?.length) return [];
    return [...items].sort((a, b) => {
      const createdDelta =
        new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime();
      if (createdDelta !== 0) return createdDelta;
      return a.id.localeCompare(b.id);
    });
  }, [childRunsData]);

  // Trace view scroll container
  const traceScrollRef = useRef<HTMLDivElement>(null);

  // Virtualized triggered runs table
  const TRIGRUN_ROW_HEIGHT = 40;
  const triggeredRunsScrollRef = useRef<HTMLDivElement>(null);
  const triggeredRunsVirtualizer = useVirtualizer({
    count: sortedStepRuns.length,
    getScrollElement: () => triggeredRunsScrollRef.current,
    estimateSize: () => TRIGRUN_ROW_HEIGHT,
    overscan: 20,
  });
  useStickToBottom({
    scrollElement: triggeredRunsScrollRef.current,
    virtualizer: triggeredRunsVirtualizer,
    count: sortedStepRuns.length,
  });

  const duration = useLiveDuration(run?.startedAt, run?.completedAt);
  const progress = sseProgress ?? run?.progress ?? 0;
  const childTotalCount = childRunsData?.pages[0]?.totalCount ?? 0;
  const canLoadMoreChildren = !!hasNextChildRunsPage;

  const loadMoreSiblingsAfter = useCallback(async () => {
    if (
      !traceData ||
      siblingsAfterCursor == null ||
      isLoadingMoreSiblingsAfter
    )
      return;
    if (!traceData.focus.parentRunId) return;
    setIsLoadingMoreSiblingsAfter(true);
    try {
      const page = await api.getRunChildren(traceData.focus.parentRunId, {
        afterCursor: siblingsAfterCursor,
        take: TRACE_SIBLING_WINDOW,
      });
      setExtraSiblingsAfter((prev) => [...prev, ...page.items]);
      setExtraSiblingsAfterCursor(page.nextCursor ?? null);
    } finally {
      setIsLoadingMoreSiblingsAfter(false);
    }
  }, [traceData, siblingsAfterCursor, isLoadingMoreSiblingsAfter]);

  // Scroll preservation on siblings-before prepend is handled inside TraceView
  // by shifting scrollTop by the prepended rows' height when
  // extraSiblingsBeforeCount grows.
  const loadMoreSiblingsBefore = useCallback(async () => {
    if (
      !traceData ||
      siblingsBeforeCursor == null ||
      isLoadingMoreSiblingsBefore
    )
      return;
    if (!traceData.focus.parentRunId) return;
    setIsLoadingMoreSiblingsBefore(true);
    try {
      const page = await api.getRunChildren(traceData.focus.parentRunId, {
        beforeCursor: siblingsBeforeCursor,
        take: TRACE_SIBLING_WINDOW,
      });
      // Before-cursor pagination returns rows in DESC order (newest-first among
      // older siblings). Reverse so the combined list stays chronological.
      const reversed = [...page.items].reverse();
      setExtraSiblingsBefore((prev) => [...reversed, ...prev]);
      setExtraSiblingsBeforeCursor(page.nextCursor ?? null);
    } finally {
      setIsLoadingMoreSiblingsBefore(false);
    }
  }, [traceData, siblingsBeforeCursor, isLoadingMoreSiblingsBefore]);

  const canLoadMoreSiblingsAfter = siblingsAfterCursor != null;
  const canLoadMoreSiblingsBefore = siblingsBeforeCursor != null;

  // Ref-based lock for pagination dedup — protects against re-entrant expand/
  // collapse/expand bursts and concurrent auto-paginations triggered by polling.
  const inFlightPaginationRef = useRef<Set<string>>(new Set());
  // Guard: only auto-paginate focus's children once per focus id per mount,
  // otherwise the 5s trace poll would refire it on every refetch.
  const focusPaginatedRef = useRef<string | null>(null);

  // Paginate *all* cursor pages for a node into childrenByNode. Runs to
  // completion; safe to call again later (skips if already loaded or in flight).
  const paginateAllChildren = useCallback(
    async (nodeId: string, firstCursor?: string) => {
      if (inFlightPaginationRef.current.has(nodeId)) return;
      inFlightPaginationRef.current.add(nodeId);
      setLoadingNodes((prev) => {
        const next = new Set(prev);
        next.add(nodeId);
        return next;
      });
      try {
        let cursor: string | undefined = firstCursor;
        // If no firstCursor and we already have the node cached, we're done.
        if (cursor === undefined) {
          // Fetch first page.
          const page = await api.getRunChildren(nodeId, {
            take: TRACE_CHILDREN_TAKE,
          });
          setChildrenByNode((prev) => ({ ...prev, [nodeId]: page.items }));
          setChildrenCursorByNode((prev) => ({
            ...prev,
            [nodeId]: page.nextCursor ?? null,
          }));
          cursor = page.nextCursor ?? undefined;
        }
        // Continue paginating until cursor exhausted.
        while (cursor) {
          const page = await api.getRunChildren(nodeId, {
            afterCursor: cursor,
            take: TRACE_CHILDREN_TAKE,
          });
          const newItems = page.items;
          setChildrenByNode((prev) => ({
            ...prev,
            [nodeId]: [...(prev[nodeId] ?? []), ...newItems],
          }));
          setChildrenCursorByNode((prev) => ({
            ...prev,
            [nodeId]: page.nextCursor ?? null,
          }));
          cursor = page.nextCursor ?? undefined;
        }
      } finally {
        inFlightPaginationRef.current.delete(nodeId);
        setLoadingNodes((prev) => {
          const next = new Set(prev);
          next.delete(nodeId);
          return next;
        });
      }
    },
    [],
  );

  const expandNode = useCallback(
    (nodeId: string) => {
      setExpandedNodes((prev) => {
        if (prev.has(nodeId)) return prev;
        const next = new Set(prev);
        next.add(nodeId);
        return next;
      });
      // Skip fetch if we've already paginated this node fully.
      if (childrenByNode[nodeId] !== undefined) return;
      void paginateAllChildren(nodeId);
    },
    [childrenByNode, paginateAllChildren],
  );

  const collapseNode = useCallback((nodeId: string) => {
    setExpandedNodes((prev) => {
      if (!prev.has(nodeId)) return prev;
      const next = new Set(prev);
      next.delete(nodeId);
      return next;
    });
  }, []);

  const toggleNode = useCallback(
    (nodeId: string) => {
      if (expandedNodes.has(nodeId)) {
        collapseNode(nodeId);
      } else {
        expandNode(nodeId);
      }
    },
    [expandedNodes, expandNode, collapseNode],
  );

  // Focus is always conceptually expanded; traceData.children carries its first
  // page. If there's a cursor for more, paginate the remainder in the background
  // so the tree shows every child without requiring user action. Guarded by
  // focusPaginatedRef so the 5s poll doesn't refire this.
  useEffect(() => {
    if (!traceData) return;
    const focusId = traceData.focus.id;
    const initialCursor = traceData.childrenCursor;
    if (!initialCursor) return;
    if (focusPaginatedRef.current === focusId) return;
    focusPaginatedRef.current = focusId;

    // Seed childrenByNode[focusId] with the first page from traceData so the
    // subsequent cursor pages append cleanly. childrenOf() merges traceData's
    // fresh statuses over this on every render, so polling updates still flow.
    setChildrenByNode((prev) =>
      prev[focusId] !== undefined ? prev : { ...prev, [focusId]: traceData.children },
    );
    setChildrenCursorByNode((prev) => ({ ...prev, [focusId]: initialCursor }));
    void paginateAllChildren(focusId, initialCursor);
  }, [traceData, paginateAllChildren]);

  // Reset per-focus paginate guards whenever the URL changes. Clearing
  // inFlightPaginationRef guarantees that a paginate left mid-flight during
  // the previous focus won't block a fresh expand of the same nodeId here.
  useEffect(() => {
    focusPaginatedRef.current = null;
    inFlightPaginationRef.current.clear();
  }, [id]);

  const { sentinelRef: triggeredRunsSentinelRef } = useInfiniteScroll({
    scrollContainerRef: triggeredRunsScrollRef,
    hasMore: canLoadMoreChildren,
    isLoading: isLoadingMoreChildren,
    onLoadMore: () => void fetchNextChildRunsPage(),
  });

  if (isError)
    return (
      <div className="space-y-6">
        <h2 className="text-xl font-semibold tracking-tight truncate">
          Run {id}
        </h2>
        <Alert variant="destructive">
          <CircleAlert />
          <AlertDescription>Failed to load run</AlertDescription>
        </Alert>
      </div>
    );
  if (!run)
    return (
      <div className="space-y-6">
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-3 min-w-0">
            <Skeleton className="h-7 w-56" />
            <Skeleton className="h-5 w-[4.5rem] rounded-full" />
          </div>
          <div className="flex gap-2 shrink-0">
            <Skeleton className="h-8 w-20" />
          </div>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i}>
              <Skeleton className="h-3 w-16 mb-1.5" />
              <Skeleton className="h-4 w-24" />
            </div>
          ))}
        </div>
        <Skeleton className="h-48 w-full rounded-lg" />
      </div>
    );

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-4">
        <div className="flex items-center gap-3 min-w-0">
          <h2 className="text-xl font-semibold tracking-tight truncate">
            Run {run.id}
          </h2>
          <StatusBadge status={run.status} />
        </div>
        <div className="flex gap-2 shrink-0">
          {isActive && (
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button variant="outline" size="sm" className="cursor-pointer">
                  <Ban className="size-3.5" />
                  Cancel
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Cancel this run?</AlertDialogTitle>
                  <AlertDialogDescription>
                    This will request cancellation of the running job.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Back</AlertDialogCancel>
                  <AlertDialogAction
                    variant="destructive"
                    onClick={() => cancel.mutate()}
                    disabled={cancel.isPending}
                  >
                    Cancel run
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          )}
          {!isActive && !run.jobName.startsWith("_") && (
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button variant="outline" size="sm" className="cursor-pointer">
                  <RotateCcw className="size-3.5" />
                  Re-run
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Re-run this job?</AlertDialogTitle>
                  <AlertDialogDescription>
                    This will create a new run for {run.jobName} with the same
                    arguments.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Back</AlertDialogCancel>
                  <AlertDialogAction
                    onClick={() => retry.mutate()}
                    disabled={retry.isPending}
                  >
                    Re-run
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          )}
        </div>
      </div>

      {run.status === JobStatus.Running && (
        <div className="flex items-center gap-3">
          <div className="relative flex-1">
            <Progress
              value={progress > 0 ? progress * 100 : null}
              className="h-1"
            />
            <Progress
              value={progress > 0 ? progress * 100 : null}
              className="absolute inset-0 h-1 blur-sm opacity-25"
            />
          </div>
          {progress > 0 && (
            <span className="text-xs text-muted-foreground tabular-nums shrink-0">
              {Math.round(progress * 100)}%
            </span>
          )}
        </div>
      )}

      <dl className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-x-6 gap-y-4">
        <DtDd label="Job">
          <Link
            to={`/jobs/${encodeURIComponent(run.jobName)}`}
            className="text-primary hover:underline truncate max-w-[200px] inline-block"
            title={run.jobName}
          >
            {run.jobName}
          </Link>
        </DtDd>
        {run.startedAt && (
          <DtDd label="Duration">
            <span className="tabular-nums">{duration}</span>
          </DtDd>
        )}
        {run.attempt > 1 && <DtDd label="Attempt">{run.attempt}</DtDd>}
        {run.nodeName && (
          <DtDd label="Node">
            <Link
              to={`/nodes/${encodeURIComponent(run.nodeName)}`}
              className="text-primary hover:underline truncate max-w-[160px] inline-block"
              title={run.nodeName}
            >
              {run.nodeName}
            </Link>
          </DtDd>
        )}
        <DtDd label="Created">{formatDate(run.createdAt)}</DtDd>
        {run.notBefore && run.notBefore !== run.createdAt && (
          <DtDd label="Not before">{formatDate(run.notBefore)}</DtDd>
        )}
        {run.startedAt && (
          <DtDd label="Started">{formatDate(run.startedAt)}</DtDd>
        )}
        {run.completedAt && (
          <DtDd label="Completed">{formatDate(run.completedAt)}</DtDd>
        )}
        {run.cancelledAt && (
          <DtDd label="Cancelled">{formatDate(run.cancelledAt)}</DtDd>
        )}
        {run.rerunOfRunId && (
          <DtDd label="Rerun of">
            <Link
              to={`/runs/${run.rerunOfRunId}`}
              className="text-primary hover:underline truncate max-w-[140px] inline-block"
              title={run.rerunOfRunId}
            >
              {run.rerunOfRunId}
            </Link>
          </DtDd>
        )}
        {run.parentRunId && (
          <DtDd label="Triggered by">
            <Link
              to={`/runs/${run.parentRunId}`}
              className="text-primary hover:underline truncate max-w-[140px] inline-block"
              title={run.parentRunId}
            >
              {run.parentRunId}
            </Link>
          </DtDd>
        )}
      </dl>

      {run.arguments && (
        <div className="rounded-lg border overflow-hidden">
          <div className="max-h-[26rem] overflow-y-auto">
            <div className="sticky top-0 z-10 flex items-center py-2.5 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground">Arguments</span>
            </div>
            <pre className="text-[13px] p-2 whitespace-pre-wrap break-all font-mono">
              {formatJsonDisplay(run.arguments)}
            </pre>
          </div>
        </div>
      )}

      {run.result && outputItems.length === 0 && (
        <div className="rounded-lg border overflow-hidden">
          <div className="max-h-[26rem] overflow-y-auto">
            <div className="sticky top-0 z-10 flex items-center py-2.5 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground">Result</span>
            </div>
            <pre className="text-[13px] p-2 whitespace-pre-wrap break-all font-mono">
              {formatJsonDisplay(run.result)}
            </pre>
          </div>
        </div>
      )}

      {run.reason && (
        <div className="rounded-lg border border-destructive/15 overflow-hidden">
          <div className="max-h-[26rem] overflow-y-auto">
            <div className="sticky top-0 z-10 flex items-center py-2.5 px-2 border-b border-destructive/10 bg-destructive/10 backdrop-blur-sm">
              <span className="text-sm text-destructive/80">Reason</span>
            </div>
            <pre className="text-[13px] p-2 whitespace-pre-wrap break-all font-mono">
              {run.reason}
            </pre>
          </div>
        </div>
      )}

      {failureRows.length > 0 && (
        <div className="rounded-lg border border-destructive/15 overflow-hidden">
          <div className="max-h-[26rem] overflow-auto">
          <table className="w-full caption-bottom text-sm min-w-[768px]">
            <TableHeader className="sticky top-0 z-10 bg-destructive/10 backdrop-blur-sm">
              <TableRow className="hover:bg-transparent border-destructive/10">
                <TableCell colSpan={4} className="text-destructive/80">
                  Errors ({failureRows.length})
                </TableCell>
              </TableRow>
              <TableRow className="hover:bg-transparent border-destructive/10">
                <TableHead>Attempt</TableHead>
                <TableHead>Occurred</TableHead>
                <TableHead>Exception</TableHead>
                <TableHead>Message</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {failureRows.map(({ failure, key }) => {
                const isExpanded = expandedFailureRow === key;
                return (
                  <Fragment key={key}>
                    <TableRow
                      className={`cursor-pointer ${
                        isExpanded ? "bg-muted/20" : ""
                      }`}
                      onClick={() =>
                        setExpandedFailureRow((prev) =>
                          prev === key ? null : key,
                        )
                      }
                    >
                      <TableCell className="tabular-nums">
                        #{failure.attempt}
                      </TableCell>
                      <TableCell className="tabular-nums">
                        {failure.occurredAt
                          ? formatDate(failure.occurredAt)
                          : ""}
                      </TableCell>
                      <TableCell className="whitespace-normal break-all">
                        {failure.exceptionType ?? ""}
                      </TableCell>
                      <TableCell className="whitespace-normal break-all">
                        {failure.message ?? ""}
                      </TableCell>
                    </TableRow>
                    {isExpanded && (
                      <TableRow className="hover:bg-transparent cursor-default">
                        <TableCell colSpan={4} className="max-w-0 w-full">
                          {failure.stackTrace ? (
                            <pre className="text-[13px] whitespace-pre-wrap break-words font-mono">
                              {failure.stackTrace}
                            </pre>
                          ) : (
                            <div className="text-sm">
                              No stack trace recorded.
                            </div>
                          )}
                        </TableCell>
                      </TableRow>
                    )}
                  </Fragment>
                );
              })}
            </TableBody>
          </table>
          </div>
        </div>
      )}

      {traceItems.length > 1 && (
        <div className="rounded-lg border overflow-hidden">
          <div
            ref={traceScrollRef}
            className="max-h-[32rem] overflow-auto"
            // scroll-padding-top keeps the sticky header/pill out of the
            // "center" area scrollIntoView targets, so auto-centering lands
            // the focus row below them rather than behind them.
            style={{ scrollPaddingTop: "2.75rem" }}
          >
          <TraceView
            items={traceItems}
            currentRunId={id!}
            ancestorIds={ancestorIds}
            expandedNodes={expandedNodes}
            knownEmptyNodes={knownEmptyNodes}
            loadingNodes={loadingNodes}
            onToggle={toggleNode}
            scrollContainerRef={traceScrollRef}
            header={<span>Trace</span>}
            loadEarlierSiblings={
              canLoadMoreSiblingsBefore
                ? {
                    onClick: () => void loadMoreSiblingsBefore(),
                    isLoading: isLoadingMoreSiblingsBefore,
                  }
                : undefined
            }
            loadMoreLaterSiblings={
              canLoadMoreSiblingsAfter
                ? {
                    onClick: () => void loadMoreSiblingsAfter(),
                    isLoading: isLoadingMoreSiblingsAfter,
                  }
                : undefined
            }
            extraSiblingsBeforeCount={extraSiblingsBefore.length}
          />
          </div>
        </div>
      )}

      {sortedStepRuns.length > 0 && (
        <div className="rounded-lg border overflow-hidden">
        <div
          ref={triggeredRunsScrollRef}
          className="max-h-[32rem] overflow-auto"
          style={{
            // Explicit fractions on every column: auto would measure each row
            // independently (StatusBadge width vs "STATUS" header text) and
            // misalign the grid between header and virtualized rows.
            ["--trigrun-cols" as string]:
              "minmax(0,2fr) minmax(0,2fr) minmax(0,1fr) minmax(0,1.5fr) minmax(0,1fr) minmax(0,1.25fr)",
          }}
        >
        <div className="min-w-[768px]">
          <div className="sticky top-0 z-10 bg-muted/30 backdrop-blur-sm border-b">
            <div className="flex items-center py-2.5 px-2">
              <span className="text-sm text-muted-foreground">
                Triggered runs ({sortedStepRuns.length} /{" "}
                {childTotalCount || sortedStepRuns.length})
              </span>
            </div>
            <div
              className="grid items-center border-t border-border/50 text-xs font-medium uppercase tracking-wider text-muted-foreground"
              style={{ gridTemplateColumns: "var(--trigrun-cols)" }}
            >
              <div className="px-2 py-2.5 pl-4">ID</div>
              <div className="px-2 py-2.5">Job</div>
              <div className="px-2 py-2.5">Status</div>
              <div className="px-2 py-2.5">Started</div>
              <div className="px-2 py-2.5">Duration</div>
              <div className="px-2 py-2.5">Node</div>
            </div>
          </div>
          <div
            className="relative"
            style={{ height: `${triggeredRunsVirtualizer.getTotalSize()}px` }}
          >
            {triggeredRunsVirtualizer.getVirtualItems().map((virtualItem) => {
              const r = sortedStepRuns[virtualItem.index];
              return (
                <div
                  key={r.id}
                  data-index={virtualItem.index}
                  className="absolute top-0 left-0 w-full grid items-center border-b border-border/50 text-sm hover:bg-muted/50 transition-colors"
                  style={{
                    gridTemplateColumns: "var(--trigrun-cols)",
                    height: `${TRIGRUN_ROW_HEIGHT}px`,
                    transform: `translateY(${virtualItem.start}px)`,
                  }}
                >
                  <div className="px-2 pl-4 flex items-center min-w-0">
                    <Link
                      to={`/runs/${r.id}`}
                      className="text-primary hover:underline truncate block"
                      title={r.id}
                    >
                      {r.id}
                    </Link>
                  </div>
                  <div className="px-2 flex items-center min-w-0">
                    <Link
                      to={`/jobs/${encodeURIComponent(r.jobName)}`}
                      className="text-primary hover:underline truncate block"
                      title={r.jobName}
                    >
                      {r.jobName}
                    </Link>
                  </div>
                  <div className="px-2 flex items-center min-w-0">
                    <StatusBadge status={r.status} />
                  </div>
                  <div className="px-2 flex items-center min-w-0 tabular-nums truncate">
                    {r.startedAt ? formatDate(r.startedAt) : ""}
                  </div>
                  <div className="px-2 flex items-center min-w-0 tabular-nums truncate">
                    {formatDuration(r.startedAt, r.completedAt)}
                  </div>
                  <div className="px-2 flex items-center min-w-0">
                    {r.nodeName ? (
                      <Link
                        to={`/nodes/${encodeURIComponent(r.nodeName)}`}
                        className="text-primary hover:underline truncate block"
                        title={r.nodeName}
                      >
                        {r.nodeName}
                      </Link>
                    ) : (
                      ""
                    )}
                  </div>
                </div>
              );
            })}
          </div>
          {canLoadMoreChildren && (
            <div
              ref={triggeredRunsSentinelRef}
              className="h-10 flex items-center justify-center text-xs text-muted-foreground"
            >
              {isLoadingMoreChildren ? "Loading…" : ""}
            </div>
          )}
        </div>
        </div>
        </div>
      )}

      {inputItems.length > 0 && (
        <div className="rounded-lg border overflow-hidden">
          <div
            ref={inputScrollContainerRef}
            className="max-h-[26rem] overflow-y-auto font-mono text-[13px]"
          >
            <div className="sticky top-0 z-10 flex items-center py-2.5 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground font-sans">
                {inputHeader}
              </span>
            </div>
            <div
              className="relative px-2"
              style={{ height: `${inputVirtualizer.getTotalSize()}px` }}
            >
              {inputVirtualizer.getVirtualItems().map((virtualItem) => {
                const item = inputItems[virtualItem.index];
                return (
                  <div
                    key={virtualItem.index}
                    ref={inputVirtualizer.measureElement}
                    data-index={virtualItem.index}
                    className="absolute top-0 left-0 w-full px-2 py-0.5 whitespace-pre-wrap break-all"
                    style={{
                      transform: `translateY(${virtualItem.start}px)`,
                    }}
                  >
                    <span className="text-muted-foreground">{item.param}:</span>{" "}
                    {JSON.stringify(item.value)}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {outputItems.length > 0 && (
        <div className="rounded-lg border overflow-hidden">
          <div
            ref={outputScrollContainerRef}
            className="max-h-[26rem] overflow-y-auto font-mono text-[13px]"
          >
            <div className="sticky top-0 z-10 flex items-center py-2.5 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground font-sans">
                Output stream ({outputItems.length} items)
              </span>
            </div>
            <div
              className="relative px-2"
              style={{ height: `${outputVirtualizer.getTotalSize()}px` }}
            >
              {outputVirtualizer.getVirtualItems().map((virtualItem) => {
                const item = outputItems[virtualItem.index];
                return (
                  <div
                    key={virtualItem.index}
                    ref={outputVirtualizer.measureElement}
                    data-index={virtualItem.index}
                    className="absolute top-0 left-0 w-full px-2 py-0.5 whitespace-pre-wrap break-all"
                    style={{
                      transform: `translateY(${virtualItem.start}px)`,
                    }}
                  >
                    {JSON.stringify(item)}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {logs.length > 0 && (
        <div className="rounded-lg border overflow-hidden">
          <div
            ref={logScrollContainerRef}
            className="max-h-[26rem] overflow-y-auto font-mono text-[13px]"
          >
            <div className="sticky top-0 z-10 flex items-center gap-3 py-2.5 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground font-sans">
                Logs (
                {logFilter !== null
                  ? `${filteredLogs.length} / ${logs.length}`
                  : logs.length}
                )
              </span>
              <Select
                value={logFilter === null ? "all" : String(logFilter)}
                onValueChange={(v) =>
                  setCurrentLogFilter(v === "all" ? null : Number(v))
                }
              >
                <SelectTrigger className="h-auto! border-0 bg-transparent! shadow-none px-1 py-0 text-sm text-muted-foreground font-sans gap-0.5 [&_svg]:size-3">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All</SelectItem>
                  <SelectItem value="0">Trace</SelectItem>
                  <SelectItem value="1">Debug</SelectItem>
                  <SelectItem value="2">Info</SelectItem>
                  <SelectItem value="3">Warning</SelectItem>
                  <SelectItem value="4">Error</SelectItem>
                  <SelectItem value="5">Critical</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div
              className="relative px-2"
              style={{ height: `${logVirtualizer.getTotalSize()}px` }}
            >
              {logVirtualizer.getVirtualItems().map((virtualItem) => {
                const log = filteredLogs[virtualItem.index];
                return (
                  <div
                    key={virtualItem.index}
                    ref={logVirtualizer.measureElement}
                    data-index={virtualItem.index}
                    className="absolute top-0 left-0 w-full px-2 py-0.5 whitespace-pre-wrap break-words"
                    style={{
                      transform: `translateY(${virtualItem.start}px)`,
                    }}
                  >
                    <span className="text-muted-foreground tabular-nums">
                      {formatLogTime(log.timestamp)}
                    </span>{" "}
                    <span className={logLevelColor(log.level)}>
                      [{LogLevelLabels[log.level] ?? "?"}]
                    </span>{" "}
                    <span>{log.message}</span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function logLevelColor(level: number): string {
  switch (level) {
    case 2:
      return "text-sky-600 dark:text-sky-400";
    case 3:
      return "text-amber-600 dark:text-amber-400";
    case 4:
      return "text-rose-600 dark:text-rose-400";
    case 5:
      return "text-rose-700 dark:text-rose-300";
    default:
      return "text-muted-foreground";
  }
}
