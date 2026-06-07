import {
  keepPreviousData,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "react-router";
import {
  api,
  type JobRun,
  JobStatus,
  LogLevelLabels,
  type RunLogEntry,
} from "@/lib/api";
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
import { StatusBadge } from "@/components/status-badge";
import { Progress } from "@/components/ui/progress";
import { formatDate, formatLogTime } from "@/lib/format";
import { useTailFollow } from "@/hooks/use-tail-follow";
import { Ban, ChevronDown, RotateCcw } from "lucide-react";
import {
  type ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { toast } from "sonner";
import {
  type TraceItem,
  TraceView,
  type TraceScrollState,
} from "@/components/trace-view";
import { useVirtualizer } from "@tanstack/react-virtual";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { PageShell } from "@/components/page-shell";
import { PageErrorBanner } from "@/components/page-error-banner";
import { TopBarActions, TopBarBadge } from "@/components/topbar-slot";
import { metadataGridClass } from "@/components/dt-dd";
import { MetadataStrip, type MetadataItem } from "@/components/metadata-strip";
import { Tabs, TabsContent } from "@/components/ui/tabs";
import { TabBar, TabBarTrigger, ToolBar } from "@/components/tab-bar";
import {
  ResizableHandle,
  ResizablePanel,
  ResizablePanelGroup,
} from "@/components/ui/resizable";
import { useMediaQuery } from "@/hooks/use-media-query";
import { useLiveDuration } from "@/hooks/use-live-duration";
import { cn } from "@/lib/utils";

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

// One indexed query returns the whole hierarchy, so polling this is the trace's entire
// freshness mechanism: no per-row polling, no visibility tracking.
const TREE_REFETCH_INTERVAL_MS = 3000;
const TREE_INVALIDATION_DEBOUNCE_MS = 1000;

const LOG_ROW_HEIGHT = 24;
const LIST_ROW_HEIGHT = 24;

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

export function RunDetailPage() {
  const { id } = useParams();
  const runKey = id ?? "";
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const treeInvalidationTimeoutRef = useRef<ReturnType<
    typeof setTimeout
  > | null>(null);
  const invalidateTreeSoon = useCallback(() => {
    if (!id) return;
    if (treeInvalidationTimeoutRef.current) {
      clearTimeout(treeInvalidationTimeoutRef.current);
    }
    treeInvalidationTimeoutRef.current = setTimeout(() => {
      treeInvalidationTimeoutRef.current = null;
      queryClient.invalidateQueries({ queryKey: ["run-tree", id] });
    }, TREE_INVALIDATION_DEBOUNCE_MS);
  }, [id, queryClient]);
  const invalidateTreeNow = useCallback(() => {
    if (treeInvalidationTimeoutRef.current) {
      clearTimeout(treeInvalidationTimeoutRef.current);
      treeInvalidationTimeoutRef.current = null;
    }
    if (id) {
      queryClient.invalidateQueries({ queryKey: ["run-tree", id] });
    }
  }, [id, queryClient]);
  const { data: run, isError } = useQuery({
    queryKey: ["run", id],
    queryFn: () => api.getRun(id!),
    placeholderData: keepPreviousData,
    refetchInterval: (query) => {
      if (query.state.error) return false;
      const s = query.state.data?.status;
      return s === JobStatus.Pending
          || s === JobStatus.Running
          || s === JobStatus.Suspended
        ? 5000
        : false;
    },
  });
  const isActive =
    run?.status === JobStatus.Pending
    || run?.status === JobStatus.Running
    || run?.status === JobStatus.Suspended;

  const { data: tree } = useQuery({
    queryKey: ["run-tree", id],
    queryFn: () => api.getRunTree(id!),
    enabled: !!id,
    placeholderData: keepPreviousData,
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) return TREE_REFETCH_INTERVAL_MS;
      return data.runs.some(
        (r) =>
          r.status === JobStatus.Pending
          || r.status === JobStatus.Running
          || r.status === JobStatus.Suspended,
      )
        ? TREE_REFETCH_INTERVAL_MS
        : false;
    },
  });
  // Logs are kept in a ref instead of state so flushes can append in-place
  // instead of spreading the entire array each frame (a real bottleneck on
  // log-heavy runs). `logsVersion` is bumped to trigger consumer re-renders.
  const logsByRunRef = useRef<Record<string, RunLogEntry[]>>({});
  const [logsVersion, setLogsVersion] = useState(0);
  const bumpLogsVersion = useCallback(() => {
    setLogsVersion((v) => v + 1);
  }, []);
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

  // Server returns runs sorted by (createdAt, id) with depth populated, so building the
  // tree is one bucketing pass plus an iterative DFS.
  const traceItems = useMemo(() => {
    if (!tree || tree.runs.length === 0) return [] as TraceItem[];

    const childrenByParent = new Map<string, JobRun[]>();
    const idSet = new Set(tree.runs.map((r) => r.id));
    const roots: JobRun[] = [];
    for (const run of tree.runs) {
      // Treat a missing parent (orphan) as a root within this tree.
      const parentId =
        run.parentRunId && idSet.has(run.parentRunId) ? run.parentRunId : null;
      if (parentId === null) {
        roots.push(run);
        continue;
      }
      let bucket = childrenByParent.get(parentId);
      if (!bucket) {
        bucket = [];
        childrenByParent.set(parentId, bucket);
      }
      bucket.push(run);
    }

    const result: TraceItem[] = [];
    const stack: JobRun[] = [...roots].reverse();
    while (stack.length > 0) {
      const node = stack.pop()!;
      result.push({ kind: "run", ...node });
      const children = childrenByParent.get(node.id);
      if (children) {
        for (let i = children.length - 1; i >= 0; i--) {
          stack.push(children[i]);
        }
      }
    }
    return result;
  }, [tree]);

  const allowedRunIds = useMemo(() => {
    const ids = new Set<string>();
    if (runKey) {
      ids.add(runKey);
    }
    for (const item of traceItems) {
      ids.add(item.id);
    }
    return ids;
  }, [runKey, traceItems]);

  useEffect(() => {
    /* eslint-disable react-hooks/set-state-in-effect -- accumulated SSE state is owned by setState; pruning when the tree changes is the synchronization */
    logsByRunRef.current = pruneRunMap(logsByRunRef.current, allowedRunIds);
    bumpLogsVersion();
    setLogFilterByRun((prev) => pruneRunMap(prev, allowedRunIds));
    setSseProgressByRun((prev) => pruneRunMap(prev, allowedRunIds));
    setOutputItemsByRun((prev) => pruneRunMap(prev, allowedRunIds));
    setInputItemsByRun((prev) => pruneRunMap(prev, allowedRunIds));
    setAttemptFailuresByRun((prev) => pruneRunMap(prev, allowedRunIds));
    /* eslint-enable react-hooks/set-state-in-effect */

    const nextSeen: Record<string, number> = {};
    for (const runId of allowedRunIds) {
      const seenEventId = lastSeenEventIdByRun.current[runId];
      if (seenEventId) {
        nextSeen[runId] = seenEventId;
      }
    }

    lastSeenEventIdByRun.current = nextSeen;
  }, [allowedRunIds, bumpLogsVersion]);

  // eslint-disable-next-line react-hooks/exhaustive-deps -- ref read; logsVersion is the re-render trigger
  const logs = useMemo(
    () => logsByRunRef.current[runKey] ?? EMPTY_LOGS,
    [runKey, logsVersion],
  );
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

  const setCurrentLogFilter = (value: number | null) => {
    setLogFilterByRun((prev) => ({ ...prev, [runKey]: value }));
  };

  const filteredLogs = useMemo(
    () =>
      logFilter === null ? logs : logs.filter((l) => l.level >= logFilter),
    [logs, logFilter],
  );

  const cancel = useMutation({
    mutationFn: () => api.cancelRun(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["run", id] });
      queryClient.invalidateQueries({ queryKey: ["run-tree", id] });
      toast.success("Run Canceled");
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

      // eslint-disable-next-line react-hooks/immutability -- ref payload is mutable bookkeeping for SSE dedup
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
        let bucket = logsByRunRef.current[runKey];
        if (!bucket) {
          bucket = [];
          logsByRunRef.current[runKey] = bucket;
        }
        bucket.push(...batch);
        bumpLogsVersion();
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
  }, [runKey, bumpLogsVersion]);

  useEffect(() => {
    if (!id) return;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- resetting the expansion state belongs to the SSE lifecycle, not derived data
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
      invalidateTreeSoon();
    });
    es.addEventListener("done", () => {
      doneReceived = true;
      es.close();
      queryClient.invalidateQueries({ queryKey: ["run", id] });
      invalidateTreeNow();
      queryClient.invalidateQueries({ queryKey: ["runs", "job"] });
    });
    es.onerror = () => {
      if (doneReceived) {
        es.close();
        return;
      }
      // Let EventSource auto-reconnect; only fall back to fetch if fully dead.
      if (es.readyState === EventSource.CLOSED) {
        api
          .getRunLogs(id)
          .then((fetched) => {
            if (!stale && fetched.length > 0) {
              logsByRunRef.current[runKey] = fetched;
              bumpLogsVersion();
            }
          })
          .catch(() => {});
      }
    };
    return () => {
      stale = true;
      es.close();
      if (treeInvalidationTimeoutRef.current) {
        clearTimeout(treeInvalidationTimeoutRef.current);
        treeInvalidationTimeoutRef.current = null;
      }
      if (rafId.current) {
        cancelAnimationFrame(rafId.current);
        rafId.current = 0;
      }
    };
  }, [
    id,
    invalidateTreeNow,
    invalidateTreeSoon,
    queryClient,
    runKey,
    scheduleFlush,
    shouldProcessEvent,
    bumpLogsVersion,
  ]);

  const traceScrollRef = useRef<HTMLDivElement>(null);
  const [traceScrollElement, setTraceScrollElement] =
    useState<HTMLDivElement | null>(null);
  const setTraceScrollNode = useCallback((node: HTMLDivElement | null) => {
    traceScrollRef.current = node;
    setTraceScrollElement(node);
  }, []);
  const mobileTraceScrollRef = useRef<HTMLDivElement>(null);
  const [mobileTraceScrollElement, setMobileTraceScrollElement] =
    useState<HTMLDivElement | null>(null);
  const setMobileTraceScrollNode = useCallback(
    (node: HTMLDivElement | null) => {
      mobileTraceScrollRef.current = node;
      setMobileTraceScrollElement(node);
    },
    [],
  );
  const traceScrollStateRef = useRef<TraceScrollState>({
    rootId: null,
    scrollTop: 0,
  });

  const duration = useLiveDuration(run?.startedAt, run?.completedAt);
  const progress = sseProgress ?? run?.progress ?? 0;

  const isWideViewport = useMediaQuery("(min-width: 1280px)");
  const persistedSplit = useMemo<Record<string, number> | undefined>(() => {
    if (typeof window === "undefined") return undefined;
    try {
      const raw = localStorage.getItem("surefire:run-detail:split:v3");
      return raw ? (JSON.parse(raw) as Record<string, number>) : undefined;
    } catch {
      return undefined;
    }
  }, []);
  const persistSplit = useCallback((layout: Record<string, number>) => {
    try {
      localStorage.setItem(
        "surefire:run-detail:split:v3",
        JSON.stringify(layout),
      );
    } catch {
      // storage quota or disabled
    }
  }, []);

  const showErrorsTab = Boolean(run?.reason) || failureRows.length > 0;
  const showLogsTab = logs.length > 0 || isActive;
  const showArgumentsTab = Boolean(run?.arguments);
  const showResultTab = Boolean(run?.result) && outputItems.length === 0;
  const showInputTab = inputItems.length > 0;
  const showOutputTab = outputItems.length > 0;
  const showMobileTraceTab = !isWideViewport && traceItems.length > 0;

  const availableTabs = useMemo(() => {
    const tabs: string[] = [];
    if (showMobileTraceTab) tabs.push("trace");
    if (showArgumentsTab) tabs.push("arguments");
    if (showResultTab) tabs.push("result");
    if (showErrorsTab) tabs.push("errors");
    if (showInputTab) tabs.push("input");
    if (showOutputTab) tabs.push("output");
    if (showLogsTab) tabs.push("logs");
    return tabs;
  }, [
    showMobileTraceTab,
    showErrorsTab,
    showLogsTab,
    showArgumentsTab,
    showResultTab,
    showInputTab,
    showOutputTab,
  ]);

  const [activeTab, setActiveTab] = useState<string>(() => {
    if (typeof window === "undefined") return "";
    try {
      return localStorage.getItem("surefire:run-detail:tab:v1") ?? "";
    } catch {
      return "";
    }
  });

  // Derive the rendered tab so a stale stored value or a vanished tab does not
  // require a setState round-trip. The user's intent (activeTab) is preserved
  // verbatim; when it can't be honored, fall back to the first available tab
  // (availableTabs is already in priority order).
  const effectiveTab = useMemo(() => {
    if (availableTabs.includes(activeTab)) return activeTab;
    return availableTabs[0] ?? "";
  }, [availableTabs, activeTab]);

  useEffect(() => {
    try {
      localStorage.setItem("surefire:run-detail:tab:v1", effectiveTab);
    } catch {
      // storage quota or disabled
    }
  }, [effectiveTab]);

  if (isError)
    return (
      <PageShell>
        <PageErrorBanner message="Failed to load run" />
      </PageShell>
    );
  if (!run)
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
        <div className="p-6">
          <Skeleton className="h-72 w-full rounded-sm" />
        </div>
      </PageShell>
    );

  const traceCount = tree?.truncated
    ? `${traceItems.length} of ${tree.totalCount}`
    : String(traceItems.length);
  const traceHeading = (
    <span className="flex items-baseline gap-1.5 text-sm font-medium tracking-tight text-foreground">
      Trace
      <span className="text-xs tnum text-muted-foreground/80">
        {traceCount}
      </span>
    </span>
  );
  const traceContentDesktop = (
    <div
      ref={setTraceScrollNode}
      className="h-full overflow-auto"
      style={{ scrollPaddingTop: "3rem" }}
    >
      {traceItems.length > 0 ? (
        <TraceView
          key={traceScrollElement ? "desktop-trace-ready" : "desktop-trace"}
          items={traceItems}
          currentRunId={id!}
          rootId={tree?.rootId}
          scrollContainerRef={traceScrollRef}
          scrollStateRef={traceScrollStateRef}
          header={traceHeading}
          headerClassName="bg-card/95 backdrop-blur-sm"
        />
      ) : (
        <div className="eyebrow py-8 text-center">No related runs</div>
      )}
    </div>
  );

  const logFilterSelect = (
    <Select
      value={logFilter === null ? "all" : String(logFilter)}
      onValueChange={(v) =>
        setCurrentLogFilter(v === "all" ? null : Number(v))
      }
    >
      <SelectTrigger size="sm" className="w-32">
        <SelectValue />
      </SelectTrigger>
      <SelectContent position="popper" align="end">
        <SelectItem value="all">All levels</SelectItem>
        <SelectItem value="0">Trace</SelectItem>
        <SelectItem value="1">Debug</SelectItem>
        <SelectItem value="2">Info</SelectItem>
        <SelectItem value="3">Warning</SelectItem>
        <SelectItem value="4">Error</SelectItem>
        <SelectItem value="5">Critical</SelectItem>
      </SelectContent>
    </Select>
  );

  const tabsContent = (
    <Tabs
      value={effectiveTab}
      onValueChange={setActiveTab}
      className="flex h-full min-h-0 flex-col gap-0"
    >
      <TabBar>
        {showMobileTraceTab && (
          <TabBarTrigger value="trace" count={traceItems.length}>
            Trace
          </TabBarTrigger>
        )}
        {showArgumentsTab && (
          <TabBarTrigger value="arguments">Arguments</TabBarTrigger>
        )}
        {showResultTab && (
          <TabBarTrigger value="result">Result</TabBarTrigger>
        )}
        {showErrorsTab && (
          <TabBarTrigger
            value="errors"
            count={failureRows.length > 0 ? failureRows.length : undefined}
          >
            Errors
          </TabBarTrigger>
        )}
        {showInputTab && (
          <TabBarTrigger value="input" count={inputItems.length}>
            Input
          </TabBarTrigger>
        )}
        {showOutputTab && (
          <TabBarTrigger value="output" count={outputItems.length}>
            Output
          </TabBarTrigger>
        )}
        {showLogsTab && (
          <TabBarTrigger
            value="logs"
            count={
              logFilter !== null && logFilter > 0
                ? `${filteredLogs.length}/${logs.length}`
                : logs.length
            }
          >
            Logs
          </TabBarTrigger>
        )}
      </TabBar>

      {effectiveTab === "logs" && <ToolBar>{logFilterSelect}</ToolBar>}

      {showMobileTraceTab && (
        <TabsContent
          value="trace"
          className="mt-0 flex-1 min-h-0 outline-none"
        >
          <div
            ref={setMobileTraceScrollNode}
            className="h-full overflow-auto"
            style={{ scrollPaddingTop: "3rem" }}
          >
            <TraceView
              key={
                mobileTraceScrollElement ? "mobile-trace-ready" : "mobile-trace"
              }
              items={traceItems}
              currentRunId={id!}
              rootId={tree?.rootId}
              scrollContainerRef={mobileTraceScrollRef}
              scrollStateRef={traceScrollStateRef}
            />
          </div>
        </TabsContent>
      )}

      {showErrorsTab && (
        <TabsContent value="errors" className="mt-0 flex-1 min-h-0 outline-none">
          <ErrorsPanel
            run={run}
            failureRows={failureRows}
            expandedFailureRow={expandedFailureRow}
            setExpandedFailureRow={setExpandedFailureRow}
          />
        </TabsContent>
      )}

      <TabsContent value="logs" className="mt-0 flex-1 min-h-0 outline-none">
        <LogsPanel
          logs={filteredLogs}
          isActive={isActive}
          followKey={`${runKey}:logs`}
        />
      </TabsContent>

      {showArgumentsTab && (
        <TabsContent
          value="arguments"
          className="mt-0 flex-1 min-h-0 outline-none"
        >
          <JsonPanel json={run.arguments!} />
        </TabsContent>
      )}

      {showResultTab && (
        <TabsContent value="result" className="mt-0 flex-1 min-h-0 outline-none">
          <JsonPanel json={run.result!} />
        </TabsContent>
      )}

      {showInputTab && (
        <TabsContent value="input" className="mt-0 flex-1 min-h-0 outline-none">
          <StreamPanel
            items={inputItems}
            isActive={isActive}
            followKey={`${runKey}:input`}
            renderItem={(item) => (
              <>
                <span className="text-muted-foreground">{item.param}:</span>{" "}
                {JSON.stringify(item.value)}
              </>
            )}
          />
        </TabsContent>
      )}

      {showOutputTab && (
        <TabsContent
          value="output"
          className="mt-0 flex-1 min-h-0 outline-none"
        >
          <StreamPanel
            items={outputItems}
            isActive={isActive}
            followKey={`${runKey}:output`}
            renderItem={(item) => JSON.stringify(item)}
          />
        </TabsContent>
      )}
    </Tabs>
  );

  return (
    <PageShell>
      <TopBarBadge>
        <StatusBadge status={run.status} />
      </TopBarBadge>
      <TopBarActions>
        {isActive && (
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button variant="outline" size="sm">
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
        {!isActive && (
          <AlertDialog>
            <AlertDialogTrigger asChild>
              <Button variant="outline" size="sm">
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
      </TopBarActions>

      {run.status === JobStatus.Running && (
        <Progress
          value={progress > 0 ? progress * 100 : null}
          className="h-1 rounded-none"
        />
      )}

      <RunMetaStrip run={run} duration={duration} />

      {isWideViewport ? (
        <ResizablePanelGroup
          orientation="horizontal"
          defaultLayout={persistedSplit}
          onLayoutChanged={persistSplit}
          className="flex-1"
        >
          <ResizablePanel
            id="trace"
            defaultSize="36%"
            minSize="20%"
            maxSize="70%"
          >
            {traceContentDesktop}
          </ResizablePanel>
          <ResizableHandle />
          <ResizablePanel id="content" defaultSize="64%" minSize="30%">
            {tabsContent}
          </ResizablePanel>
        </ResizablePanelGroup>
      ) : (
        <div className="flex-1 min-h-0">{tabsContent}</div>
      )}
    </PageShell>
  );
}

function LogsPanel({
  logs,
  isActive,
  followKey,
}: {
  logs: RunLogEntry[];
  isActive: boolean;
  followKey: string;
}) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [scrollElement, setScrollElement] = useState<HTMLDivElement | null>(
    null,
  );
  const setScrollNode = useCallback((node: HTMLDivElement | null) => {
    scrollRef.current = node;
    setScrollElement(node);
  }, []);
  const contentRef = useRef<HTMLDivElement>(null);

  // eslint-disable-next-line react-hooks/incompatible-library -- useVirtualizer manages its own state; React Compiler memoization is unnecessary.
  const virtualizer = useVirtualizer({
    count: logs.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => LOG_ROW_HEIGHT,
    overscan: 20,
    measureElement: (el) => Math.ceil(el.getBoundingClientRect().height),
  });

  useTailFollow({
    scrollElement,
    contentElementRef: contentRef,
    followKey: isActive ? followKey : undefined,
  });

  if (logs.length === 0) {
    return <div className="eyebrow py-10 text-center">No logs yet</div>;
  }

  return (
    <div
      ref={setScrollNode}
      className="h-full overflow-auto"
      style={{ overflowAnchor: "none" }}
    >
      <div ref={contentRef} className="py-2 font-mono text-xs">
        <div
          className="relative"
          style={{ height: `${virtualizer.getTotalSize()}px` }}
        >
          {virtualizer.getVirtualItems().map((virtualItem) => {
            const log = logs[virtualItem.index];
            return (
              <div
                key={virtualItem.index}
                ref={virtualizer.measureElement}
                data-index={virtualItem.index}
                className="absolute top-0 left-0 w-full px-6 py-0.5 whitespace-pre-wrap wrap-break-word"
                style={{ transform: `translateY(${virtualItem.start}px)` }}
              >
                <span className="text-muted-foreground tnum">
                  {formatLogTime(log.timestamp)}
                </span>{" "}
                <span className={logLevelColor(log.level)}>
                  [{LogLevelLabels[log.level] ?? "?"}]
                </span>{" "}
                <span>{log.message}</span>
                {log.exception && (
                  <pre className="mt-1 whitespace-pre-wrap wrap-break-word text-muted-foreground">
                    {log.exception}
                  </pre>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function StreamPanel<T>({
  items,
  renderItem,
  isActive,
  followKey,
}: {
  items: T[];
  renderItem: (item: T, index: number) => ReactNode;
  isActive: boolean;
  followKey: string;
}) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [scrollElement, setScrollElement] = useState<HTMLDivElement | null>(
    null,
  );
  const setScrollNode = useCallback((node: HTMLDivElement | null) => {
    scrollRef.current = node;
    setScrollElement(node);
  }, []);
  const contentRef = useRef<HTMLDivElement>(null);

  // eslint-disable-next-line react-hooks/incompatible-library -- useVirtualizer manages its own state; React Compiler memoization is unnecessary.
  const virtualizer = useVirtualizer({
    count: items.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => LIST_ROW_HEIGHT,
    overscan: 20,
    measureElement: (el) => Math.ceil(el.getBoundingClientRect().height),
  });

  useTailFollow({
    scrollElement,
    contentElementRef: contentRef,
    followKey: isActive ? followKey : undefined,
  });

  if (items.length === 0) {
    return <div className="eyebrow py-10 text-center">No items yet</div>;
  }

  return (
    <div
      ref={setScrollNode}
      className="h-full overflow-auto"
      style={{ overflowAnchor: "none" }}
    >
      <div ref={contentRef} className="py-2 font-mono text-xs">
        <div
          className="relative"
          style={{ height: `${virtualizer.getTotalSize()}px` }}
        >
          {virtualizer.getVirtualItems().map((virtualItem) => {
            const item = items[virtualItem.index];
            return (
              <div
                key={virtualItem.index}
                ref={virtualizer.measureElement}
                data-index={virtualItem.index}
                className="absolute top-0 left-0 w-full px-6 py-0.5 whitespace-pre-wrap wrap-break-word"
                style={{ transform: `translateY(${virtualItem.start}px)` }}
              >
                {renderItem(item, virtualItem.index)}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function ErrorsPanel({
  run,
  failureRows,
  expandedFailureRow,
  setExpandedFailureRow,
}: {
  run: JobRun;
  failureRows: { failure: AttemptFailureItem; key: string }[];
  expandedFailureRow: string | null;
  setExpandedFailureRow: (value: string | null | ((prev: string | null) => string | null)) => void;
}) {
  return (
    <div className="h-full overflow-auto">
      <div
        style={{
          ["--errors-cols" as string]:
            "minmax(0,5rem) minmax(0,14rem) minmax(0,1fr) auto",
        }}
      >
        <div className="min-w-3xl">
          {run.reason && (
            <div
              className={cn(
                "px-6 py-3 text-sm whitespace-pre-wrap wrap-break-word",
                failureRows.length > 0 && "border-b border-border",
              )}
            >
              {run.reason}
            </div>
          )}
          {failureRows.map(({ failure, key }, index) => {
            const isExpanded = expandedFailureRow === key;
            const hasStackTrace = Boolean(failure.stackTrace);
            const headline = [failure.exceptionType, failure.message]
              .filter(Boolean)
              .join(": ");
            return (
              <div
                key={key}
                className={
                  index < failureRows.length - 1
                    ? "border-b border-border"
                    : ""
                }
              >
                <button
                  type="button"
                  onClick={
                    hasStackTrace
                      ? () =>
                          setExpandedFailureRow((prev) =>
                            prev === key ? null : key,
                          )
                      : undefined
                  }
                  disabled={!hasStackTrace}
                  className={cn(
                    "w-full grid items-start text-left text-sm transition-colors",
                    hasStackTrace
                      ? "hover:bg-muted/40 cursor-pointer"
                      : "cursor-default",
                  )}
                  style={{ gridTemplateColumns: "var(--errors-cols)" }}
                >
                  <div className="px-2 pl-6 py-2.5 text-muted-foreground tnum truncate">
                    #{failure.attempt}
                  </div>
                  <div className="px-2 py-2.5 text-muted-foreground tnum truncate">
                    {failure.occurredAt
                      ? formatDate(failure.occurredAt)
                      : ""}
                  </div>
                  <div className="px-2 py-2.5 min-w-0 whitespace-pre-wrap wrap-break-word">
                    {headline}
                  </div>
                  <div className="px-2 pr-6 py-2.5">
                    {hasStackTrace && (
                      <ChevronDown
                        className={cn(
                          "size-4 text-muted-foreground transition-transform",
                          isExpanded && "rotate-180",
                        )}
                      />
                    )}
                  </div>
                </button>
                {isExpanded && failure.stackTrace && (
                  <pre className="text-xs px-6 py-3 whitespace-pre-wrap break-all font-mono text-muted-foreground border-t border-border">
                    {failure.stackTrace}
                  </pre>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function JsonPanel({ json }: { json: string }) {
  return (
    <div className="h-full overflow-auto">
      <pre className="text-xs leading-[1.55] px-6 py-4 whitespace-pre-wrap wrap-break-word font-mono">
        {formatJsonDisplay(json)}
      </pre>
    </div>
  );
}

function logLevelColor(level: number): string {
  switch (level) {
    case 2:
      return "text-status-running";
    case 3:
      return "text-status-pending";
    case 4:
      return "text-status-failed";
    case 5:
      return "text-status-failed";
    default:
      return "text-muted-foreground";
  }
}

interface MetaItem {
  key: string;
  label: string;
  value: React.ReactNode;
  mono?: boolean;
}

interface RunMetaStripProps {
  run: JobRun;
  duration: string;
}

function RunMetaStrip({ run, duration }: RunMetaStripProps) {
  const items: MetaItem[] = [];

  items.push({
    key: "job",
    label: "Job",
    value: (
      <Link
        to={`/jobs/${encodeURIComponent(run.jobName)}`}
        className="text-foreground hover:text-accent-brand transition-colors truncate inline-block max-w-50 align-bottom"
        title={run.jobName}
      >
        {run.jobName}
      </Link>
    ),
  });

  if (run.startedAt) {
    items.push({ key: "duration", label: "Duration", value: duration, mono: true });
  }

  items.push({
    key: "created",
    label: "Created",
    value: formatDate(run.createdAt),
    mono: true,
  });

  if (run.startedAt) {
    items.push({
      key: "started",
      label: "Started",
      value: formatDate(run.startedAt),
      mono: true,
    });
  }
  if (run.completedAt) {
    items.push({
      key: "completed",
      label: "Completed",
      value: formatDate(run.completedAt),
      mono: true,
    });
  }
  if (run.canceledAt) {
    items.push({
      key: "canceled",
      label: "Canceled",
      value: formatDate(run.canceledAt),
      mono: true,
    });
  }

  if (run.nodeName) {
    items.push({
      key: "node",
      label: "Node",
      value: (
        <Link
          to={`/nodes/${encodeURIComponent(run.nodeName)}`}
          className="font-mono text-foreground/85 hover:text-accent-brand transition-colors truncate inline-block max-w-40 align-bottom"
          title={run.nodeName}
        >
          {run.nodeName}
        </Link>
      ),
    });
  }

  if (run.attempt > 1) {
    items.push({ key: "attempt", label: "Attempt", value: run.attempt, mono: true });
  }
  if (run.failureCount > 0) {
    items.push({ key: "failures", label: "Failures", value: run.failureCount, mono: true });
  }
  if (run.replayCount > 0) {
    items.push({ key: "replays", label: "Replays", value: run.replayCount, mono: true });
  }
  items.push({ key: "priority", label: "Priority", value: run.priority, mono: true });

  if (run.parentRunId) {
    items.push({
      key: "parent",
      label: "Parent",
      value: (
        <Link
          to={`/runs/${run.parentRunId}`}
          className="font-mono text-foreground/85 hover:text-accent-brand transition-colors truncate inline-block max-w-32 align-bottom"
          title={run.parentRunId}
        >
          {run.parentRunId}
        </Link>
      ),
    });
  }
  if (run.rerunOfRunId) {
    items.push({
      key: "rerunOf",
      label: "Rerun of",
      value: (
        <Link
          to={`/runs/${run.rerunOfRunId}`}
          className="font-mono text-foreground/85 hover:text-accent-brand transition-colors truncate inline-block max-w-32 align-bottom"
          title={run.rerunOfRunId}
        >
          {run.rerunOfRunId}
        </Link>
      ),
    });
  }
  if (run.deduplicationId) {
    items.push({
      key: "dedup",
      label: "Dedup",
      value: (
        <span
          className="font-mono text-foreground/85 truncate inline-block max-w-40 align-bottom"
          title={run.deduplicationId}
        >
          {run.deduplicationId}
        </span>
      ),
    });
  }
  if (run.notBefore && run.notBefore !== run.createdAt) {
    items.push({
      key: "notBefore",
      label: "Not before",
      value: formatDate(run.notBefore),
      mono: true,
    });
  }
  if (run.notAfter) {
    items.push({
      key: "notAfter",
      label: "Not after",
      value: formatDate(run.notAfter),
      mono: true,
    });
  }
  if (run.expiresAt) {
    items.push({
      key: "expiresAt",
      label: "Expires at",
      value: formatDate(run.expiresAt),
      mono: true,
    });
  }

  const metadataItems: MetadataItem[] = items.map((item) => ({
    key: item.key,
    label: item.label,
    align: item.mono ? "mono" : "default",
    children: item.value,
  }));

  return <MetadataStrip items={metadataItems}/>;
}
