import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { useParams, useNavigate, Link } from "react-router";
import { api, type JobRun, type RunLogEntry, LogLevelLabels } from "@/lib/api";
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
  Table,
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
import { TraceView } from "@/components/trace-view";
import { useVirtualizer } from "@tanstack/react-virtual";
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
const TRACE_PAGE_SIZE = 500;

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
      const s = query.state.data?.status;
      return s === 0 || s === 1 ? 5000 : false;
    },
  });
  const isActive = run?.status === 0 || run?.status === 1;
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
        (r) => r.status === 0 || r.status === 1,
      );
      return isActive || hasActiveChild ? 5000 : false;
    },
    enabled: !!id,
  });

  const {
    data: tracePageData,
    hasNextPage: hasNextTracePage,
    fetchNextPage: fetchNextTracePage,
    isFetchingNextPage: isLoadingMoreTrace,
  } = useInfiniteQuery({
    queryKey: ["run-trace", id],
    queryFn: ({ pageParam }) =>
      api.getRunTracePage(id!, {
        skip: pageParam * TRACE_PAGE_SIZE,
        take: TRACE_PAGE_SIZE,
      }),
    initialPageParam: 0,
    getNextPageParam: (lastPage, pages) => {
      const loaded = pages.reduce((sum, page) => sum + page.items.length, 0);
      return loaded < lastPage.totalCount ? pages.length : undefined;
    },
    refetchInterval: (query) => {
      const pages = query.state.data?.pages;
      if (!pages) return 5000;

      const runs = dedupeRuns(pages.flatMap((page) => page.items));
      return runs.some((r) => r.status === 0 || r.status === 1) ? 5000 : false;
    },
    enabled: !!id,
  });
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

  const allowedRunIds = useMemo(() => {
    const ids = new Set<string>();
    if (runKey) {
      ids.add(runKey);
    }

    for (const traceRun of (tracePageData?.pages ?? []).flatMap(
      (page) => page.items,
    )) {
      ids.add(traceRun.id);
    }

    return ids;
  }, [runKey, tracePageData]);

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

  const isDeadLetter = run?.status === 5;

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

  // Virtualized log viewer
  const logScrollContainerRef = useRef<HTMLDivElement>(null);
  const logIsAtBottom = useRef(true);
  const logProgrammatic = useRef(false);
  const logVirtualizer = useVirtualizer({
    count: filteredLogs.length,
    getScrollElement: () => logScrollContainerRef.current,
    estimateSize: () => 24,
    overscan: 20,
  });

  // Virtualized input stream
  const inputScrollContainerRef = useRef<HTMLDivElement>(null);
  const inputIsAtBottom = useRef(true);
  const inputProgrammatic = useRef(false);
  const inputVirtualizer = useVirtualizer({
    count: inputItems.length,
    getScrollElement: () => inputScrollContainerRef.current,
    estimateSize: () => 24,
    overscan: 20,
  });

  // Virtualized output stream
  const outputScrollContainerRef = useRef<HTMLDivElement>(null);
  const outputIsAtBottom = useRef(true);
  const outputProgrammatic = useRef(false);
  const outputVirtualizer = useVirtualizer({
    count: outputItems.length,
    getScrollElement: () => outputScrollContainerRef.current,
    estimateSize: () => 24,
    overscan: 20,
  });

  // Stick-to-bottom for log virtualizer
  useEffect(() => {
    const el = logScrollContainerRef.current;
    if (!el) return;
    const onScroll = () => {
      if (logProgrammatic.current) return;
      logIsAtBottom.current =
        el.scrollHeight - el.scrollTop - el.clientHeight <= 4;
    };
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, []);

  useEffect(() => {
    if (logIsAtBottom.current && filteredLogs.length > 0) {
      logProgrammatic.current = true;
      logVirtualizer.scrollToIndex(filteredLogs.length - 1, { align: "end" });
      requestAnimationFrame(() => {
        logProgrammatic.current = false;
      });
    }
  }, [filteredLogs.length, logVirtualizer]);

  // Stick-to-bottom for input virtualizer
  useEffect(() => {
    const el = inputScrollContainerRef.current;
    if (!el) return;
    const onScroll = () => {
      if (inputProgrammatic.current) return;
      inputIsAtBottom.current =
        el.scrollHeight - el.scrollTop - el.clientHeight <= 4;
    };
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, []);

  useEffect(() => {
    if (inputIsAtBottom.current && inputItems.length > 0) {
      inputProgrammatic.current = true;
      inputVirtualizer.scrollToIndex(inputItems.length - 1, { align: "end" });
      requestAnimationFrame(() => {
        inputProgrammatic.current = false;
      });
    }
  }, [inputItems.length, inputVirtualizer]);

  // Stick-to-bottom for output virtualizer
  useEffect(() => {
    const el = outputScrollContainerRef.current;
    if (!el) return;
    const onScroll = () => {
      if (outputProgrammatic.current) return;
      outputIsAtBottom.current =
        el.scrollHeight - el.scrollTop - el.clientHeight <= 4;
    };
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, []);

  useEffect(() => {
    if (outputIsAtBottom.current && outputItems.length > 0) {
      outputProgrammatic.current = true;
      outputVirtualizer.scrollToIndex(outputItems.length - 1, { align: "end" });
      requestAnimationFrame(() => {
        outputProgrammatic.current = false;
      });
    }
  }, [outputItems.length, outputVirtualizer]);

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
  }, [id, queryClient, runKey, scheduleFlush]);

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
  const triggeredRunsScrollRef = useRef<HTMLDivElement>(null);
  const triggeredRunsVirtualizer = useVirtualizer({
    count: sortedStepRuns.length,
    getScrollElement: () => triggeredRunsScrollRef.current,
    estimateSize: () => 40,
    overscan: 10,
  });

  const duration = useLiveDuration(run?.startedAt, run?.completedAt);
  const progress = sseProgress ?? run?.progress ?? 0;
  const traceRuns = dedupeRuns(
    (tracePageData?.pages ?? []).flatMap((page) => page.items),
  );
  const traceTotalCount = tracePageData?.pages[0]?.totalCount ?? 0;
  const childTotalCount = childRunsData?.pages[0]?.totalCount ?? 0;
  const canLoadMoreTrace = !!hasNextTracePage;
  const canLoadMoreChildren = !!hasNextChildRunsPage;
  const traceContainsCurrentRun = traceRuns.some(
    (traceRun) => traceRun.id === runKey,
  );

  useEffect(() => {
    if (
      !runKey ||
      traceContainsCurrentRun ||
      !canLoadMoreTrace ||
      isLoadingMoreTrace
    ) {
      return;
    }

    void fetchNextTracePage();
  }, [
    runKey,
    traceContainsCurrentRun,
    canLoadMoreTrace,
    isLoadingMoreTrace,
    fetchNextTracePage,
  ]);

  if (isError)
    return (
      <Alert variant="destructive">
        <CircleAlert />
        <AlertDescription>Failed to load run</AlertDescription>
      </Alert>
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

      {run.status === 1 && (
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
        <div className="flex max-h-[26rem]">
          <div className="flex-1 min-h-0 rounded-md border overflow-y-auto">
            <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground">Arguments</span>
            </div>
            <pre className="text-[13px] p-2 whitespace-pre-wrap break-all font-mono">
              {formatJsonDisplay(run.arguments)}
            </pre>
          </div>
        </div>
      )}

      {run.result && outputItems.length === 0 && (
        <div className="flex max-h-[26rem]">
          <div className="flex-1 min-h-0 rounded-md border overflow-y-auto">
            <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground">Result</span>
            </div>
            <pre className="text-[13px] p-2 whitespace-pre-wrap break-all font-mono">
              {formatJsonDisplay(run.result)}
            </pre>
          </div>
        </div>
      )}

      {run.error && failureRows.length === 0 && (
        <div className="rounded-md border border-destructive/20 overflow-hidden">
          <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b border-destructive/15 bg-destructive/[0.03] backdrop-blur-sm">
            <span className="text-sm text-destructive/90">Failure reason</span>
          </div>
          <pre className="text-[13px] p-2 whitespace-pre-wrap break-all font-mono">
            {run.error}
          </pre>
        </div>
      )}

      {failureRows.length > 0 && (
        <div className="rounded-md border border-destructive/20 overflow-hidden">
          <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b border-destructive/15 bg-destructive/[0.03] backdrop-blur-sm">
            <span className="text-sm text-destructive/90">
              Errors ({failureRows.length})
            </span>
          </div>
          <div className="overflow-x-auto">
            <Table className="min-w-[40rem] md:min-w-full">
              <TableHeader>
                <TableRow>
                  <TableHead>Attempt</TableHead>
                  <TableHead>Occurred</TableHead>
                  <TableHead className="hidden md:table-cell">
                    Exception
                  </TableHead>
                  <TableHead className="hidden md:table-cell">
                    Message
                  </TableHead>
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
                        <TableCell className="hidden md:table-cell whitespace-normal break-all">
                          {failure.exceptionType ?? ""}
                        </TableCell>
                        <TableCell className="hidden md:table-cell whitespace-normal break-all">
                          {failure.message ?? ""}
                        </TableCell>
                      </TableRow>
                      {isExpanded && (
                        <TableRow className="hover:bg-transparent cursor-default">
                          <TableCell colSpan={4}>
                            <div className="space-y-2 text-sm">
                              <div className="md:hidden">
                                <div>Exception</div>
                                <div className="whitespace-normal break-all">
                                  {failure.exceptionType ?? ""}
                                </div>
                              </div>
                              <div className="md:hidden">
                                <div>Message</div>
                                <div className="whitespace-normal break-all">
                                  {failure.message ?? ""}
                                </div>
                              </div>
                              {failure.stackTrace ? (
                                <pre className="text-[13px] whitespace-pre overflow-x-auto font-mono">
                                  {failure.stackTrace}
                                </pre>
                              ) : (
                                <div>No stack trace recorded.</div>
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                      )}
                    </Fragment>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        </div>
      )}

      {traceRuns.length > 1 && (
        <div ref={traceScrollRef} className="max-h-[32rem] overflow-auto rounded-md border">
          <div className="sticky top-0 z-10 flex items-center justify-between h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
            <span className="text-sm text-muted-foreground">
              Trace ({traceRuns.length} / {traceTotalCount || traceRuns.length})
            </span>
            {canLoadMoreTrace && (
              <Button
                variant="ghost"
                size="sm"
                className="text-sm font-normal text-muted-foreground"
                disabled={isLoadingMoreTrace}
                onClick={() => void fetchNextTracePage()}
              >
                {isLoadingMoreTrace ? "Loading..." : "Load more"}
              </Button>
            )}
          </div>
          <TraceView runs={traceRuns} currentRunId={id!} scrollContainerRef={traceScrollRef} />
        </div>
      )}

      {sortedStepRuns.length > 0 && (
        <div
          ref={triggeredRunsScrollRef}
          className="max-h-[32rem] overflow-auto rounded-md border"
        >
          <div className="sticky top-0 z-10 flex items-center justify-between h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
            <span className="text-sm text-muted-foreground">
              Triggered runs ({sortedStepRuns.length} /{" "}
              {childTotalCount || sortedStepRuns.length})
            </span>
            {canLoadMoreChildren && (
              <Button
                variant="ghost"
                size="sm"
                className="text-sm font-normal text-muted-foreground"
                disabled={isLoadingMoreChildren}
                onClick={() => void fetchNextChildRunsPage()}
              >
                {isLoadingMoreChildren ? "Loading..." : "Load more"}
              </Button>
            )}
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="pl-4">ID</TableHead>
                <TableHead>Job</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Started</TableHead>
                <TableHead>Duration</TableHead>
                <TableHead>Node</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              <tr style={{ height: `${triggeredRunsVirtualizer.getTotalSize()}px` }}>
                <td colSpan={6} className="p-0 relative">
                  {triggeredRunsVirtualizer.getVirtualItems().map((virtualItem) => {
                    const r = sortedStepRuns[virtualItem.index];
                    return (
                      <div
                        key={r.id}
                        className="absolute top-0 left-0 w-full flex items-center border-b border-border/50 text-sm"
                        style={{
                          height: `${virtualItem.size}px`,
                          transform: `translateY(${virtualItem.start}px)`,
                        }}
                      >
                        <div className="flex-1 grid grid-cols-6 items-center">
                          <div className="pl-4 py-2 truncate">
                            <Link
                              to={`/runs/${r.id}`}
                              className="text-primary hover:underline truncate max-w-[200px] inline-block"
                              title={r.id}
                            >
                              {r.id}
                            </Link>
                          </div>
                          <div className="py-2 truncate">
                            <Link
                              to={`/jobs/${encodeURIComponent(r.jobName)}`}
                              className="text-sm text-primary hover:underline truncate max-w-[200px] inline-block"
                              title={r.jobName}
                            >
                              {r.jobName}
                            </Link>
                          </div>
                          <div className="py-2">
                            <StatusBadge status={r.status} />
                          </div>
                          <div className="py-2 tabular-nums">
                            {r.startedAt ? formatDate(r.startedAt) : ""}
                          </div>
                          <div className="py-2 tabular-nums">
                            {formatDuration(r.startedAt, r.completedAt)}
                          </div>
                          <div className="py-2 truncate">
                            {r.nodeName ? (
                              <Link
                                to={`/nodes/${encodeURIComponent(r.nodeName)}`}
                                className="text-sm text-primary hover:underline truncate max-w-[160px] inline-block"
                                title={r.nodeName}
                              >
                                {r.nodeName}
                              </Link>
                            ) : (
                              ""
                            )}
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </td>
              </tr>
            </TableBody>
          </Table>
        </div>
      )}

      {inputItems.length > 0 && (
        <div className="flex max-h-[26rem]">
          <div
            ref={inputScrollContainerRef}
            className="flex-1 min-h-0 rounded-md border font-mono text-[13px] overflow-y-auto"
          >
            <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
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
                    className="absolute top-0 left-0 w-full px-2 py-0.5 break-all"
                    style={{
                      height: `${virtualItem.size}px`,
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
        <div className="flex max-h-[26rem]">
          <div
            ref={outputScrollContainerRef}
            className="flex-1 min-h-0 rounded-md border font-mono text-[13px] overflow-y-auto"
          >
            <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
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
                    className="absolute top-0 left-0 w-full px-2 py-0.5 break-all"
                    style={{
                      height: `${virtualItem.size}px`,
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
        <div className="flex max-h-[26rem]">
          <div
            ref={logScrollContainerRef}
            className="flex-1 min-h-0 rounded-md border font-mono text-[13px] overflow-y-auto"
          >
            <div className="sticky top-0 z-10 flex items-center gap-3 h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground font-sans">
                Logs (
                {logFilter !== null
                  ? `${filteredLogs.length} / ${logs.length}`
                  : logs.length}
                )
              </span>
              <div className="ml-auto">
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
                    className="absolute top-0 left-0 w-full px-2 py-0.5"
                    style={{
                      height: `${virtualItem.size}px`,
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
