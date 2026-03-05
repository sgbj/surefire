import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useParams, useNavigate, Link } from "react-router";
import { api, type RunLogEntry, LogLevelLabels } from "@/lib/api";
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
import { ScrollArea } from "@/components/ui/scroll-area";
import { formatDate, formatLogTime } from "@/lib/format";
import { Spinner } from "@/components/ui/spinner";
import { useLiveDuration } from "@/hooks/use-live-duration";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Ban, CircleAlert, RotateCcw } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { toast } from "sonner";
import { DtDd } from "@/components/dt-dd";

function formatJsonDisplay(json: string): string {
  try {
    return JSON.stringify(JSON.parse(json), null, 2);
  } catch {
    return json;
  }
}

export function RunDetailPage() {
  const { id } = useParams();
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
  const { data: childRuns } = useQuery({
    queryKey: ["runs", "children", id],
    queryFn: () => api.getRuns({ parentRunId: id! }),
    refetchInterval: isActive ? 5000 : false,
  });
  const { data: retryRuns } = useQuery({
    queryKey: ["runs", "retries", id],
    queryFn: () => api.getRuns({ retryOfRunId: id! }),
    refetchInterval: isActive ? 5000 : false,
  });
  const [logs, setLogs] = useState<RunLogEntry[]>([]);
  const [sseProgress, setSseProgress] = useState<number | null>(null);
  const [outputItems, setOutputItems] = useState<unknown[]>([]);
  const [inputItems, setInputItems] = useState<{ param: string; value: unknown }[]>([]);
  const logViewportRef = useRef<HTMLElement | null>(null);
  const logWrapperRef = useRef<HTMLDivElement>(null);
  const outputWrapperRef = useRef<HTMLDivElement>(null);
  const outputViewportRef = useRef<HTMLElement | null>(null);
  const isAtBottom = useRef(true);
  const isOutputAtBottom = useRef(true);
  const programmaticScroll = useRef(false);
  const programmaticOutputScroll = useRef(false);

  const cancel = useMutation({
    mutationFn: () => api.cancelRun(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["run", id] });
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

  useEffect(() => {
    if (!id) return;
    setLogs([]);
    setSseProgress(null);
    setOutputItems([]);
    setInputItems([]);
    let stale = false;
    let doneReceived = false;
    const es = api.streamRun(id);
    es.onmessage = (e) => {
      try {
        const entry: RunLogEntry = JSON.parse(e.data);
        if (entry?.timestamp) setLogs((prev) => [...prev, entry]);
      } catch {
        /* ignore malformed messages */
      }
    };
    es.addEventListener("progress", (e: MessageEvent) => {
      try {
        const data = JSON.parse(e.data);
        setSseProgress(data.value ?? Number(e.data));
      } catch {
        setSseProgress(Number(e.data));
      }
    });
    es.addEventListener("output", (e: MessageEvent) => {
      try {
        const item = JSON.parse(e.data);
        setOutputItems((prev) => [...prev, item]);
      } catch { /* ignore malformed */ }
    });
    es.addEventListener("outputComplete", () => { /* stream ended */ });
    es.addEventListener("input", (e: MessageEvent) => {
      try {
        const data = JSON.parse(e.data);
        setInputItems((prev) => [...prev, { param: data.param, value: data.value }]);
      } catch { /* ignore */ }
    });
    es.addEventListener("inputComplete", () => { /* no special display action */ });
    es.addEventListener("status", () => {
      queryClient.invalidateQueries({ queryKey: ["run", id] });
    });
    es.addEventListener("done", () => {
      doneReceived = true;
      es.close();
      queryClient.invalidateQueries({ queryKey: ["run", id] });
      queryClient.invalidateQueries({ queryKey: ["runs", "children", id] });
      queryClient.invalidateQueries({ queryKey: ["runs", "retries", id] });
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
            if (!stale && fetched.length > 0) setLogs(fetched);
          })
          .catch(() => {});
      }
    };
    return () => {
      stale = true;
      es.close();
    };
  }, [id, queryClient]);

  const handleLogScroll = useCallback((e: Event) => {
    if (programmaticScroll.current) return;
    const el = e.target as HTMLElement;
    isAtBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 30;
  }, []);

  const handleOutputScroll = useCallback((e: Event) => {
    if (programmaticOutputScroll.current) return;
    const el = e.target as HTMLElement;
    isOutputAtBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 30;
  }, []);

  const hasRun = !!run;
  useEffect(() => {
    const viewport = logWrapperRef.current?.querySelector<HTMLElement>(
      '[data-slot="scroll-area-viewport"]',
    );
    if (!viewport) return;
    logViewportRef.current = viewport;
    viewport.addEventListener("scroll", handleLogScroll, { passive: true });
    return () => viewport.removeEventListener("scroll", handleLogScroll);
  }, [hasRun, handleLogScroll]);

  const hasOutputItems = outputItems.length > 0;
  useEffect(() => {
    const viewport = outputWrapperRef.current?.querySelector<HTMLElement>(
      '[data-slot="scroll-area-viewport"]',
    );
    if (!viewport) return;
    outputViewportRef.current = viewport;
    viewport.addEventListener("scroll", handleOutputScroll, { passive: true });
    return () => viewport.removeEventListener("scroll", handleOutputScroll);
  }, [hasOutputItems, handleOutputScroll]);

  useEffect(() => {
    const viewport = logViewportRef.current;
    if (isAtBottom.current && viewport) {
      programmaticScroll.current = true;
      viewport.scrollTop = viewport.scrollHeight;
      requestAnimationFrame(() => {
        programmaticScroll.current = false;
      });
    }
  }, [logs]);

  useEffect(() => {
    const viewport = outputViewportRef.current;
    if (isOutputAtBottom.current && viewport) {
      programmaticOutputScroll.current = true;
      viewport.scrollTop = viewport.scrollHeight;
      requestAnimationFrame(() => {
        programmaticOutputScroll.current = false;
      });
    }
  }, [outputItems]);

  const inputHeader = useMemo(() => {
    if (inputItems.length === 0) return "";
    const counts = new Map<string, number>();
    for (const item of inputItems) {
      counts.set(item.param, (counts.get(item.param) ?? 0) + 1);
    }
    if (counts.size <= 1) return `Input stream (${inputItems.length} items)`;
    return `Input stream (${[...counts.entries()].map(([k, v]) => `${k}: ${v}`).join(", ")})`;
  }, [inputItems]);

  const sortedRetries = useMemo(() => {
    if (!retryRuns?.items.length) return [];
    return [...retryRuns.items].sort((a, b) => a.attempt - b.attempt);
  }, [retryRuns]);

  const sortedChildRuns = useMemo(() => {
    if (!childRuns?.items.length) return [];
    return [...childRuns.items].sort(
      (a, b) =>
        new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime(),
    );
  }, [childRuns]);

  const duration = useLiveDuration(run?.startedAt, run?.completedAt);
  const progress = sseProgress ?? run?.progress ?? 0;

  if (isError)
    return (
      <Alert variant="destructive">
        <CircleAlert />
        <AlertDescription>Failed to load run</AlertDescription>
      </Alert>
    );
  if (!run)
    return (
      <div className="flex items-center gap-2 text-muted-foreground">
        <Spinner className="size-4" />
        Loading...
      </div>
    );

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-4">
        <div className="flex items-center gap-3 min-w-0">
          <h2 className="text-2xl font-bold tracking-tight truncate">
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
          {!isActive && (
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
          <Progress
            value={progress > 0 ? progress * 100 : null}
            className="h-1.5"
          />
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
            className="text-primary hover:underline"
          >
            {run.jobName}
          </Link>
        </DtDd>
        <DtDd label="Duration">
          <span className="tabular-nums">{duration}</span>
        </DtDd>
        {run.attempt > 1 && <DtDd label="Attempt">{run.attempt}</DtDd>}
        <DtDd label="Node">
          {run.nodeName ? (
            <Link
              to={`/nodes/${encodeURIComponent(run.nodeName)}`}
              className="text-primary hover:underline"
            >
              {run.nodeName}
            </Link>
          ) : (
            "-"
          )}
        </DtDd>
        <DtDd label="Created">{formatDate(run.createdAt)}</DtDd>
        {run.notBefore && run.notBefore !== run.createdAt && (
          <DtDd label="Not before">{formatDate(run.notBefore)}</DtDd>
        )}
        <DtDd label="Started">{formatDate(run.startedAt)}</DtDd>
        <DtDd label="Completed">{formatDate(run.completedAt)}</DtDd>
        {run.cancelledAt && (
          <DtDd label="Cancelled">{formatDate(run.cancelledAt)}</DtDd>
        )}
        {run.retryOfRunId && (
          <DtDd label="Retry of">
            <Link
              to={`/runs/${run.retryOfRunId}`}
              className="text-primary hover:underline"
            >
              {run.retryOfRunId}
            </Link>
          </DtDd>
        )}
        {run.rerunOfRunId && (
          <DtDd label="Rerun of">
            <Link
              to={`/runs/${run.rerunOfRunId}`}
              className="text-primary hover:underline"
            >
              {run.rerunOfRunId}
            </Link>
          </DtDd>
        )}
        {run.parentRunId && (
          <DtDd label="Triggered by">
            <Link
              to={`/runs/${run.parentRunId}`}
              className="text-primary hover:underline"
            >
              {run.parentRunId}
            </Link>
          </DtDd>
        )}
      </dl>

      {run.arguments && (
        <div>
          <h3 className="text-sm text-muted-foreground mb-1">Arguments</h3>
          <pre className="text-xs bg-muted/30 p-2 rounded whitespace-pre-wrap break-all font-mono">
            {formatJsonDisplay(run.arguments)}
          </pre>
        </div>
      )}

      {run.result && outputItems.length === 0 && (
        <div>
          <h3 className="text-sm text-muted-foreground mb-1">Result</h3>
          <pre className="text-xs bg-muted/30 p-2 rounded whitespace-pre-wrap break-all font-mono">
            {formatJsonDisplay(run.result)}
          </pre>
        </div>
      )}

      {run.error && (
        <div>
          <h3 className="text-sm text-muted-foreground mb-1">Error</h3>
          <pre className="text-xs bg-muted/30 p-2 rounded overflow-x-auto font-mono">
            {run.error}
          </pre>
        </div>
      )}

      {sortedRetries.length > 0 && (
        <div>
          <h3 className="text-sm text-muted-foreground mb-1">
            Retries ({sortedRetries.length})
          </h3>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-4">ID</TableHead>
                  <TableHead>Attempt</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Started</TableHead>
                  <TableHead>Node</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedRetries.map((r) => (
                  <TableRow key={r.id}>
                    <TableCell className="pl-4">
                      <Link
                        to={`/runs/${r.id}`}
                        className="text-sm text-primary hover:underline"
                      >
                        {r.id}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <span className="text-sm">{r.attempt}</span>
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={r.status} />
                    </TableCell>
                    <TableCell>
                      <span className="text-sm">{formatDate(r.startedAt)}</span>
                    </TableCell>
                    <TableCell>
                      {r.nodeName ? (
                        <Link
                          to={`/nodes/${encodeURIComponent(r.nodeName)}`}
                          className="text-sm text-primary hover:underline"
                        >
                          {r.nodeName}
                        </Link>
                      ) : (
                        <span className="text-sm">-</span>
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </div>
      )}

      {sortedChildRuns.length > 0 && (
        <div>
          <h3 className="text-sm text-muted-foreground mb-1">
            Triggered runs ({sortedChildRuns.length})
          </h3>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="pl-4">ID</TableHead>
                  <TableHead>Job</TableHead>
                  <TableHead>Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedChildRuns.map((child) => (
                  <TableRow key={child.id}>
                    <TableCell className="pl-4">
                      <Link
                        to={`/runs/${child.id}`}
                        className="text-sm text-primary hover:underline"
                      >
                        {child.id}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <Link
                        to={`/jobs/${encodeURIComponent(child.jobName)}`}
                        className="text-sm text-primary hover:underline"
                      >
                        {child.jobName}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={child.status} />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </div>
      )}

      {inputItems.length > 0 && (
        <div>
          <h3 className="text-sm text-muted-foreground mb-1">{inputHeader}</h3>
          <ScrollArea className="h-[26rem] rounded-md bg-muted/30 p-2 font-mono text-xs">
            {inputItems.map((item, i) => (
              <div key={i} className="py-0.5">
                <span className="text-muted-foreground">{item.param}:</span>{" "}
                {typeof item.value === "object"
                  ? JSON.stringify(item.value)
                  : String(item.value)}
              </div>
            ))}
          </ScrollArea>
        </div>
      )}

      {outputItems.length > 0 && (
        <div>
          <h3 className="text-sm text-muted-foreground mb-1">
            Output stream ({outputItems.length} items)
          </h3>
          <div ref={outputWrapperRef}>
            <ScrollArea className="h-[26rem] rounded-md bg-muted/30 p-2 font-mono text-xs">
              {outputItems.map((item, i) => (
                <div key={i} className="py-0.5">
                  {typeof item === "object" ? JSON.stringify(item) : String(item)}
                </div>
              ))}
            </ScrollArea>
          </div>
        </div>
      )}

      {logs.length > 0 && (
        <div>
          <h3 className="text-sm text-muted-foreground mb-1">
            Logs ({logs.length})
          </h3>
          <div ref={logWrapperRef}>
            <ScrollArea className="h-[26rem] rounded-md bg-muted/30 p-2 font-mono text-xs">
              {logs.map((log, i) => (
                <div key={i} className="py-0.5">
                  <span className="text-muted-foreground tabular-nums">
                    {formatLogTime(log.timestamp)}
                  </span>{" "}
                  <span className={logLevelColor(log.level)}>
                    [{LogLevelLabels[log.level] ?? "?"}]
                  </span>{" "}
                  <span>{log.message}</span>
                </div>
              ))}
            </ScrollArea>
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
