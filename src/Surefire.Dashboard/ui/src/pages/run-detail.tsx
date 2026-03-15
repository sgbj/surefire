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
import { ScrollArea } from "@/components/ui/scroll-area";
import { formatDate, formatDuration, formatLogTime } from "@/lib/format";
import { useLiveDuration } from "@/hooks/use-live-duration";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Ban, CircleAlert, RotateCcw } from "lucide-react";
import { DtDd } from "@/components/dt-dd";
import { useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { TraceView } from "@/components/trace-view";
import { PlanGraphView } from "@/components/plan-graph-view";
import { useStickToBottom } from "@/hooks/use-stick-to-bottom";
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
  const { data: retryRuns } = useQuery({
    queryKey: ["runs", "retries", id],
    queryFn: () => api.getRuns({ retryOfRunId: id! }),
    refetchInterval: isActive ? 5000 : false,
  });
  const { data: childRuns } = useQuery({
    queryKey: ["runs", "children", id],
    queryFn: () => api.getRuns({ parentRunId: id! }),
    refetchInterval: isActive ? 5000 : false,
  });
  const { data: traceRuns } = useQuery({
    queryKey: ["run-trace", id],
    queryFn: () => api.getRunTrace(id!),
    refetchInterval: isActive ? 5000 : false,
  });
  const isPlan = !!run?.planGraph;
  const isStepRun = !!run?.planRunId && !isPlan;
  const planRunId = isPlan ? id : run?.planRunId;
  const { data: parentPlanRun } = useQuery({
    queryKey: ["run", run?.planRunId],
    queryFn: () => api.getRun(run!.planRunId!),
    enabled: isStepRun,
  });
  const { data: planStepRuns } = useQuery({
    queryKey: ["runs", "plan-steps", planRunId],
    queryFn: () => api.getAllRuns({ planRunId: planRunId! }),
    enabled: !!planRunId,
    refetchInterval: isActive || (isStepRun && (parentPlanRun?.status === 0 || parentPlanRun?.status === 1)) ? 5000 : false,
  });
  const [logs, setLogs] = useState<RunLogEntry[]>([]);
  const [logFilter, setLogFilter] = useState<number | null>(null);
  const [sseProgress, setSseProgress] = useState<number | null>(null);
  const [outputItems, setOutputItems] = useState<unknown[]>([]);
  const [inputItems, setInputItems] = useState<
    { param: string; value: unknown }[]
  >([]);
  const filteredLogs = useMemo(
    () =>
      logFilter === null ? logs : logs.filter((l) => l.level >= logFilter),
    [logs, logFilter],
  );
  const logScrollRef = useStickToBottom([filteredLogs]);
  const outputScrollRef = useStickToBottom([outputItems]);

  const cancel = useMutation({
    mutationFn: () => api.cancelRun(id!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["run", id] });
      queryClient.invalidateQueries({ queryKey: ["run-trace", id] });
      queryClient.invalidateQueries({ queryKey: ["runs", "plan-steps", id] });
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
    setLogFilter(null);
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
      } catch {
        /* ignore malformed */
      }
    });
    es.addEventListener("outputComplete", () => {
      /* stream ended */
    });
    es.addEventListener("input", (e: MessageEvent) => {
      try {
        const data = JSON.parse(e.data);
        setInputItems((prev) => [
          ...prev,
          { param: data.param, value: data.value },
        ]);
      } catch {
        /* ignore */
      }
    });
    es.addEventListener("inputComplete", () => {
      /* no special display action */
    });
    es.addEventListener("status", () => {
      queryClient.invalidateQueries({ queryKey: ["run", id] });
    });
    es.addEventListener("done", () => {
      doneReceived = true;
      es.close();
      queryClient.invalidateQueries({ queryKey: ["run", id] });
      queryClient.invalidateQueries({ queryKey: ["run-trace", id] });
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

  const sortedStepRuns = useMemo(() => {
    const items = isPlan ? planStepRuns?.items : childRuns?.items;
    if (!items?.length) return [];
    return [...items].sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime());
  }, [isPlan, planStepRuns, childRuns]);

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
          {isPlan && <Badge variant="secondary" className="shrink-0">Plan</Badge>}
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
        {run.startedAt && <DtDd label="Started">{formatDate(run.startedAt)}</DtDd>}
        {run.completedAt && <DtDd label="Completed">{formatDate(run.completedAt)}</DtDd>}
        {run.cancelledAt && (
          <DtDd label="Cancelled">{formatDate(run.cancelledAt)}</DtDd>
        )}
        {run.retryOfRunId && (
          <DtDd label="Retry of">
            <Link
              to={`/runs/${run.retryOfRunId}`}
              className="text-primary hover:underline truncate max-w-[140px] inline-block"
              title={run.retryOfRunId}
            >
              {run.retryOfRunId}
            </Link>
          </DtDd>
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
        {run.planRunId && !isPlan && (
          <DtDd label="Plan run">
            <Link
              to={`/runs/${run.planRunId}`}
              className="text-primary hover:underline truncate max-w-[140px] inline-block"
              title={run.planRunId}
            >
              {run.planRunId}
            </Link>
          </DtDd>
        )}
        {run.planStepName && !isPlan && (
          <DtDd label="Plan step">{run.planStepName}</DtDd>
        )}
      </dl>

      {isPlan && run?.planGraph && (
        <PlanGraphView planGraph={run.planGraph} stepRuns={planStepRuns?.items ?? []} planRunId={id} />
      )}

      {isStepRun && parentPlanRun?.planGraph && (
        <PlanGraphView planGraph={parentPlanRun.planGraph} stepRuns={planStepRuns?.items ?? []} highlightStepId={run.planStepId ?? undefined} planRunId={run.planRunId ?? undefined} />
      )}

      {sortedStepRuns.length > 0 && (
        <div className="max-h-[32rem] overflow-auto rounded-md border">
            <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground">{isPlan ? 'Steps' : 'Triggered runs'} ({sortedStepRuns.length})</span>
            </div>
              <Table>
                <TableHeader>
                  <TableRow>
                    {isPlan ? (
                      <TableHead className="pl-4">Step</TableHead>
                    ) : (
                      <TableHead className="pl-4">ID</TableHead>
                    )}
                    <TableHead>Job</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Started</TableHead>
                    <TableHead>Duration</TableHead>
                    <TableHead>Node</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {sortedStepRuns.map((r) => (
                    <TableRow key={r.id}>
                      <TableCell className="pl-4">
                        <Link
                          to={`/runs/${r.id}`}
                          className="text-primary hover:underline truncate max-w-[200px] inline-block"
                          title={isPlan ? (r.planStepName ?? r.id) : r.id}
                        >
                          {isPlan ? (r.planStepName ?? r.id) : r.id}
                        </Link>
                      </TableCell>
                      <TableCell>
                        <Link to={`/jobs/${encodeURIComponent(r.jobName)}`} className="text-sm text-primary hover:underline truncate max-w-[200px] inline-block" title={r.jobName}>
                          {r.jobName}
                        </Link>
                      </TableCell>
                      <TableCell>
                        <StatusBadge status={r.status} />
                      </TableCell>
                      <TableCell>
                        {r.startedAt ? formatDate(r.startedAt) : '-'}
                      </TableCell>
                      <TableCell>
                        {formatDuration(r.startedAt, r.completedAt)}
                      </TableCell>
                      <TableCell>
                        {r.nodeName ? (
                          <Link
                            to={`/nodes/${encodeURIComponent(r.nodeName)}`}
                            className="text-sm text-primary hover:underline truncate max-w-[160px] inline-block"
                            title={r.nodeName}
                          >
                            {r.nodeName}
                          </Link>
                        ) : '-'}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
        </div>
      )}

      {run.error && (
        <div className="flex max-h-[26rem]">
          <ScrollArea className="flex-1 min-h-0 rounded-md border border-destructive/15">
            <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b border-destructive/10 bg-destructive/[0.03] backdrop-blur-sm">
              <span className="text-sm text-destructive/90">Error</span>
            </div>
            <pre className="text-[13px] p-2 whitespace-pre-wrap break-all font-mono">
              {run.error}
            </pre>
          </ScrollArea>
        </div>
      )}

      {run.arguments && (
        <div className="flex max-h-[26rem]">
          <ScrollArea className="flex-1 min-h-0 rounded-md border">
            <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground">Arguments</span>
            </div>
            <pre className="text-[13px] p-2 whitespace-pre-wrap break-all font-mono">
              {formatJsonDisplay(run.arguments)}
            </pre>
          </ScrollArea>
        </div>
      )}

      {run.result && outputItems.length === 0 && (
        <div className="flex max-h-[26rem]">
          <ScrollArea className="flex-1 min-h-0 rounded-md border">
            <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground">Result</span>
            </div>
            <pre className="text-[13px] p-2 whitespace-pre-wrap break-all font-mono">
              {formatJsonDisplay(run.result)}
            </pre>
          </ScrollArea>
        </div>
      )}

      {traceRuns && traceRuns.length > 1 && !isPlan && (
        <TraceView runs={traceRuns} currentRunId={id!} />
      )}

      {sortedRetries.length > 0 && (
        <div className="max-h-[26rem] overflow-auto rounded-md border">
            <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground">Retries ({sortedRetries.length})</span>
            </div>
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
                          className="text-primary hover:underline truncate max-w-[140px] inline-block"
                          title={r.id}
                        >
                          {r.id}
                        </Link>
                      </TableCell>
                      <TableCell>
                        {r.attempt}
                      </TableCell>
                      <TableCell>
                        <StatusBadge status={r.status} />
                      </TableCell>
                      <TableCell>
                        {formatDate(r.startedAt)}
                      </TableCell>
                      <TableCell>
                        {r.nodeName ? (
                          <Link
                            to={`/nodes/${encodeURIComponent(r.nodeName)}`}
                            className="text-primary hover:underline truncate max-w-[160px] inline-block"
                            title={r.nodeName}
                          >
                            {r.nodeName}
                          </Link>
                        ) : null}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
        </div>
      )}

      {inputItems.length > 0 && (
        <div className="flex max-h-[26rem]">
          <ScrollArea className="flex-1 min-h-0 rounded-md border font-mono text-[13px]">
            <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground font-sans">{inputHeader}</span>
            </div>
            <div className="p-2">
              {inputItems.map((item, i) => (
                <div key={i} className="py-0.5 break-all">
                  <span className="text-muted-foreground">{item.param}:</span>{" "}
                  {JSON.stringify(item.value)}
                </div>
              ))}
            </div>
          </ScrollArea>
        </div>
      )}

      {outputItems.length > 0 && (
        <div className="flex max-h-[26rem]">
          <div ref={outputScrollRef} className="flex-1 min-h-0 rounded-md border font-mono text-[13px] overflow-y-auto">
            <div className="sticky top-0 z-10 flex items-center h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
              <span className="text-sm text-muted-foreground font-sans">
                Output stream ({outputItems.length} items)
              </span>
            </div>
            <div className="p-2">
              {outputItems.map((item, i) => (
                <div key={i} className="py-0.5 break-all">
                  {JSON.stringify(item)}
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {logs.length > 0 && (
        <div className="flex max-h-[26rem]">
          <div ref={logScrollRef} className="flex-1 min-h-0 rounded-md border font-mono text-[13px] overflow-y-auto">
            <div className="sticky top-0 z-10 flex items-center gap-3 h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
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
                  setLogFilter(v === "all" ? null : Number(v))
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
            <div className="p-2">
              {filteredLogs.map((log, i) => (
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
