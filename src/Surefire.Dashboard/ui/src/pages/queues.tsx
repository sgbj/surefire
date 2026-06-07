import {useCallback, useMemo, useState} from "react";
import {useMutation, useQuery, useQueryClient} from "@tanstack/react-query";
import {type ColumnDef} from "@tanstack/react-table";
import {api, type QueueResponse} from "@/lib/api";
import {Button} from "@/components/ui/button";
import {DataTable} from "@/components/data-table";
import {StatePill} from "@/components/status-badge";
import {SortableHeader} from "@/components/sortable-header";
import {Input} from "@/components/ui/input";
import {Pause, Play, Search} from "lucide-react";
import {toast} from "sonner";
import {PageShell} from "@/components/page-shell";
import {PageErrorBanner} from "@/components/page-error-banner";
import {ToolBar} from "@/components/tab-bar";
import {cn} from "@/lib/utils";

export function QueuesPage() {
  const queryClient = useQueryClient();
  const {data: queues, isError} = useQuery({
    queryKey: ["queues"],
    queryFn: () => api.getQueues(),
    refetchInterval: 10000,
  });
  const [filter, setFilter] = useState("");

  const togglePause = useMutation({
    mutationFn: ({name, isPaused}: { name: string; isPaused: boolean }) =>
      api.updateQueue(name, {isPaused}),
    onSuccess: (_data, {isPaused}) => {
      queryClient.invalidateQueries({queryKey: ["queues"]});
      toast.success(isPaused ? "Queue paused" : "Queue resumed");
    },
    onError: () => toast.error("Failed to update queue"),
  });

  const isTogglePending = togglePause.isPending;
  const toggleQueuePause = useCallback(
    (name: string, isPaused: boolean) => {
      togglePause.mutate({name, isPaused});
    },
    [togglePause],
  );

  const columns: ColumnDef<QueueResponse>[] = useMemo(
    () => [
      {
        accessorKey: "name",
        header: ({column}) => (
          <SortableHeader column={column}>Name</SortableHeader>
        ),
        cell: ({row}) => (
          <span className="text-sm font-medium text-foreground">
            {row.original.name}
          </span>
        ),
      },
      {
        accessorKey: "priority",
        header: ({column}) => (
          <SortableHeader column={column}>Priority</SortableHeader>
        ),
        cell: ({row}) => (
          <span className="text-sm tnum text-foreground/85">
            {row.original.priority}
          </span>
        ),
      },
      {
        accessorKey: "pendingCount",
        header: ({column}) => (
          <SortableHeader column={column}>Pending</SortableHeader>
        ),
        cell: ({row}) => (
          <span
            className={cn(
              "text-sm tnum",
              row.original.pendingCount > 0
                ? "text-foreground font-medium"
                : "text-muted-foreground/60",
            )}
          >
            {row.original.pendingCount}
          </span>
        ),
      },
      {
        accessorKey: "runningCount",
        header: ({column}) => (
          <SortableHeader column={column}>Running</SortableHeader>
        ),
        cell: ({row}) => (
          <span
            className={cn(
              "text-sm tnum",
              row.original.runningCount > 0 ? "text-foreground" : "text-muted-foreground/60",
            )}
          >
            {row.original.runningCount}
          </span>
        ),
      },
      {
        id: "maxConcurrency",
        header: "Concurrency",
        cell: ({row}) => (
          <span className="text-sm tnum text-muted-foreground">
            {row.original.maxConcurrency != null
              ? `${row.original.runningCount} / ${row.original.maxConcurrency}`
              : "Unlimited"}
          </span>
        ),
      },
      {
        id: "status",
        header: "State",
        cell: ({row}) =>
          row.original.isPaused ? (
            <StatePill tone="warning">
              <span className="inline-block size-1.5 rounded-full bg-current"/>
              Paused
            </StatePill>
          ) : (
            <StatePill tone="success">
              <span className="inline-block size-1.5 rounded-full bg-current"/>
              Active
            </StatePill>
          ),
      },
      {
        id: "nodes",
        header: "Nodes",
        cell: ({row}) => (
          <span className="text-sm tnum text-muted-foreground">
            {row.original.processingNodes.length}
          </span>
        ),
      },
      {
        id: "actions",
        cell: ({row}) => (
          <div className="flex justify-end">
            <Button
              variant="ghost"
              size="icon"
              className="size-8"
              aria-label={
                row.original.isPaused ? "Resume queue" : "Pause queue"
              }
              onClick={() =>
                toggleQueuePause(row.original.name, !row.original.isPaused)
              }
              disabled={isTogglePending}
            >
              {row.original.isPaused ? (
                <Play className="size-3.5"/>
              ) : (
                <Pause className="size-3.5"/>
              )}
            </Button>
          </div>
        ),
      },
    ],
    [isTogglePending, toggleQueuePause],
  );

  const filtered = useMemo(() => {
    if (!queues) return [];
    if (!filter) return queues;
    const lower = filter.toLowerCase();
    return queues.filter((q) => q.name.toLowerCase().includes(lower));
  }, [queues, filter]);

  return (
    <PageShell>
      {isError && <PageErrorBanner message="Failed to load queues" />}

      <ToolBar>
        <div className="relative max-w-sm">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground/60"/>
          <Input
            placeholder="Filter queues…"
            aria-label="Search queues"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="h-8 pl-8"
          />
        </div>
      </ToolBar>

      <DataTable
        columns={columns}
        data={filtered}
      />
    </PageShell>
  );
}
