import {useMemo, useState} from "react";
import {useQuery} from "@tanstack/react-query";
import {type ColumnDef} from "@tanstack/react-table";
import {api, type NodeResponse} from "@/lib/api";
import {DataTable} from "@/components/data-table";
import {StatePill} from "@/components/status-badge";
import {Switch} from "@/components/ui/switch";
import {SortableHeader} from "@/components/sortable-header";
import {Input} from "@/components/ui/input";
import {formatRelative} from "@/lib/format";
import {Search} from "lucide-react";
import {PageShell} from "@/components/page-shell";
import {PageErrorBanner} from "@/components/page-error-banner";
import {cn} from "@/lib/utils";

const columns: ColumnDef<NodeResponse>[] = [
  {
    accessorKey: "name",
    header: ({column}) => <SortableHeader column={column}>Name</SortableHeader>,
    cell: ({row}) => (
      <span
        className={cn(
          "text-sm font-medium truncate max-w-72 inline-block",
          row.original.isActive ? "text-foreground" : "text-muted-foreground/70",
        )}
        title={row.original.name}
      >
        {row.original.name}
      </span>
    ),
  },
  {
    accessorKey: "lastHeartbeatAt",
    header: ({column}) => <SortableHeader column={column}>Last heartbeat</SortableHeader>,
    cell: ({row}) => (
      <span className={cn(
        "text-sm tnum",
        row.original.isActive ? "text-foreground/85" : "text-muted-foreground/50",
      )}>
        {formatRelative(row.original.lastHeartbeatAt)}
      </span>
    ),
  },
  {
    accessorKey: "runningCount",
    header: ({column}) => <SortableHeader column={column}>Running</SortableHeader>,
    cell: ({row}) => (
      <span className={cn(
        "text-sm tnum",
        row.original.runningCount > 0 ? "text-foreground" : "text-muted-foreground/60",
      )}>
        {row.original.runningCount}
      </span>
    ),
  },
  {
    id: "status",
    header: "State",
    cell: ({row}) =>
      row.original.isActive ? (
        <StatePill tone="success">
          <span className="inline-block size-1.5 rounded-full bg-current"/>
          Active
        </StatePill>
      ) : (
        <StatePill tone="muted">Inactive</StatePill>
      ),
  },
  {
    id: "queues",
    header: "Queues",
    cell: ({row}) => (
      <span className="text-sm text-muted-foreground truncate max-w-72 block">
        {row.original.registeredQueueNames.join(", ")}
      </span>
    ),
  },
  {
    id: "registeredJobs",
    header: "Jobs",
    cell: ({row}) => (
      <span className="text-sm tnum text-muted-foreground">
        {row.original.registeredJobNames.length}
      </span>
    ),
  },
];

export function NodesPage() {
  const [showInactive, setShowInactive] = useState(false);
  const {data: nodes, isError} = useQuery({
    queryKey: ["nodes", showInactive],
    queryFn: () => api.getNodes(showInactive ? {includeInactive: true} : undefined),
    refetchInterval: 10000,
  });
  const [filter, setFilter] = useState("");

  const filtered = useMemo(() => {
    if (!nodes) return [];
    if (!filter) return nodes;
    const lower = filter.toLowerCase();
    return nodes.filter((n) => n.name.toLowerCase().includes(lower));
  }, [nodes, filter]);

  return (
    <PageShell>
      {isError && <PageErrorBanner message="Failed to load nodes" />}

      <DataTable
        columns={columns}
        data={filtered}
        getRowHref={(r) => `/nodes/${encodeURIComponent(r.name)}`}
        getRowLinkLabel={(r) => `Open node ${r.name}`}
        toolbar={
          <>
            <div className="relative max-w-sm">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground/60"/>
              <Input
                aria-label="Search nodes"
                placeholder="Filter nodes…"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                className="pl-8"
              />
            </div>
            <label className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors cursor-pointer select-none">
              <Switch
                size="sm"
                checked={showInactive}
                onCheckedChange={setShowInactive}
              />
              Show inactive
            </label>
          </>
        }
      />
    </PageShell>
  );
}
