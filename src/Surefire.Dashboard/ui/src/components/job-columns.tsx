import {type ColumnDef} from "@tanstack/react-table";

import {type JobResponse} from "@/lib/api";
import {SortableHeader} from "@/components/sortable-header";
import {StatePill} from "@/components/status-badge";
import {cn} from "@/lib/utils";

export function buildJobColumns(): ColumnDef<JobResponse>[] {
  return [
    {
      accessorKey: "name",
      header: ({column}) => (
        <SortableHeader column={column}>Name</SortableHeader>
      ),
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
      accessorKey: "description",
      header: "Description",
      cell: ({row}) => (
        <span
          className={cn(
            "text-sm",
            row.original.isActive
              ? "text-muted-foreground"
              : "text-muted-foreground/50",
          )}
        >
          {row.original.description ?? ""}
        </span>
      ),
    },
    {
      accessorKey: "cronExpression",
      header: "Schedule",
      cell: ({row}) => {
        const text = row.original.isContinuous
          ? "Continuous"
          : (row.original.cronExpression ?? "Manual");
        return (
          <span
            className={cn(
              row.original.cronExpression ? "font-mono text-xs" : "text-sm",
              "text-foreground/85",
            )}
          >
            {text}
          </span>
        );
      },
    },
    {
      accessorKey: "queue",
      header: "Queue",
      cell: ({row}) => (
        <span className="text-sm text-muted-foreground">
          {row.original.queue ?? "default"}
        </span>
      ),
    },
    {
      accessorKey: "isEnabled",
      header: "State",
      cell: ({row}) => {
        if (!row.original.isActive) {
          return <StatePill tone="muted">Inactive</StatePill>;
        }
        return row.original.isEnabled ? (
          <StatePill tone="success">
            <span className="inline-block size-1.5 rounded-full bg-current"/>
            Enabled
          </StatePill>
        ) : (
          <StatePill tone="muted">Disabled</StatePill>
        );
      },
    },
    {
      accessorKey: "tags",
      header: "Tags",
      cell: ({row}) => (
        <div className="flex flex-wrap gap-1">
          {row.original.tags.map((tag) => (
            <span
              key={tag}
              className="inline-flex items-center h-5 px-1.5 rounded-sm border border-border bg-muted/40 text-[11px] text-muted-foreground"
            >
              {tag}
            </span>
          ))}
        </div>
      ),
    },
  ];
}
