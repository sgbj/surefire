import {type ColumnDef} from "@tanstack/react-table";
import {Link} from "react-router";

import {type JobResponse} from "@/lib/api";
import {Badge} from "@/components/ui/badge";
import {SortableHeader} from "@/components/sortable-header";

export function buildJobColumns(): ColumnDef<JobResponse>[] {
  return [
    {
      accessorKey: "name",
      header: ({column}) => (
        <SortableHeader column={column}>Name</SortableHeader>
      ),
      cell: ({row}) => (
        <Link
          to={`/jobs/${encodeURIComponent(row.original.name)}`}
          className={`font-medium text-primary hover:underline truncate max-w-50 inline-block ${!row.original.isActive ? "opacity-50" : ""}`}
          title={row.original.name}
        >
          {row.original.name}
        </Link>
      ),
    },
    {
      accessorKey: "description",
      header: "Description",
      cell: ({row}) => (
        <span
          className={`text-muted-foreground ${!row.original.isActive ? "opacity-50" : ""}`}
        >
          {row.original.description}
        </span>
      ),
    },
    {
      accessorKey: "cronExpression",
      header: "Schedule",
      cell: ({row}) => (
        <span
          className={`text-sm ${!row.original.isActive ? "opacity-50" : ""}`}
        >
          {row.original.isContinuous
            ? "Continuous"
            : (row.original.cronExpression ?? "Manual")}
        </span>
      ),
    },
    {
      accessorKey: "queue",
      header: "Queue",
      cell: ({row}) => (
        <span
          className={`text-sm ${!row.original.isActive ? "opacity-50" : ""}`}
        >
          {row.original.queue ?? "default"}
        </span>
      ),
    },
    {
      accessorKey: "isEnabled",
      header: "Status",
      cell: ({row}) => {
        if (!row.original.isActive)
          return (
            <Badge
              variant="outline"
              className="text-muted-foreground opacity-50"
            >
              Inactive
            </Badge>
          );
        return row.original.isEnabled ? (
          <Badge
            variant="outline"
            className="text-emerald-700 dark:text-emerald-400"
          >
            Enabled
          </Badge>
        ) : (
          <Badge variant="outline" className="text-muted-foreground">
            Disabled
          </Badge>
        );
      },
    },
    {
      accessorKey: "tags",
      header: "Tags",
      cell: ({row}) => (
        <div
          className={`flex gap-1 ${!row.original.isActive ? "opacity-50" : ""}`}
        >
          {row.original.tags.map((tag) => (
            <Badge key={tag} variant="outline">
              {tag}
            </Badge>
          ))}
        </div>
      ),
    },
  ];
}
