import {type ColumnDef} from "@tanstack/react-table";
import {Link} from "react-router";

import {type JobRun} from "@/lib/api";
import {formatDate} from "@/lib/format";
import {LiveDuration} from "@/components/live-duration";
import {StatusBadge} from "@/components/status-badge";
import {SortableHeader} from "@/components/sortable-header";

export interface RunColumnOptions {
  showJob?: boolean;
  showNode?: boolean;
  showStarted?: boolean;
  showAttempt?: boolean;
}

export function buildRunColumns(
  options: RunColumnOptions = {},
): ColumnDef<JobRun>[] {
  const {
    showJob = true,
    showNode = true,
    showStarted = false,
    showAttempt = false,
  } = options;

  const columns: ColumnDef<JobRun>[] = [
    {
      accessorKey: "id",
      header: "ID",
      cell: ({row}) => (
        <span
          className="font-mono text-xs text-foreground/85 truncate max-w-35 inline-block"
          title={row.original.id}
        >
          {row.original.id}
        </span>
      ),
    },
  ];

  if (showJob) {
    columns.push({
      accessorKey: "jobName",
      header: "Job",
      cell: ({row}) => (
        <Link
          to={`/jobs/${encodeURIComponent(row.original.jobName)}`}
          className="relative text-sm font-medium text-foreground hover:text-accent-brand transition-colors truncate max-w-50 inline-block"
          title={row.original.jobName}
        >
          {row.original.jobName}
        </Link>
      ),
    });
  }

  columns.push(
    {
      accessorKey: "status",
      header: "Status",
      cell: ({row}) => <StatusBadge status={row.original.status}/>,
    },
    {
      accessorKey: "createdAt",
      header: ({column}) => <SortableHeader column={column}>Created</SortableHeader>,
      cell: ({row}) => (
        <span className="font-mono text-xs tnum text-muted-foreground">
          {formatDate(row.original.createdAt)}
        </span>
      ),
    },
    {
      id: "duration",
      header: "Duration",
      cell: ({row}) => (
        <LiveDuration
          startedAt={row.original.startedAt}
          completedAt={row.original.completedAt}
        />
      ),
    },
  );

  if (showNode) {
    columns.push({
      accessorKey: "nodeName",
      header: "Node",
      cell: ({row}) =>
        row.original.nodeName ? (
          <Link
            to={`/nodes/${encodeURIComponent(row.original.nodeName)}`}
            className="relative font-mono text-xs text-muted-foreground hover:text-foreground transition-colors truncate max-w-40 inline-block"
            title={row.original.nodeName}
          >
            {row.original.nodeName}
          </Link>
        ) : (
          null
        ),
    });
  }

  if (showStarted) {
    columns.push({
      accessorKey: "startedAt",
      header: "Started",
      cell: ({row}) => (
        <span className="font-mono text-xs tnum text-muted-foreground">
          {formatDate(row.original.startedAt)}
        </span>
      ),
    });
  }

  if (showAttempt) {
    columns.push({
      accessorKey: "attempt",
      header: "Attempt",
      cell: ({row}) => (
        <span className="font-mono text-xs tnum text-foreground/85">
          {row.original.attempt}
        </span>
      ),
    });
  }

  return columns;
}
