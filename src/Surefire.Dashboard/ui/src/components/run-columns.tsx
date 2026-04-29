import {type ColumnDef} from "@tanstack/react-table";
import {Link} from "react-router";

import {type JobRun} from "@/lib/api";
import {formatDate} from "@/lib/format";
import {LiveDuration} from "@/components/live-duration";
import {StatusBadge} from "@/components/status-badge";

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
        <Link
          to={`/runs/${row.original.id}`}
          className="text-sm text-primary hover:underline truncate max-w-[140px] inline-block"
          title={row.original.id}
        >
          {row.original.id}
        </Link>
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
          className="text-sm text-primary hover:underline truncate max-w-[200px] inline-block"
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
      header: "Created",
      cell: ({row}) => (
        <span className="text-sm">{formatDate(row.original.createdAt)}</span>
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
            className="text-sm text-primary hover:underline truncate max-w-[160px] inline-block"
            title={row.original.nodeName}
          >
            {row.original.nodeName}
          </Link>
        ) : null,
    });
  }

  if (showStarted) {
    columns.push({
      accessorKey: "startedAt",
      header: "Started",
      cell: ({row}) => (
        <span className="text-sm">{formatDate(row.original.startedAt)}</span>
      ),
    });
  }

  if (showAttempt) {
    columns.push({
      accessorKey: "attempt",
      header: "Attempt",
    });
  }

  return columns;
}
