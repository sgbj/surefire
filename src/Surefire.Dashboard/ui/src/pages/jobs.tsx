import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { type ColumnDef } from "@tanstack/react-table";
import { api, type JobResponse } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { DataTable } from "@/components/data-table";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { SortableHeader } from "@/components/sortable-header";
import { Input } from "@/components/ui/input";
import { Link } from "react-router";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { CircleAlert, ListFilter, Search } from "lucide-react";

const columns: ColumnDef<JobResponse>[] = [
  {
    accessorKey: "name",
    header: ({ column }) => (
      <SortableHeader column={column}>Name</SortableHeader>
    ),
    cell: ({ row }) => (
      <Link
        to={`/jobs/${encodeURIComponent(row.original.name)}`}
        className={`font-medium text-primary hover:underline truncate max-w-[200px] inline-block ${!row.original.isActive ? "opacity-50" : ""}`}
        title={row.original.name}
      >
        {row.original.name}
      </Link>
    ),
  },
  {
    accessorKey: "description",
    header: "Description",
    cell: ({ row }) => (
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
    cell: ({ row }) => (
      <span className={`text-sm ${!row.original.isActive ? "opacity-50" : ""}`}>
        {row.original.isContinuous
          ? "Continuous"
          : (row.original.cronExpression ?? "Manual")}
      </span>
    ),
  },
  {
    accessorKey: "queue",
    header: "Queue",
    cell: ({ row }) => (
      <span className={`text-sm ${!row.original.isActive ? "opacity-50" : ""}`}>
        {row.original.queue ?? "default"}
      </span>
    ),
  },
  {
    accessorKey: "isEnabled",
    header: "Status",
    cell: ({ row }) => {
      if (!row.original.isActive)
        return (
          <Badge variant="outline" className="text-muted-foreground opacity-50">
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
    cell: ({ row }) => (
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

export function JobsPage() {
  const [showInactive, setShowInactive] = useState(false);
  const hasFilters = showInactive;
  const { data: jobs, isError } = useQuery({
    queryKey: ["jobs", showInactive],
    queryFn: () =>
      api.getJobs({
        includeInactive: showInactive || undefined,
      }),
  });
  const [filter, setFilter] = useState("");

  const filtered = useMemo(() => {
    if (!jobs) return [];
    if (!filter) return jobs;
    const lower = filter.toLowerCase();
    return jobs.filter(
      (j) =>
        j.name.toLowerCase().includes(lower) ||
        j.description?.toLowerCase().includes(lower) ||
        j.tags.some((t) => t.toLowerCase().includes(lower)),
    );
  }, [jobs, filter]);

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold tracking-tight">Jobs</h2>
      {isError && (
        <Alert variant="destructive">
          <CircleAlert />
          <AlertDescription>Failed to load jobs</AlertDescription>
        </Alert>
      )}
      <DataTable
        columns={columns}
        data={filtered}
        toolbar={
          <>
            <div className="relative max-w-sm">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-4 text-muted-foreground/60" />
              <Input
                aria-label="Search jobs"
                placeholder="Search..."
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                className="pl-8"
              />
            </div>
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button
                  variant="outline"
                  size="sm"
                  className={hasFilters ? "border-primary/50" : ""}
                >
                  <ListFilter className="size-4" />
                  Filter
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuCheckboxItem
                  checked={showInactive}
                  onCheckedChange={setShowInactive}
                >
                  Inactive jobs
                </DropdownMenuCheckboxItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </>
        }
      />
    </div>
  );
}
