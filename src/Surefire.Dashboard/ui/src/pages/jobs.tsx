import {useMemo, useState} from "react";
import {useQuery} from "@tanstack/react-query";
import {api} from "@/lib/api";
import {Button} from "@/components/ui/button";
import {DataTable} from "@/components/data-table";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {Input} from "@/components/ui/input";
import {Alert, AlertDescription} from "@/components/ui/alert";
import {CircleAlert, ListFilter, Search} from "lucide-react";
import {buildJobColumns} from "@/components/job-columns";

const columns = buildJobColumns();

export function JobsPage() {
  const [showInactive, setShowInactive] = useState(false);
  const hasFilters = showInactive;
  const {data: jobs, isError} = useQuery({
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
          <CircleAlert/>
          <AlertDescription>Failed to load jobs</AlertDescription>
        </Alert>
      )}
      <DataTable
        columns={columns}
        data={filtered}
        toolbar={
          <>
            <div className="relative max-w-sm">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-4 text-muted-foreground/60"/>
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
                  <ListFilter className="size-4"/>
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
