import {useMemo, useState} from "react";
import {useQuery} from "@tanstack/react-query";
import {api} from "@/lib/api";
import {DataTable} from "@/components/data-table";
import {Switch} from "@/components/ui/switch";
import {Input} from "@/components/ui/input";
import {Alert, AlertDescription} from "@/components/ui/alert";
import {CircleAlert, Search} from "lucide-react";
import {buildJobColumns} from "@/components/job-columns";
import {PageShell} from "@/components/page-shell";

const columns = buildJobColumns();

export function JobsPage() {
  const [showInactive, setShowInactive] = useState(false);
  const {data: jobs, isError} = useQuery({
    queryKey: ["jobs", showInactive],
    queryFn: () =>
      api.getJobs({includeInactive: showInactive || undefined}),
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
    <PageShell>
      {isError && (
        <div className="px-6 pt-5">
          <Alert variant="destructive">
            <CircleAlert/>
            <AlertDescription>Failed to load jobs</AlertDescription>
          </Alert>
        </div>
      )}

      <DataTable
        columns={columns}
        data={filtered}
        getRowHref={(r) => `/jobs/${encodeURIComponent(r.name)}`}
        getRowLinkLabel={(r) => `Open job ${r.name}`}
        toolbar={
          <>
            <div className="relative max-w-sm">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground/60"/>
              <Input
                aria-label="Search jobs"
                placeholder="Filter by name, tag, description…"
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
