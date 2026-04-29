import {useMemo, useState} from "react";
import {keepPreviousData, useQuery} from "@tanstack/react-query";
import {type PaginationState} from "@tanstack/react-table";
import {api, JobStatusLabels} from "@/lib/api";
import {DataTable} from "@/components/data-table";
import {Input} from "@/components/ui/input";
import {Select, SelectContent, SelectItem, SelectTrigger, SelectValue,} from "@/components/ui/select";
import {Alert, AlertDescription} from "@/components/ui/alert";
import {CircleAlert, Search} from "lucide-react";
import {buildRunColumns} from "@/components/run-columns";
import {RUN_DATE_PRESETS} from "@/lib/run-date-presets";
import {useDebouncedValue} from "@/hooks/use-debounced-value";

const columns = buildRunColumns();

export function RunsPage() {
  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: 15,
  });
  const [jobNameInput, setJobNameInput] = useState("");
  const [statusFilter, setStatusFilter] = useState<string>("all");
  const [datePreset, setDatePreset] = useState("all");

  const debouncedJobName = useDebouncedValue(jobNameInput, 300);

  // Use datePreset (not a computed date) in the query key so the cache key is stable.
  // Compute the actual createdAfter date inside queryFn so each refetch uses a fresh timestamp.
  const queryKey = useMemo(
    () => ({
      jobNameContains: debouncedJobName || undefined,
      status: statusFilter !== "all" ? Number(statusFilter) : undefined,
      datePreset,
      skip: pagination.pageIndex * pagination.pageSize,
      take: pagination.pageSize,
    }),
    [debouncedJobName, statusFilter, datePreset, pagination],
  );

  const {data, isError} = useQuery({
    queryKey: ["runs", queryKey],
    queryFn: () => {
      const preset = RUN_DATE_PRESETS.find((p) => p.value === datePreset);
      return api.getRuns({
        jobNameContains: queryKey.jobNameContains,
        status: queryKey.status,
        createdAfter: preset?.getAfter(),
        skip: queryKey.skip,
        take: queryKey.take,
      });
    },
    refetchInterval: 5000,
    placeholderData: keepPreviousData,
  });

  const totalCount = data?.totalCount ?? 0;
  const pageCount = Math.ceil(totalCount / pagination.pageSize);

  const resetPage = () => setPagination((prev) => ({...prev, pageIndex: 0}));

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold tracking-tight">Runs</h2>
      {isError && (
        <Alert variant="destructive">
          <CircleAlert/>
          <AlertDescription>Failed to load runs</AlertDescription>
        </Alert>
      )}
      <DataTable
        columns={columns}
        data={data?.items ?? []}
        manualPagination
        pageCount={pageCount}
        totalCount={totalCount}
        pagination={pagination}
        onPaginationChange={setPagination}
        defaultPageSize={15}
        toolbar={
          <>
            <div className="relative max-w-sm">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-4 text-muted-foreground/60"/>
              <Input
                aria-label="Search runs"
                placeholder="Search..."
                value={jobNameInput}
                onChange={(e) => {
                  const next = e.target.value;
                  setJobNameInput(next);
                  if (pagination.pageIndex !== 0) {
                    setPagination((prev) => ({...prev, pageIndex: 0}));
                  }
                }}
                className="pl-8"
              />
            </div>
            <Select
              value={statusFilter}
              onValueChange={(v) => {
                setStatusFilter(v);
                resetPage();
              }}
            >
              <SelectTrigger size="sm" className="w-[140px]">
                <SelectValue/>
              </SelectTrigger>
              <SelectContent position="popper">
                <SelectItem value="all">All statuses</SelectItem>
                {Object.entries(JobStatusLabels).map(([val, label]) => (
                  <SelectItem key={val} value={val}>
                    {label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select
              value={datePreset}
              onValueChange={(v) => {
                setDatePreset(v);
                resetPage();
              }}
            >
              <SelectTrigger size="sm" className="w-[140px]">
                <SelectValue/>
              </SelectTrigger>
              <SelectContent position="popper">
                {RUN_DATE_PRESETS.map((p) => (
                  <SelectItem key={p.value} value={p.value}>
                    {p.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </>
        }
      />
    </div>
  );
}
