import {useMemo, useState} from "react";
import {keepPreviousData, useQuery} from "@tanstack/react-query";
import {type PaginationState} from "@tanstack/react-table";
import {api, JobStatusLabels} from "@/lib/api";
import {DataTable} from "@/components/data-table";
import {Input} from "@/components/ui/input";
import {Select, SelectContent, SelectItem, SelectTrigger, SelectValue,} from "@/components/ui/select";
import {Search} from "lucide-react";
import {buildRunColumns} from "@/components/run-columns";
import {RUN_DATE_PRESETS} from "@/lib/run-date-presets";
import {useDebouncedValue} from "@/hooks/use-debounced-value";
import {PageShell} from "@/components/page-shell";
import {PageErrorBanner} from "@/components/page-error-banner";
import {ToolBar} from "@/components/tab-bar";

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
    <PageShell>
      {isError && <PageErrorBanner message="Failed to load runs" />}

      <ToolBar>
        <div className="relative max-w-sm">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground/60"/>
          <Input
            aria-label="Search runs"
            placeholder="Filter runs…"
            value={jobNameInput}
            onChange={(e) => {
              const next = e.target.value;
              setJobNameInput(next);
              if (pagination.pageIndex !== 0) {
                setPagination((prev) => ({...prev, pageIndex: 0}));
              }
            }}
            className="h-8 pl-8"
          />
        </div>
        <Select
          value={statusFilter}
          onValueChange={(v) => {
            setStatusFilter(v);
            resetPage();
          }}
        >
          <SelectTrigger size="sm" className="ml-auto w-36">
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
          <SelectTrigger size="sm" className="w-36">
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
      </ToolBar>

      <DataTable
        columns={columns}
        data={data?.items ?? []}
        manualPagination
        pageCount={pageCount}
        totalCount={totalCount}
        pagination={pagination}
        onPaginationChange={setPagination}
        defaultPageSize={15}
        getRowHref={(r) => `/runs/${r.id}`}
        getRowLinkLabel={(r) => `Open run ${r.id}`}
      />
    </PageShell>
  );
}
