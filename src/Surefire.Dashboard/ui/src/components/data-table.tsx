import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  type PaginationState,
  type SortingState,
  useReactTable,
  type VisibilityState,
} from "@tanstack/react-table";
import {type Dispatch, type SetStateAction, useState} from "react";
import {Link} from "react-router";
import {ChevronLeft, ChevronRight, Settings2} from "lucide-react";

import {cn} from "@/lib/utils";
import {Button} from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {Select, SelectContent, SelectItem, SelectTrigger, SelectValue,} from "@/components/ui/select";
import {Table, TableBody, TableCell, TableHead, TableHeader, TableRow,} from "@/components/ui/table";

interface DataTableBaseProps<TData, TValue> {
  columns: ColumnDef<TData, TValue>[];
  data: TData[];
  toolbar?: React.ReactNode;
  header?: React.ReactNode;
  showColumnVisibility?: boolean;
  defaultPageSize?: number;
  getRowHref?: (row: TData) => string | undefined;
  getRowLinkLabel?: (row: TData) => string;
}

interface DataTableClientProps<TData, TValue> extends DataTableBaseProps<TData, TValue> {
  manualPagination?: false;
  pageCount?: never;
  totalCount?: never;
  pagination?: never;
  onPaginationChange?: never;
}

interface DataTableServerProps<TData, TValue> extends DataTableBaseProps<TData, TValue> {
  manualPagination: true;
  pageCount: number;
  totalCount: number;
  pagination: PaginationState;
  onPaginationChange: Dispatch<SetStateAction<PaginationState>>;
}

type DataTableProps<TData, TValue> =
  | DataTableClientProps<TData, TValue>
  | DataTableServerProps<TData, TValue>;

export function DataTable<TData, TValue>({
                                           columns,
                                           data,
                                           toolbar,
                                           header,
                                           showColumnVisibility = false,
                                           defaultPageSize = 15,
                                           getRowHref,
                                           getRowLinkLabel,
                                           ...rest
                                         }: DataTableProps<TData, TValue>) {
  const isServer = rest.manualPagination === true;

  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({});
  const [clientPagination, setClientPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: defaultPageSize,
  });

  const pagination = isServer ? rest.pagination : clientPagination;
  const onPaginationChange = isServer
    ? rest.onPaginationChange
    : setClientPagination;

  // eslint-disable-next-line react-hooks/incompatible-library -- useReactTable manages its own state; React Compiler memoization is unnecessary.
  const table = useReactTable({
    data,
    columns,
    defaultColumn: {enableSorting: !isServer},
    state: {sorting, columnVisibility, pagination},
    ...(!isServer && {
      onSortingChange: (
        updater: SortingState | ((prev: SortingState) => SortingState),
      ) => {
        setSorting(updater);
        onPaginationChange((prev: PaginationState) => ({...prev, pageIndex: 0}));
      },
    }),
    onColumnVisibilityChange: setColumnVisibility,
    onPaginationChange,
    getCoreRowModel: getCoreRowModel(),
    ...(!isServer && {getSortedRowModel: getSortedRowModel()}),
    ...(!isServer && {getPaginationRowModel: getPaginationRowModel()}),
    ...(isServer && {manualPagination: true, pageCount: rest.pageCount}),
  });

  const totalCount = isServer ? rest.totalCount : data.length;
  const currentPageCount = table.getPageCount();
  const start = totalCount === 0 ? 0 : pagination.pageIndex * pagination.pageSize + 1;
  const end = Math.min(totalCount, (pagination.pageIndex + 1) * pagination.pageSize);

  return (
    <div>
      <div className="flex items-center gap-2 px-6 py-5">
        {toolbar ? (
          <div className="flex flex-wrap items-center gap-2 flex-1 min-w-0">
            {toolbar}
          </div>
        ) : (
          <div className="flex-1"/>
        )}
        {showColumnVisibility && (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="sm" className="shrink-0 text-xs font-medium text-muted-foreground hover:text-foreground">
                <Settings2 className="size-3.5"/>
                Columns
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              {table
                .getAllColumns()
                .filter((col) => col.getCanHide())
                .map((col) => (
                  <DropdownMenuCheckboxItem
                    key={col.id}
                    className="capitalize"
                    checked={col.getIsVisible()}
                    onCheckedChange={(value) => col.toggleVisibility(!!value)}
                  >
                    {typeof col.columnDef.header === "string"
                      ? col.columnDef.header
                      : col.id}
                  </DropdownMenuCheckboxItem>
                ))}
            </DropdownMenuContent>
          </DropdownMenu>
        )}
      </div>
      <div className="relative border-y border-border">
        {header && (
          <div className="sticky top-0 z-10 flex items-center px-4 py-2.5 border-b border-border bg-card/95 backdrop-blur-sm">
            {header}
          </div>
        )}
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id} className="hover:bg-transparent border-b border-border">
                {headerGroup.headers.map((header, i) => (
                  <TableHead
                    key={header.id}
                    className={cn(
                      "h-9 px-3",
                      i === 0 && "pl-6",
                      i === headerGroup.headers.length - 1 && "pr-6",
                    )}
                  >
                    {header.isPlaceholder
                      ? null
                      : flexRender(
                        header.column.columnDef.header,
                        header.getContext(),
                      )}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows?.length ? (
              table.getRowModel().rows.map((row) => {
                const href = getRowHref?.(row.original);
                const linkLabel = href
                  ? (getRowLinkLabel?.(row.original) ?? "Open row")
                  : undefined;
                return (
                  <TableRow
                    key={row.id}
                    data-state={row.getIsSelected() && "selected"}
                    className={cn(
                      "relative border-b border-border/60 last:border-b-0 hover:bg-accent/40",
                      href && "focus-within:bg-accent/60 has-[a.row-link:focus-visible]:ring-1 has-[a.row-link:focus-visible]:ring-accent-brand/50 has-[a.row-link:focus-visible]:ring-inset",
                    )}
                  >
                    {row.getVisibleCells().map((cell, i) => (
                      <TableCell
                        key={cell.id}
                        className={cn(
                          "h-11 px-3 align-middle",
                          i === 0 && "pl-6",
                          i === row.getVisibleCells().length - 1 && "pr-6",
                        )}
                      >
                        {/* The stretched link lives in the first cell because
                            anchors must be a DOM child of td. Inner clickable
                            elements need `relative` to stack above it. */}
                        {i === 0 && href && (
                          <Link
                            to={href}
                            aria-label={linkLabel}
                            className="row-link absolute inset-0 cursor-pointer focus:outline-none"
                          >
                            <span className="sr-only">{linkLabel}</span>
                          </Link>
                        )}
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </TableCell>
                    ))}
                  </TableRow>
                );
              })
            ) : (
              <TableRow className="hover:bg-transparent">
                <TableCell
                  colSpan={columns.length}
                  className="py-8 text-center"
                >
                  <span className="eyebrow">no results</span>
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between px-6 py-5 text-xs text-muted-foreground">
        <div className="tnum">
          {totalCount === 0 ? (
            "no results"
          ) : (
            <>
              <span className="text-foreground/85">{start}</span>
              <span className="px-1 text-muted-foreground/40">–</span>
              <span className="text-foreground/85">{end}</span>
              <span className="px-2 text-muted-foreground/40">of</span>
              <span className="text-foreground/85">{totalCount}</span>
            </>
          )}
        </div>
        <div className="flex items-center gap-4">
          {currentPageCount > 1 && (
            <div className="flex items-center gap-1">
              <button
                type="button"
                onClick={() => table.previousPage()}
                disabled={!table.getCanPreviousPage()}
                aria-label="Previous page"
                className="flex h-7 w-7 items-center justify-center rounded text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:opacity-30 disabled:pointer-events-none"
              >
                <ChevronLeft className="size-4"/>
              </button>
              <span className="px-2 tnum text-foreground/85">
                {pagination.pageIndex + 1}
                <span className="px-1 text-muted-foreground/40">/</span>
                {currentPageCount}
              </span>
              <button
                type="button"
                onClick={() => table.nextPage()}
                disabled={!table.getCanNextPage()}
                aria-label="Next page"
                className="flex h-7 w-7 items-center justify-center rounded text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:opacity-30 disabled:pointer-events-none"
              >
                <ChevronRight className="size-4"/>
              </button>
            </div>
          )}
          <div className="flex items-center gap-2">
            <span className="whitespace-nowrap">rows</span>
            <Select
              value={pagination.pageSize.toString()}
              onValueChange={(value) =>
                onPaginationChange((prev: PaginationState) => ({
                  ...prev,
                  pageSize: Number(value),
                  pageIndex: 0,
                }))
              }
            >
              <SelectTrigger size="sm" className="h-7 w-16 px-2 font-mono text-[11px] tracking-wider text-foreground/85">
                <SelectValue/>
              </SelectTrigger>
              <SelectContent position="popper" align="end">
                {[15, 25, 50, 100].map((size) => (
                  <SelectItem key={size} value={size.toString()}>
                    {size}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>
    </div>
  );
}
