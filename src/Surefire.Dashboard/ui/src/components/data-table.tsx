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
import {Settings2} from "lucide-react";

import {Button} from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Pagination,
  PaginationContent,
  PaginationEllipsis,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from "@/components/ui/pagination";
import {Select, SelectContent, SelectItem, SelectTrigger, SelectValue,} from "@/components/ui/select";
import {Table, TableBody, TableCell, TableHead, TableHeader, TableRow,} from "@/components/ui/table";

function getPageNumbers(
  current: number,
  total: number,
): (number | "ellipsis")[] {
  if (total <= 7) return Array.from({length: total}, (_, i) => i + 1);
  const pages: (number | "ellipsis")[] = [1];
  if (current > 3) pages.push("ellipsis");
  for (
    let i = Math.max(2, current - 1);
    i <= Math.min(total - 1, current + 1);
    i++
  )
    pages.push(i);
  if (current < total - 2) pages.push("ellipsis");
  if (total > 1) pages.push(total);
  return pages;
}

interface DataTableBaseProps<TData, TValue> {
  columns: ColumnDef<TData, TValue>[];
  data: TData[];
  toolbar?: React.ReactNode;
  header?: React.ReactNode;
  showColumnVisibility?: boolean;
  defaultPageSize?: number;
}

interface DataTableClientProps<TData, TValue> extends DataTableBaseProps<
  TData,
  TValue
> {
  manualPagination?: false;
  pageCount?: never;
  totalCount?: never;
  pagination?: never;
  onPaginationChange?: never;
}

interface DataTableServerProps<TData, TValue> extends DataTableBaseProps<
  TData,
  TValue
> {
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

  // TanStack Table's hook trips react-hooks/incompatible-library.
  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data,
    columns,
    defaultColumn: {
      enableSorting: !isServer,
    },
    state: {
      sorting,
      columnVisibility,
      pagination,
    },
    ...(!isServer && {
      onSortingChange: (
        updater: SortingState | ((prev: SortingState) => SortingState),
      ) => {
        setSorting(updater);
        onPaginationChange((prev: PaginationState) => ({
          ...prev,
          pageIndex: 0,
        }));
      },
    }),
    onColumnVisibilityChange: setColumnVisibility,
    onPaginationChange,
    getCoreRowModel: getCoreRowModel(),
    ...(!isServer && {getSortedRowModel: getSortedRowModel()}),
    ...(!isServer && {getPaginationRowModel: getPaginationRowModel()}),
    ...(isServer && {
      manualPagination: true,
      pageCount: rest.pageCount,
    }),
  });

  const totalCount = isServer ? rest.totalCount : data.length;
  const currentPageCount = table.getPageCount();

  return (
    <div className="space-y-4">
      {(toolbar || showColumnVisibility) && (
        <div className="flex items-center gap-2">
          {toolbar && (
            <div className="flex flex-wrap items-center gap-2 flex-1 min-w-0">
              {toolbar}
            </div>
          )}
          {!toolbar && <div className="flex-1"/>}
          {showColumnVisibility && (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" className="shrink-0">
                  <Settings2 className="size-4"/>
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
      )}
      <div className="rounded-lg border overflow-hidden">
        {header && (
          <div className="sticky top-0 z-10 flex items-center py-2.5 px-2 border-b bg-muted/30 backdrop-blur-sm">
            {header}
          </div>
        )}
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header, i) => (
                  <TableHead
                    key={header.id}
                    className={i === 0 ? "pl-4" : undefined}
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
              table.getRowModel().rows.map((row) => (
                <TableRow
                  key={row.id}
                  data-state={row.getIsSelected() && "selected"}
                >
                  {row.getVisibleCells().map((cell, i) => (
                    <TableCell
                      key={cell.id}
                      className={i === 0 ? "pl-4" : undefined}
                    >
                      {flexRender(
                        cell.column.columnDef.cell,
                        cell.getContext(),
                      )}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell
                  colSpan={columns.length}
                  className="h-24 text-center"
                >
                  No results.
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div className="text-sm text-muted-foreground tabular-nums">
          {totalCount} {totalCount === 1 ? "result" : "results"}
        </div>
        <div className="flex items-center gap-4">
          {currentPageCount > 1 && (
            <Pagination className="mx-0 w-auto">
              <PaginationContent>
                <PaginationItem>
                  <PaginationPrevious
                    onClick={() => table.previousPage()}
                    className={
                      !table.getCanPreviousPage()
                        ? "pointer-events-none opacity-50"
                        : "cursor-pointer"
                    }
                    aria-disabled={!table.getCanPreviousPage()}
                    tabIndex={!table.getCanPreviousPage() ? -1 : undefined}
                  />
                </PaginationItem>
                {getPageNumbers(pagination.pageIndex + 1, currentPageCount).map(
                  (page, i) =>
                    page === "ellipsis" ? (
                      <PaginationItem key={`e${i}`}>
                        <PaginationEllipsis/>
                      </PaginationItem>
                    ) : (
                      <PaginationItem key={page}>
                        <PaginationLink
                          isActive={page === pagination.pageIndex + 1}
                          onClick={() => table.setPageIndex(page - 1)}
                          className="cursor-pointer"
                        >
                          {page}
                        </PaginationLink>
                      </PaginationItem>
                    ),
                )}
                <PaginationItem>
                  <PaginationNext
                    onClick={() => table.nextPage()}
                    className={
                      !table.getCanNextPage()
                        ? "pointer-events-none opacity-50"
                        : "cursor-pointer"
                    }
                    aria-disabled={!table.getCanNextPage()}
                    tabIndex={!table.getCanNextPage() ? -1 : undefined}
                  />
                </PaginationItem>
              </PaginationContent>
            </Pagination>
          )}
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground whitespace-nowrap">
              Rows
            </span>
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
              <SelectTrigger size="sm" className="w-[80px]">
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
