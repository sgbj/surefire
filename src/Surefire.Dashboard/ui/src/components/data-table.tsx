import {
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type PaginationState,
  type SortingState,
  type VisibilityState,
} from "@tanstack/react-table"
import { type Dispatch, type SetStateAction, useState } from "react"
import { ChevronFirst, ChevronLast, ChevronLeft, ChevronRight, Settings2 } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

interface DataTableBaseProps<TData, TValue> {
  columns: ColumnDef<TData, TValue>[]
  data: TData[]
  toolbar?: React.ReactNode
  showColumnVisibility?: boolean
  defaultPageSize?: number
}

interface DataTableClientProps<TData, TValue> extends DataTableBaseProps<TData, TValue> {
  manualPagination?: false
  pageCount?: never
  totalCount?: never
  pagination?: never
  onPaginationChange?: never
}

interface DataTableServerProps<TData, TValue> extends DataTableBaseProps<TData, TValue> {
  manualPagination: true
  pageCount: number
  totalCount: number
  pagination: PaginationState
  onPaginationChange: Dispatch<SetStateAction<PaginationState>>
}

type DataTableProps<TData, TValue> = DataTableClientProps<TData, TValue> | DataTableServerProps<TData, TValue>

export function DataTable<TData, TValue>({
  columns,
  data,
  toolbar,
  showColumnVisibility = false,
  defaultPageSize = 20,
  ...rest
}: DataTableProps<TData, TValue>) {
  const isServer = rest.manualPagination === true

  const [sorting, setSorting] = useState<SortingState>([])
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({})
  const [clientPagination, setClientPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize: defaultPageSize,
  })

  const pagination = isServer ? rest.pagination : clientPagination
  const onPaginationChange = isServer ? rest.onPaginationChange : setClientPagination

  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
      columnVisibility,
      pagination,
    },
    onSortingChange: (updater) => {
      setSorting(updater)
      if (!isServer) {
        onPaginationChange((prev: PaginationState) => ({ ...prev, pageIndex: 0 }))
      }
    },
    onColumnVisibilityChange: setColumnVisibility,
    onPaginationChange,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    ...(!isServer && { getPaginationRowModel: getPaginationRowModel() }),
    ...(isServer && {
      manualPagination: true,
      pageCount: rest.pageCount,
    }),
  })

  const totalCount = isServer ? rest.totalCount : data.length
  const currentPageCount = table.getPageCount()

  return (
    <div className="space-y-4">
      {(toolbar || showColumnVisibility) && (
        <div className="flex items-center gap-2">
          {toolbar && <div className="flex flex-wrap items-center gap-2 flex-1 min-w-0">{toolbar}</div>}
          {!toolbar && <div className="flex-1" />}
          {showColumnVisibility && (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" className="shrink-0">
                  <Settings2 className="size-4" />
                  Columns
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                {table.getAllColumns()
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
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id}>
                {headerGroup.headers.map((header, i) => (
                  <TableHead key={header.id} className={i === 0 ? "pl-4" : undefined}>
                    {header.isPlaceholder
                      ? null
                      : flexRender(header.column.columnDef.header, header.getContext())}
                  </TableHead>
                ))}
              </TableRow>
            ))}
          </TableHeader>
          <TableBody>
            {table.getRowModel().rows?.length ? (
              table.getRowModel().rows.map((row) => (
                <TableRow key={row.id} data-state={row.getIsSelected() && "selected"}>
                  {row.getVisibleCells().map((cell, i) => (
                    <TableCell key={cell.id} className={i === 0 ? "pl-4" : undefined}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell colSpan={columns.length} className="h-24 text-center">
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
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground whitespace-nowrap">Rows</span>
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
                <SelectValue />
              </SelectTrigger>
              <SelectContent position="popper" align="end">
                {[20, 50, 100].map((size) => (
                  <SelectItem key={size} value={size.toString()}>
                    {size}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <span className="text-sm text-muted-foreground tabular-nums whitespace-nowrap">
            Page {pagination.pageIndex + 1} of {currentPageCount || 1}
          </span>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="icon-sm"
              onClick={() => table.firstPage()}
              disabled={!table.getCanPreviousPage()}
            >
              <ChevronFirst />
            </Button>
            <Button
              variant="outline"
              size="icon-sm"
              onClick={() => table.previousPage()}
              disabled={!table.getCanPreviousPage()}
            >
              <ChevronLeft />
            </Button>
            <Button
              variant="outline"
              size="icon-sm"
              onClick={() => table.nextPage()}
              disabled={!table.getCanNextPage()}
            >
              <ChevronRight />
            </Button>
            <Button
              variant="outline"
              size="icon-sm"
              onClick={() => table.lastPage()}
              disabled={!table.getCanNextPage()}
            >
              <ChevronLast />
            </Button>
          </div>
        </div>
      </div>
    </div>
  )
}
