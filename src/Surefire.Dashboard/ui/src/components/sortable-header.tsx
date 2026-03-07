import type { Column } from "@tanstack/react-table";
import { Button } from "@/components/ui/button";
import { ArrowUpDown } from "lucide-react";

interface SortableHeaderProps<TData> {
  column: Column<TData>;
  children: React.ReactNode;
}

export function SortableHeader<TData>({ column, children }: SortableHeaderProps<TData>) {
  return (
    <Button
      variant="ghost"
      onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
      className="-ml-4 text-xs font-medium uppercase tracking-wider text-muted-foreground"
    >
      {children}
      <ArrowUpDown className="ml-1 h-3.5 w-3.5 opacity-40" />
    </Button>
  );
}
