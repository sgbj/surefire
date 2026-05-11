import type {Column} from "@tanstack/react-table";
import {ArrowDown, ArrowUp, ArrowUpDown} from "lucide-react";
import {cn} from "@/lib/utils";

interface SortableHeaderProps<TData> {
  column: Column<TData>;
  children: React.ReactNode;
  align?: "left" | "right";
}

export function SortableHeader<TData>({
                                        column,
                                        children,
                                        align = "left",
                                      }: SortableHeaderProps<TData>) {
  if (!column.getCanSort()) {
    return (
      <span
        className={cn(
          "eyebrow",
          align === "right" ? "block text-right" : "",
        )}
      >
        {children}
      </span>
    );
  }
  const sort = column.getIsSorted();
  const Icon = sort === "asc" ? ArrowUp : sort === "desc" ? ArrowDown : ArrowUpDown;
  return (
    <button
      type="button"
      onClick={() => column.toggleSorting(sort === "asc")}
      className={cn(
        "inline-flex items-center gap-1 text-xs font-medium text-muted-foreground transition-colors hover:text-foreground",
        sort && "text-foreground",
        align === "right" ? "ml-auto" : "",
      )}
    >
      {children}
      <Icon className="size-3 opacity-60"/>
    </button>
  );
}
