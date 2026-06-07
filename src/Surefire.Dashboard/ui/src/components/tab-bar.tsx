import type {ReactNode} from "react";
import {Tabs as TabsPrimitive} from "radix-ui";
import {cn} from "@/lib/utils";

export function TabBar({
                         className,
                         children,
                       }: {
  className?: string;
  children: ReactNode;
}) {
  return (
    <div className={cn("border-b border-border bg-card/95 backdrop-blur-sm", className)}>
      <TabsPrimitive.List
        data-slot="tab-bar-list"
        className="flex h-[2.625rem] items-center gap-6 overflow-x-auto overflow-y-hidden px-6 [scrollbar-width:thin]"
      >
        {children}
      </TabsPrimitive.List>
    </div>
  );
}

export function ToolBar({
                          className,
                          children,
                        }: {
  className?: string;
  children: ReactNode;
}) {
  return (
    <div
      className={cn(
        "flex min-h-[3rem] shrink-0 flex-wrap items-center gap-3 border-b border-border px-6 py-2",
        className,
      )}
    >
      {children}
    </div>
  );
}

export function TabBarTrigger({
                                className,
                                children,
                                count,
                                ...props
                              }: React.ComponentProps<typeof TabsPrimitive.Trigger> & {
  count?: number | string;
}) {
  return (
    <TabsPrimitive.Trigger
      data-slot="tab-bar-trigger"
      className={cn(
        "relative inline-flex h-[2.625rem] items-center text-sm font-medium tracking-tight transition-colors",
        "text-muted-foreground hover:text-foreground",
        "data-[state=active]:text-foreground",
        "after:pointer-events-none after:absolute after:inset-x-0 after:bottom-0 after:h-[2px] after:rounded-full after:bg-accent-brand after:opacity-0 after:transition-opacity",
        "data-[state=active]:after:opacity-100",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/50 rounded-sm",
        className,
      )}
      {...props}
    >
      <span className="inline-flex items-baseline gap-1.5">
        {children}
        {count !== undefined && (
          <span className="text-xs tnum text-muted-foreground/80">
            {count}
          </span>
        )}
      </span>
    </TabsPrimitive.Trigger>
  );
}
