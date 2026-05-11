import {cn} from "@/lib/utils";
import type {ReactNode} from "react";

interface SectionHeaderProps {
  title: ReactNode;
  actions?: ReactNode;
  className?: string;
  children?: ReactNode;
}

export function SectionHeader({
                                title,
                                actions,
                                className,
                                children,
                              }: SectionHeaderProps) {
  return (
    <header
      className={cn(
        "mb-3 flex flex-wrap items-end justify-between gap-x-6 gap-y-2",
        className,
      )}
    >
      <div className="min-w-0 flex-1">
        <div className="min-w-0 text-base font-semibold tracking-tight text-foreground">
          {title}
        </div>
        {children}
      </div>
      {actions && <div className="flex items-center gap-2 shrink-0">{actions}</div>}
    </header>
  );
}

