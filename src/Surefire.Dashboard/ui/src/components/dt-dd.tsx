import type {ReactNode} from "react";
import {cn} from "@/lib/utils";

export function DtDd({
                       label,
                       children,
                       align = "default",
                       className,
                     }: {
  label: string;
  children: ReactNode;
  align?: "default" | "mono";
  className?: string;
}) {
  return (
    <div className={cn("min-w-0", className)}>
      <dt className="eyebrow">{label}</dt>
      <dd
        className={cn(
          "mt-1 text-sm text-foreground",
          align === "mono" ? "font-mono tnum" : "",
        )}
      >
        {children}
      </dd>
    </div>
  );
}
