import type {ReactNode} from "react";
import {cn} from "@/lib/utils";

export const metadataGridClass =
  "grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-x-6 gap-y-5 border-b border-border px-6 py-5";

export function DtDd({
                        label,
                        children,
                       align = "default",
                       className,
                     }: {
  label?: string;
  children: ReactNode;
  align?: "default" | "mono";
  className?: string;
}) {
  return (
    <div className={cn("min-w-0", className)}>
      {label && <dt className="eyebrow">{label}</dt>}
      <dd
        className={cn(
          "text-sm text-foreground",
          label && "mt-1",
          align === "mono" ? "font-mono tnum" : "",
        )}
      >
        {children}
      </dd>
    </div>
  );
}
