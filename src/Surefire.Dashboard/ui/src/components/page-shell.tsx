import type {ReactNode} from "react";
import {cn} from "@/lib/utils";

export function PageShell({
                            children,
                            className,
                          }: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("relative w-full flex-1 flex flex-col min-h-0 animate-lift-in", className)}>
      {children}
    </div>
  );
}

export function PageBody({
                           children,
                           className,
                         }: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("p-6", className)}>
      {children}
    </div>
  );
}
