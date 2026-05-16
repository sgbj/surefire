import { CircleAlert } from "lucide-react";
import { cn } from "@/lib/utils";

export function PageErrorBanner({
  message,
  className,
}: {
  message: string;
  className?: string;
}) {
  return (
    <div
      role="alert"
      className={cn(
        "flex items-center gap-2 border-b border-border bg-destructive/5 px-6 py-3 text-sm text-destructive",
        className,
      )}
    >
      <CircleAlert className="size-4 shrink-0" />
      <span>{message}</span>
    </div>
  );
}
