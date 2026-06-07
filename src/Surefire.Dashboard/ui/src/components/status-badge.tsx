import {JobStatus, JobStatusLabels} from "@/lib/api";
import {cn} from "@/lib/utils";

const statusColor: Record<number, string> = {
  [JobStatus.Pending]: "var(--status-pending)",
  [JobStatus.Running]: "var(--status-running)",
  [JobStatus.Succeeded]: "var(--status-succeeded)",
  [JobStatus.Suspended]: "var(--status-suspended)",
  [JobStatus.Canceled]: "var(--status-canceled)",
  [JobStatus.Failed]: "var(--status-failed)",
};

interface StatusBadgeProps {
  status: number;
  className?: string;
}

/** Quiet pill: tinted bg + hairline border + sentence-case label. */
export function StatusBadge({status, className}: StatusBadgeProps) {
  const isRunning = status === JobStatus.Running;
  const label = JobStatusLabels[status] ?? "Unknown";
  return (
    <span
      className={cn("status-pill", isRunning && "is-running", className)}
      style={{["--pill-fg" as string]: statusColor[status]}}
    >
      <span className="dot"/>
      {label}
    </span>
  );
}

interface StatePillProps {
  tone?: "success" | "warning" | "muted" | "neutral";
  children: React.ReactNode;
  className?: string;
}

/** Generic state pill for non-status flags (Enabled, Disabled, Paused, etc). */
export function StatePill({tone = "neutral", children, className}: StatePillProps) {
  const fg =
    tone === "success"
      ? "var(--status-succeeded)"
      : tone === "warning"
        ? "var(--status-pending)"
        : tone === "muted"
          ? "var(--muted-foreground)"
          : undefined;
  return (
    <span
      className={cn("status-pill", className)}
      style={fg ? {["--pill-fg" as string]: fg} : undefined}
    >
      {children}
    </span>
  );
}
