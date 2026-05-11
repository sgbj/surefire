import {useLiveDuration} from "@/hooks/use-live-duration";
import {cn} from "@/lib/utils";

export function LiveDuration({
                               startedAt,
                               completedAt,
                               className,
                             }: {
  startedAt?: string | null;
  completedAt?: string | null;
  className?: string;
}) {
  const duration = useLiveDuration(startedAt, completedAt);
  return (
    <span
      className={cn(
        "font-mono text-xs tnum text-foreground/85",
        className,
      )}
    >
      {duration}
    </span>
  );
}
