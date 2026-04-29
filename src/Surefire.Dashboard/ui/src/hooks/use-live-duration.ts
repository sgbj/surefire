import {useEffect, useState} from "react";
import {formatMs} from "@/lib/format";

export function useLiveDuration(
  start?: string | null,
  end?: string | null,
): string {
  const [now, setNow] = useState(Date.now);
  const isLive = !!start && !end;

  useEffect(() => {
    if (!isLive) return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [isLive]);

  if (!start) return "";
  const ms = (end ? new Date(end).getTime() : now) - new Date(start).getTime();
  return formatMs(Math.max(0, ms));
}
