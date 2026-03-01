const dateFormat = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
});

export function formatDate(iso?: string | null): string {
  if (!iso) return "";
  return dateFormat.format(new Date(iso));
}

const logTimeFormat = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  fractionalSecondDigits: 3,
});

export function formatLogTime(iso: string): string {
  return logTimeFormat.format(new Date(iso));
}

const relativeFormat = new Intl.RelativeTimeFormat(undefined, {
  numeric: "always",
  style: "narrow",
});

const RELATIVE_UNITS: [number, Intl.RelativeTimeFormatUnit][] = [
  [60, "second"],
  [3600, "minute"],
  [86400, "hour"],
  [Infinity, "day"],
];

export function formatRelative(iso?: string | null): string {
  if (!iso) return "";
  const diff = (Date.now() - new Date(iso).getTime()) / 1000;
  for (const [threshold, unit] of RELATIVE_UNITS) {
    if (diff < threshold) {
      const divisor =
        unit === "second"
          ? 1
          : unit === "minute"
            ? 60
            : unit === "hour"
              ? 3600
              : 86400;
      return relativeFormat.format(-Math.floor(diff / divisor), unit);
    }
  }
  return relativeFormat.format(-Math.floor(diff / 86400), "day");
}

export function formatDuration(
  start?: string | null,
  end?: string | null,
): string {
  if (!start || !end) return "";
  const ms = new Date(end).getTime() - new Date(start).getTime();
  return formatMs(ms);
}

/** Parse a .NET TimeSpan string ("HH:mm:ss" or "d.HH:mm:ss") to ms */
export function parseTimeSpan(ts?: string | null): number | null {
  if (!ts) return null;
  const match = ts.match(/^(?:(\d+)\.)?(\d+):(\d+):(\d+)(?:\.(\d+))?$/);
  if (!match) return null;
  const [, d, h, m, s, frac] = match;
  const whole =
    (Number(d ?? 0) * 86400 + Number(h) * 3600 + Number(m) * 60 + Number(s)) *
    1000;
  const fracMs = frac ? Number(frac.slice(0, 3).padEnd(3, "0")) : 0;
  return whole + fracMs;
}

export function formatTimeSpan(ts?: string | null): string {
  const ms = parseTimeSpan(ts);
  if (ms === null) return "";
  return formatMs(ms);
}

export function formatMs(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const totalSeconds = Math.floor(ms / 1000);
  if (totalSeconds < 60) return `${totalSeconds}s`;
  const m = Math.floor(totalSeconds / 60);
  const s = totalSeconds % 60;
  if (m < 60) return s > 0 ? `${m}m ${s}s` : `${m}m`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  if (rm > 0) return `${h}h ${rm}m`;
  return `${h}h`;
}
