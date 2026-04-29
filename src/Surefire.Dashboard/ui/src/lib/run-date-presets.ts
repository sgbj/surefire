export interface DatePreset {
  label: string;
  value: string;
  getAfter: () => string | undefined;
}

export const RUN_DATE_PRESETS: DatePreset[] = [
  {label: "All time", value: "all", getAfter: () => undefined},
  {
    label: "Last hour",
    value: "1h",
    getAfter: () => new Date(Date.now() - 3600_000).toISOString(),
  },
  {
    label: "Last 24 hours",
    value: "24h",
    getAfter: () => new Date(Date.now() - 86400_000).toISOString(),
  },
  {
    label: "Last 7 days",
    value: "7d",
    getAfter: () => new Date(Date.now() - 604800_000).toISOString(),
  },
  {
    label: "Last 30 days",
    value: "30d",
    getAfter: () => new Date(Date.now() - 2592000_000).toISOString(),
  },
];
