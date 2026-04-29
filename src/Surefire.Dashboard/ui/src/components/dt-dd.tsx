export function DtDd({label, children}: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <dt className="text-xs font-medium text-muted-foreground uppercase tracking-wider">{label}</dt>
      <dd className="text-sm mt-0.5">{children}</dd>
    </div>
  );
}
