import { Badge } from '@/components/ui/badge';
import { JobStatusLabels, JobStatusColors } from '@/lib/api';

export function StatusBadge({ status }: { status: number }) {
  return (
    <Badge variant="secondary" className={`border-transparent ${JobStatusColors[status] ?? ''}`}>
      <span className="size-1.5 rounded-full bg-current" />
      {JobStatusLabels[status] ?? 'Unknown'}
    </Badge>
  );
}
