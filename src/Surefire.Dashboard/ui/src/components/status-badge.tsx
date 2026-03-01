import { Badge } from '@/components/ui/badge';
import { JobStatusLabels, JobStatusColors } from '@/lib/api';

export function StatusBadge({ status }: { status: number }) {
  return (
    <Badge variant="secondary" className={JobStatusColors[status] ?? ''}>
      {JobStatusLabels[status] ?? 'Unknown'}
    </Badge>
  );
}
