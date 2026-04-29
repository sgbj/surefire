import {useLiveDuration} from '@/hooks/use-live-duration';

export function LiveDuration({startedAt, completedAt}: { startedAt?: string | null; completedAt?: string | null }) {
  const duration = useLiveDuration(startedAt, completedAt);
  return <span className="text-sm">{duration}</span>;
}
