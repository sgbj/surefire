import { useRef, useEffect, useCallback } from 'react';

/**
 * Auto-scrolls a container to the bottom when content changes,
 * but only if the user hasn't scrolled up manually.
 */
export function useStickToBottom(deps: unknown[]) {
  const ref = useRef<HTMLDivElement>(null);
  const isAtBottom = useRef(true);
  const programmatic = useRef(false);

  const onScroll = useCallback(() => {
    if (programmatic.current) return;
    const el = ref.current;
    if (el) {
      isAtBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 30;
    }
  }, []);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    el.addEventListener('scroll', onScroll, { passive: true });
    return () => el.removeEventListener('scroll', onScroll);
  }, [onScroll]);

  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    const el = ref.current;
    if (isAtBottom.current && el) {
      programmatic.current = true;
      el.scrollTop = el.scrollHeight;
      requestAnimationFrame(() => { programmatic.current = false; });
    }
  }, deps);

  return ref;
}
