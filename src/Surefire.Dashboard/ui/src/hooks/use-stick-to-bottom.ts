import { useRef, useEffect, useCallback, useState } from "react";

interface StickToBottomOptions {
  bottomOffsetPx?: number;
}

/**
 * Auto-scrolls a container to the bottom when content changes,
 * but only if the user hasn't scrolled up manually.
 */
export function useStickToBottom(
  changeKey: unknown,
  options: StickToBottomOptions = {},
) {
  const { bottomOffsetPx = 4 } = options;
  const [element, setElement] = useState<HTMLDivElement | null>(null);
  const isAtBottom = useRef(true);
  const programmatic = useRef(false);

  const onScroll = useCallback(() => {
    if (programmatic.current) return;
    const el = element;
    if (el) {
      isAtBottom.current =
        el.scrollHeight - el.scrollTop - el.clientHeight <= bottomOffsetPx;
    }
  }, [bottomOffsetPx, element]);

  useEffect(() => {
    const el = element;
    if (!el) return;
    // Recompute current stickiness whenever a new element is attached.
    isAtBottom.current =
      el.scrollHeight - el.scrollTop - el.clientHeight <= bottomOffsetPx;
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, [bottomOffsetPx, element, onScroll]);

  useEffect(() => {
    const el = element;
    if (isAtBottom.current && el) {
      programmatic.current = true;
      el.scrollTop = el.scrollHeight;
      requestAnimationFrame(() => {
        programmatic.current = false;
      });
    }
  }, [changeKey, element]);

  return setElement;
}
