import { useRef, useEffect, useCallback } from "react";
import type { Virtualizer } from "@tanstack/react-virtual";

interface UseStickToBottomOptions<TScrollElement extends Element | Window> {
  /** The scroll container element. */
  scrollElement: HTMLElement | null;
  /** The virtualizer instance — used to scrollToIndex on auto-scroll. */
  virtualizer: Virtualizer<TScrollElement, Element>;
  /** Number of items in the list (triggers auto-scroll when it changes). */
  count: number;
  /** Pixel threshold for "at bottom" detection. Default: 16. */
  bottomOffsetPx?: number;
}

/**
 * Auto-scrolls a virtualised list to the bottom when new items arrive, but
 * *only* while the user hasn't scrolled away from the bottom. Scrolling up
 * disables the stick; scrolling back down re-enables it.
 *
 * Implementation note: we re-read the distance from the bottom on every scroll
 * event (user- or programmatic-initiated). With fixed-height rows, our own
 * scrollToIndex lands cleanly at the bottom, leaving distance = 0 — so the
 * stick stays engaged. When the user manually scrolls up, distance > threshold
 * and we disengage; once they return, distance <= threshold and we re-engage.
 *
 * We deliberately do NOT gate scroll events with a timeout window (the earlier
 * design did, which on spammy jobs locked out user-initiated scrolling because
 * fresh items keep renewing the window).
 */
export function useStickToBottom<TScrollElement extends Element | Window>({
  scrollElement,
  virtualizer,
  count,
  bottomOffsetPx = 16,
}: UseStickToBottomOptions<TScrollElement>): void {
  const isAtBottom = useRef(true);

  const onScroll = useCallback(() => {
    const el = scrollElement;
    if (!el) return;
    isAtBottom.current =
      el.scrollHeight - el.scrollTop - el.clientHeight <= bottomOffsetPx;
  }, [scrollElement, bottomOffsetPx]);

  useEffect(() => {
    const el = scrollElement;
    if (!el) return;
    isAtBottom.current =
      el.scrollHeight - el.scrollTop - el.clientHeight <= bottomOffsetPx;
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, [scrollElement, bottomOffsetPx, onScroll]);

  // Re-run when `count` changes (new row arrived) AND when `getTotalSize()`
  // changes (row measurements settled). With variable-height rows, the first
  // pass after a new append scrolls based on the row's *estimated* height;
  // once measureElement produces the true height on the next frame,
  // totalSize updates and we re-scroll so the row is actually at the bottom.
  const totalSize = virtualizer.getTotalSize();
  useEffect(() => {
    if (!isAtBottom.current || count === 0) return;
    virtualizer.scrollToIndex(count - 1, { align: "end" });
  }, [count, totalSize, virtualizer]);
}
