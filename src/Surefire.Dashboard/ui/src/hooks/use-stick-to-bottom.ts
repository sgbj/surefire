import {useCallback, useEffect, useRef} from "react";
import type {Virtualizer} from "@tanstack/react-virtual";

interface UseStickToBottomOptions<TScrollElement extends Element | Window> {
  /** The scroll container element. */
  scrollElement: HTMLElement | null;
  /** The virtualizer instance, used to scrollToIndex on auto-scroll. */
  virtualizer: Virtualizer<TScrollElement, Element>;
  /** Number of items in the list (triggers auto-scroll when it changes). */
  count: number;
  /** Pixel threshold for "at bottom" detection. Default: 16. */
  bottomOffsetPx?: number;
}

/**
 * Auto-scrolls a virtualised list to the bottom when new items arrive, but
 * only while the user is at the bottom. Distance is re-read on every scroll
 * event (user- and programmatic-initiated); a timeout-window approach was
 * tried and rejected because fresh items kept renewing the window on busy
 * jobs and locked out user scrolling.
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
    el.addEventListener("scroll", onScroll, {passive: true});
    return () => el.removeEventListener("scroll", onScroll);
  }, [scrollElement, bottomOffsetPx, onScroll]);

  // Re-run on `count` (new row) and `totalSize` (variable-height measurements
  // settle on the frame after append; without this re-scroll the new row lands
  // off-bottom because the first pass used estimateSize).
  const totalSize = virtualizer.getTotalSize();
  useEffect(() => {
    if (!isAtBottom.current || count === 0) return;
    virtualizer.scrollToIndex(count - 1, {align: "end"});
  }, [count, totalSize, virtualizer]);
}


