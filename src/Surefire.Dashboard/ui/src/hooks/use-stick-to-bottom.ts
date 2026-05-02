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
 * Auto-scrolls a virtualized list to the bottom when new items arrive, but
 * only while the user is at the bottom. Programmatic scrolling is paused while
 * the user is actively scrolling or dragging the scrollbar so native scrollbar
 * tracking does not fight auto-scroll on log-heavy runs.
 */
export function useStickToBottom<TScrollElement extends Element | Window>({
  scrollElement,
  virtualizer,
  count,
  bottomOffsetPx = 16,
}: UseStickToBottomOptions<TScrollElement>): void {
  const isAtBottom = useRef(true);
  const isUserScrolling = useRef(false);
  const isPointerActive = useRef(false);
  const isProgrammaticScroll = useRef(false);
  const interactionTimer = useRef<number | null>(null);
  const countRef = useRef(count);

  useEffect(() => {
    countRef.current = count;
  }, [count]);

  const scrollToBottom = useCallback(() => {
    if (countRef.current === 0) return;
    isProgrammaticScroll.current = true;
    virtualizer.scrollToIndex(countRef.current - 1, {align: "end"});
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        isProgrammaticScroll.current = false;
      });
    });
  }, [virtualizer]);

  const finishUserScroll = useCallback(() => {
    if (interactionTimer.current !== null) {
      window.clearTimeout(interactionTimer.current);
    }

    interactionTimer.current = window.setTimeout(() => {
      if (isPointerActive.current) return;
      isUserScrolling.current = false;
      interactionTimer.current = null;
      if (isAtBottom.current) {
        scrollToBottom();
      }
    }, 120);
  }, [scrollToBottom]);

  const startUserScroll = useCallback(() => {
    isUserScrolling.current = true;
    finishUserScroll();
  }, [finishUserScroll]);

  const onPointerStart = useCallback(() => {
    isPointerActive.current = true;
    startUserScroll();
  }, [startUserScroll]);

  const onPointerEnd = useCallback(() => {
    isPointerActive.current = false;
    finishUserScroll();
  }, [finishUserScroll]);

  const onScroll = useCallback(() => {
    const el = scrollElement;
    if (!el) return;
    isAtBottom.current =
      el.scrollHeight - el.scrollTop - el.clientHeight <= bottomOffsetPx;
    if (!isProgrammaticScroll.current) {
      startUserScroll();
    }
  }, [scrollElement, bottomOffsetPx, startUserScroll]);

  useEffect(() => {
    const el = scrollElement;
    if (!el) return;
    isAtBottom.current =
      el.scrollHeight - el.scrollTop - el.clientHeight <= bottomOffsetPx;
    el.addEventListener("scroll", onScroll, {passive: true});
    el.addEventListener("pointerdown", onPointerStart, {passive: true});
    el.addEventListener("wheel", startUserScroll, {passive: true});
    el.addEventListener("touchstart", onPointerStart, {passive: true});
    el.addEventListener("keydown", startUserScroll);
    window.addEventListener("pointerup", onPointerEnd);
    window.addEventListener("pointercancel", onPointerEnd);
    window.addEventListener("touchend", onPointerEnd);
    window.addEventListener("touchcancel", onPointerEnd);
    window.addEventListener("blur", onPointerEnd);

    return () => {
      el.removeEventListener("scroll", onScroll);
      el.removeEventListener("pointerdown", onPointerStart);
      el.removeEventListener("wheel", startUserScroll);
      el.removeEventListener("touchstart", onPointerStart);
      el.removeEventListener("keydown", startUserScroll);
      window.removeEventListener("pointerup", onPointerEnd);
      window.removeEventListener("pointercancel", onPointerEnd);
      window.removeEventListener("touchend", onPointerEnd);
      window.removeEventListener("touchcancel", onPointerEnd);
      window.removeEventListener("blur", onPointerEnd);
      if (interactionTimer.current !== null) {
        window.clearTimeout(interactionTimer.current);
        interactionTimer.current = null;
      }
      isUserScrolling.current = false;
      isPointerActive.current = false;
    };
  }, [
    scrollElement,
    bottomOffsetPx,
    onScroll,
    startUserScroll,
    onPointerStart,
    onPointerEnd,
  ]);

  // Re-run on `count` (new row) and `totalSize` (variable-height measurements
  // settle on the frame after append; without this re-scroll the new row lands
  // off-bottom because the first pass used estimateSize).
  const totalSize = virtualizer.getTotalSize();
  useEffect(() => {
    if (!isAtBottom.current || isUserScrolling.current || count === 0) return;
    scrollToBottom();
  }, [count, totalSize, scrollToBottom]);
}
