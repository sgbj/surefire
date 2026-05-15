import {useEffect, useRef, type RefObject} from "react";

interface UseTailFollowOptions {
  scrollElement: HTMLElement | null;
  contentElementRef: RefObject<HTMLElement | null>;
  /** When this changes to a defined value, the hook force-pins to the bottom
   *  and starts following. Use it to engage tail-follow on initial visits to
   *  live streams (e.g., active runs) so the user doesn't have to scroll down
   *  manually. Pass `undefined` to fall back to "pin only if already near
   *  bottom" behavior. */
  followKey?: string;
  thresholdPx?: number;
}

export function useTailFollow({
                                scrollElement,
                                contentElementRef,
                                followKey,
                                thresholdPx = 48,
                              }: UseTailFollowOptions): void {
  const pinnedRef = useRef(false);
  const lastFollowKeyRef = useRef<string | undefined>(undefined);

  useEffect(() => {
    const scrollEl = scrollElement;
    if (!scrollEl) return;
    const contentEl = contentElementRef.current;
    if (!contentEl) return;

    const distanceFromBottom = () =>
      scrollEl.scrollHeight - scrollEl.scrollTop - scrollEl.clientHeight;

    // Mid-burst, scrollHeight can grow between our scrollTop write and the
    // resulting scroll event, so the scroll handler would unpin us. Ignore
    // scroll events that come from our own writes via a double-rAF window.
    let isProgrammatic = false;
    const scrollToBottom = () => {
      isProgrammatic = true;
      scrollEl.scrollTo({
        top: scrollEl.scrollHeight - scrollEl.clientHeight,
        behavior: "instant",
      });
      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          isProgrammatic = false;
        });
      });
    };

    if (followKey !== undefined && lastFollowKeyRef.current !== followKey) {
      lastFollowKeyRef.current = followKey;
      pinnedRef.current = true;
      scrollToBottom();
    } else if (followKey === undefined) {
      lastFollowKeyRef.current = undefined;
      pinnedRef.current = distanceFromBottom() <= thresholdPx;
    }

    const onScroll = () => {
      if (isProgrammatic) return;
      pinnedRef.current = distanceFromBottom() <= thresholdPx;
    };
    scrollEl.addEventListener("scroll", onScroll, {passive: true});

    const ro = new ResizeObserver(() => {
      if (pinnedRef.current) scrollToBottom();
    });
    ro.observe(contentEl);

    return () => {
      scrollEl.removeEventListener("scroll", onScroll);
      ro.disconnect();
    };
  }, [scrollElement, contentElementRef, followKey, thresholdPx]);
}
