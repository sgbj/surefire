import {type RefObject, useEffect, useRef} from "react";

/**
 * Fires `onLoadMore` when a sentinel placed at the end of the list enters the
 * scroll container's viewport. Place `sentinelRef` on a 1px-tall element at
 * the end of the list, inside the same container as `scrollContainerRef`.
 * `rootMargin` of 400px prefetches ~14 rows of 28px height; raise for taller
 * rows or larger pages.
 */
export function useInfiniteScroll(params: {
  scrollContainerRef: RefObject<HTMLElement | null>;
  hasMore: boolean;
  isLoading: boolean;
  onLoadMore: () => void;
  /** CSS margin around the root box. Default "400px 0px" triggers well ahead of the bottom. */
  rootMargin?: string;
}): { sentinelRef: RefObject<HTMLDivElement | null> } {
  const {
    scrollContainerRef,
    hasMore,
    isLoading,
    onLoadMore,
    rootMargin = "400px 0px",
  } = params;
  const sentinelRef = useRef<HTMLDivElement | null>(null);

  // Stash latest callback in a ref so the observer effect doesn't tear down
  // and recreate IntersectionObserver every render.
  const onLoadMoreRef = useRef(onLoadMore);
  useEffect(() => {
    onLoadMoreRef.current = onLoadMore;
  }, [onLoadMore]);

  useEffect(() => {
    const sentinel = sentinelRef.current;
    const root = scrollContainerRef.current;
    if (!sentinel || !root || !hasMore || isLoading) return;

    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            onLoadMoreRef.current();
            break;
          }
        }
      },
      {root, rootMargin},
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [scrollContainerRef, hasMore, isLoading, rootMargin]);

  return {sentinelRef};
}
