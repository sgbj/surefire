import { type RefObject, useEffect, useRef } from "react";

/**
 * Observes a 1px sentinel element at the bottom of a scrollable list and invokes
 * `onLoadMore` when it enters the scroll container's viewport. Replaces the
 * manual "Load more" buttons with seamless prefetch-ahead loading.
 *
 * Usage: place the returned `sentinelRef` on a 1px-tall element at the end of
 * the list, inside the same scroll container referenced by `scrollContainerRef`.
 *
 * `rootMargin` tunes how far ahead the fetch fires. 400px is appropriate for
 * ~28px row heights × ~14 rows of prefetch; raise for taller rows or larger pages.
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

  // Callbacks change per-render but observer setup is stable — stash the latest
  // via ref so we don't tear down & recreate the IntersectionObserver each render.
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
      { root, rootMargin },
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [scrollContainerRef, hasMore, isLoading, rootMargin]);

  return { sentinelRef };
}
