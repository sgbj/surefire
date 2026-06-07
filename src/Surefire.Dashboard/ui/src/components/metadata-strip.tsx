import {type ReactNode, useEffect, useRef, useState} from "react";
import {DtDd} from "./dt-dd";

export interface MetadataItem {
  key: string;
  label?: string;
  children: ReactNode;
  align?: "default" | "mono";
  fullWidth?: boolean;
}

export function MetadataStrip({items}: { items: MetadataItem[] }) {
  const [expanded, setExpanded] = useState(false);
  const [colCount, setColCount] = useState(5);
  const gridRef = useRef<HTMLDListElement>(null);

  useEffect(() => {
    const grid = gridRef.current;
    if (!grid) return;
    const update = () => {
      const cs = window.getComputedStyle(grid);
      const cols = cs.gridTemplateColumns.split(" ").filter(Boolean).length;
      if (cols > 0) setColCount(cols);
    };
    update();
    const ro = new ResizeObserver(update);
    ro.observe(grid);
    return () => ro.disconnect();
  }, []);

  const fullWidth = items.filter((i) => i.fullWidth);
  const regular = items.filter((i) => !i.fullWidth);
  const collapsedRowCount = 2;
  const collapsedLimit = colCount * collapsedRowCount;
  const canExpand = regular.length > collapsedLimit;
  const visibleRegular =
    !canExpand || expanded ? regular : regular.slice(0, collapsedLimit);
  const hiddenCount = canExpand ? regular.length - collapsedLimit : 0;

  return (
    <div className={`border-b border-border px-6 pt-5 ${canExpand ? "pb-3" : "pb-5"}`}>
      <dl
        ref={gridRef}
        className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 2xl:grid-cols-6 gap-x-6 gap-y-5"
      >
        {fullWidth.map((item) => (
          <DtDd key={item.key} label={item.label} align={item.align} className="col-span-full">
            {item.children}
          </DtDd>
        ))}
        {visibleRegular.map((item) => (
          <DtDd key={item.key} label={item.label} align={item.align}>
            {item.children}
          </DtDd>
        ))}
      </dl>
      {canExpand && (
        <div className="mt-5 flex justify-center">
          <button
            type="button"
            onClick={() => setExpanded((e) => !e)}
            aria-expanded={expanded}
            className="rounded-sm text-xs font-medium text-muted-foreground transition-colors hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/50"
          >
            {expanded ? "Show less" : `Show ${hiddenCount} more`}
          </button>
        </div>
      )}
    </div>
  );
}
