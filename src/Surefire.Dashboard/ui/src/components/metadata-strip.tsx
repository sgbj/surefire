import {type ReactNode, useEffect, useRef, useState} from "react";
import {ChevronDown, ChevronUp} from "lucide-react";
import {DtDd} from "./dt-dd";

export interface MetadataItem {
  key: string;
  label: string;
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
  const canExpand = regular.length > colCount;
  const visibleRegular = !canExpand || expanded ? regular : regular.slice(0, colCount);

  return (
    <div className="relative border-b border-border">
      <dl
        ref={gridRef}
        className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-x-6 gap-y-5 px-6 py-5"
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
        <button
          type="button"
          onClick={() => setExpanded((e) => !e)}
          aria-expanded={expanded}
          aria-label={expanded ? "Show less" : "Show more"}
          className="absolute -bottom-[7px] left-1/2 z-20 inline-flex h-3.5 -translate-x-1/2 items-center rounded-full border border-border bg-background px-1.5 text-muted-foreground transition-colors hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/50"
        >
          {expanded ? (
            <ChevronUp className="size-2.5"/>
          ) : (
            <ChevronDown className="size-2.5"/>
          )}
        </button>
      )}
    </div>
  );
}
