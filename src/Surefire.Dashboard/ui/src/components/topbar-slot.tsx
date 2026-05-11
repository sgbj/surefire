import {createContext, useCallback, useContext, useMemo, useState, type ReactNode} from "react";
import {createPortal} from "react-dom";

type SlotName = "badge" | "actions";

interface SlotState {
  badge: HTMLElement | null;
  actions: HTMLElement | null;
}

interface TopBarSlotContextValue {
  slots: SlotState;
  setSlot: (name: SlotName, el: HTMLElement | null) => void;
}

const TopBarSlotContext = createContext<TopBarSlotContextValue | null>(null);

export function TopBarSlotProvider({children}: { children: ReactNode }) {
  const [slots, setSlots] = useState<SlotState>({badge: null, actions: null});
  // setSlot is stable across renders so consumers can use it as a ref callback
  // without React detaching/reattaching the ref every render and stomping the
  // recorded element back to null.
  const setSlot = useCallback((name: SlotName, el: HTMLElement | null) => {
    setSlots((prev) => (prev[name] === el ? prev : {...prev, [name]: el}));
  }, []);
  const value = useMemo(() => ({slots, setSlot}), [slots, setSlot]);
  return (
    <TopBarSlotContext.Provider value={value}>
      {children}
    </TopBarSlotContext.Provider>
  );
}

export function TopBarActionsHost({className}: { className?: string }) {
  // Depend on setSlot directly (it is stable) so the ref callback identity
  // does not change every render and React does not stomp the slot back to null.
  const setSlot = useContext(TopBarSlotContext)?.setSlot;
  const setRef = useCallback(
    (el: HTMLDivElement | null) => setSlot?.("actions", el),
    [setSlot],
  );
  return <div ref={setRef} className={className}/>;
}

export function TopBarBadgeHost({className}: { className?: string }) {
  const setSlot = useContext(TopBarSlotContext)?.setSlot;
  const setRef = useCallback(
    (el: HTMLDivElement | null) => setSlot?.("badge", el),
    [setSlot],
  );
  return <div ref={setRef} className={className}/>;
}

function useSlotPortal(slot: SlotName, children: ReactNode) {
  const ctx = useContext(TopBarSlotContext);
  const target = ctx?.slots[slot] ?? null;
  if (!target) return null;
  return createPortal(children, target);
}

export function TopBarActions({children}: { children: ReactNode }) {
  return useSlotPortal("actions", children);
}

export function TopBarBadge({children}: { children: ReactNode }) {
  return useSlotPortal("badge", children);
}
