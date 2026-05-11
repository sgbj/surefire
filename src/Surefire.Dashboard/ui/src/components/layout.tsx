import {Outlet} from "react-router";
import {useCallback, useEffect, useState} from "react";
import {AppRail, MobileRail, NavTopBar} from "@/components/app-shell";
import {CommandPalette} from "@/components/command-palette";
import {useCommandPalette} from "@/hooks/use-command-palette";
import {TopBarSlotProvider} from "@/components/topbar-slot";

export type Theme = "system" | "light" | "dark";

function resolveTheme(theme: Theme): boolean {
  if (theme === "system")
    return window.matchMedia("(prefers-color-scheme: dark)").matches;
  return theme === "dark";
}

export function Layout() {
  const [theme, setTheme] = useState<Theme>(() => {
    const stored = localStorage.getItem("theme");
    if (stored === "light" || stored === "dark") return stored;
    return "system";
  });

  const applyTheme = useCallback((t: Theme) => {
    document.documentElement.classList.toggle("dark", resolveTheme(t));
  }, []);

  useEffect(() => {
    applyTheme(theme);
    if (theme === "system") {
      localStorage.removeItem("theme");
    } else {
      localStorage.setItem("theme", theme);
    }
  }, [theme, applyTheme]);

  useEffect(() => {
    if (theme !== "system") return;
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = () => applyTheme("system");
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, [theme, applyTheme]);

  const cycleTheme = () => {
    setTheme((prev) => {
      if (prev === "system") return "light";
      if (prev === "light") return "dark";
      return "system";
    });
  };

  const commandPalette = useCommandPalette();
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <TopBarSlotProvider>
      <div className="relative flex h-svh w-full">
        <AppRail
          theme={theme}
          onCycleTheme={cycleTheme}
          onOpenSearch={commandPalette.toggle}
        />

        <MobileRail
          open={mobileOpen}
          setOpen={setMobileOpen}
          theme={theme}
          onCycleTheme={cycleTheme}
          onOpenSearch={commandPalette.toggle}
        />

        <div className="relative flex min-w-0 flex-1 flex-col md:pl-14">
          <NavTopBar onOpenMobile={() => setMobileOpen(true)}/>
          <main className="relative z-10 flex-1 flex flex-col min-h-0 overflow-y-auto">
            <Outlet/>
          </main>
        </div>

        <CommandPalette
          open={commandPalette.open}
          setOpen={commandPalette.setOpen}
        />
      </div>
    </TopBarSlotProvider>
  );
}
