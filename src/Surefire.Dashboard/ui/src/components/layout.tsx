import { Outlet, Link } from "react-router"
import { useState, useEffect, useCallback } from "react"
import { Flame } from "lucide-react"
import { SidebarInset, SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar"
import { AppSidebar } from "@/components/app-sidebar"
import { CommandPalette } from "@/components/command-palette"

type Theme = "system" | "light" | "dark"

function resolveTheme(theme: Theme): boolean {
  if (theme === "system") return window.matchMedia("(prefers-color-scheme: dark)").matches
  return theme === "dark"
}

export function Layout() {
  const [theme, setTheme] = useState<Theme>(() => {
    if (typeof window !== "undefined") {
      const stored = localStorage.getItem("theme")
      if (stored === "light" || stored === "dark") return stored
    }
    return "system"
  })

  const applyTheme = useCallback((t: Theme) => {
    document.documentElement.classList.toggle("dark", resolveTheme(t))
  }, [])

  useEffect(() => {
    applyTheme(theme)
    if (theme === "system") {
      localStorage.removeItem("theme")
    } else {
      localStorage.setItem("theme", theme)
    }
  }, [theme, applyTheme])

  useEffect(() => {
    if (theme !== "system") return
    const mq = window.matchMedia("(prefers-color-scheme: dark)")
    const handler = () => applyTheme("system")
    mq.addEventListener("change", handler)
    return () => mq.removeEventListener("change", handler)
  }, [theme, applyTheme])

  const cycleTheme = () => {
    setTheme(prev => {
      if (prev === "system") return "light"
      if (prev === "light") return "dark"
      return "system"
    })
  }

  return (
    <SidebarProvider>
      <AppSidebar theme={theme} onCycleTheme={cycleTheme} />
      <SidebarInset className="min-w-0">
        <header className="sticky top-0 z-40 flex h-12 shrink-0 items-center gap-2 border-b bg-background/80 backdrop-blur-sm px-4 md:hidden">
          <SidebarTrigger />
          <Link to="/" className="flex items-center gap-1.5">
            <Flame className="size-5 text-primary fill-primary" />
            <span className="text-sm font-semibold tracking-tight">Surefire</span>
          </Link>
        </header>
        <div className="p-4 md:p-6 lg:p-8 overflow-x-clip">
          <Outlet />
        </div>
      </SidebarInset>
      <CommandPalette />
    </SidebarProvider>
  )
}
