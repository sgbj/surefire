import { House, Workflow, Play, Server, Layers, Search, Moon, Sun, Monitor, BookOpen, Github } from "lucide-react"
import { Link } from "react-router"

import { NavMain } from "@/components/nav-main"
import { Button } from "@/components/ui/button"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar"
import type { Theme } from "@/components/layout"

const navItems = [
  { title: "Dashboard", url: "/", icon: House },
  { title: "Jobs", url: "/jobs", icon: Workflow },
  { title: "Runs", url: "/runs", icon: Play },
  { title: "Queues", url: "/queues", icon: Layers },
  { title: "Nodes", url: "/nodes", icon: Server },
]

interface AppSidebarProps extends React.ComponentProps<typeof Sidebar> {
  theme: Theme
  onCycleTheme: () => void
  onOpenSearch: () => void
}

export function AppSidebar({ theme, onCycleTheme, onOpenSearch, ...props }: AppSidebarProps) {
  const ThemeIcon = theme === "dark" ? Moon : theme === "light" ? Sun : Monitor
  const themeLabel = theme === "system" ? "System" : theme === "dark" ? "Dark" : "Light"

  return (
    <Sidebar collapsible="offcanvas" {...props}>
      <SidebarHeader className="border-b border-sidebar-border px-3 py-2.5">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              size="lg"
              asChild
              className="data-[slot=sidebar-menu-button]:!p-1.5 data-[slot=sidebar-menu-button]:!h-10"
            >
              <Link to="/">
                <img src={`${import.meta.env.BASE_URL}surefire.svg`} alt="" className="!size-6" />
                <span className="text-base font-semibold tracking-tight">Surefire</span>
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <NavMain items={navItems} />
      </SidebarContent>
      <SidebarFooter className="flex-row items-center justify-center gap-1 border-t border-sidebar-border px-3 py-2.5">
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="size-8"
              onClick={onOpenSearch}
            >
              <Search className="size-4 opacity-60" />
              <span className="sr-only">Search</span>
            </Button>
          </TooltipTrigger>
          <TooltipContent side="top">Search</TooltipContent>
        </Tooltip>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="size-8"
              onClick={onCycleTheme}
            >
              <ThemeIcon className="size-4 opacity-60" />
              <span className="sr-only">{themeLabel}</span>
            </Button>
          </TooltipTrigger>
          <TooltipContent side="top">{themeLabel}</TooltipContent>
        </Tooltip>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button variant="ghost" size="icon" className="size-8" asChild>
              <a href="https://batary.dev/surefire" target="_blank" rel="noopener noreferrer">
                <BookOpen className="size-4 opacity-60" />
                <span className="sr-only">Docs</span>
              </a>
            </Button>
          </TooltipTrigger>
          <TooltipContent side="top">Docs</TooltipContent>
        </Tooltip>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button variant="ghost" size="icon" className="size-8" asChild>
              <a href="https://github.com/sgbj/surefire" target="_blank" rel="noopener noreferrer">
                <Github className="size-4 opacity-60" />
                <span className="sr-only">GitHub</span>
              </a>
            </Button>
          </TooltipTrigger>
          <TooltipContent side="top">GitHub</TooltipContent>
        </Tooltip>
      </SidebarFooter>
    </Sidebar>
  )
}
