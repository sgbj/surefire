import {
  BookOpen,
  CodeXml,
  House,
  Layers,
  Menu,
  Monitor,
  Moon,
  Play,
  Search,
  Server,
  Sun,
  Workflow,
} from "lucide-react";
import { Link, useLocation } from "react-router";
import { useEffect } from "react";

import { cn } from "@/lib/utils";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { TopBarActionsHost, TopBarBadgeHost } from "@/components/topbar-slot";
import type { Theme } from "@/components/layout";

interface NavItem {
  title: string;
  url: string;
  icon: typeof House;
}

const navItems: NavItem[] = [
  { title: "Dashboard", url: "/", icon: House },
  { title: "Jobs", url: "/jobs", icon: Workflow },
  { title: "Runs", url: "/runs", icon: Play },
  { title: "Queues", url: "/queues", icon: Layers },
  { title: "Nodes", url: "/nodes", icon: Server },
];

function isActive(pathname: string, url: string) {
  return url === "/" ? pathname === "/" : pathname.startsWith(url);
}

interface RailProps {
  theme: Theme;
  onCycleTheme: () => void;
  onOpenSearch: () => void;
}

export function AppRail({ theme, onCycleTheme, onOpenSearch }: RailProps) {
  const location = useLocation();
  const ThemeIcon = theme === "dark" ? Moon : theme === "light" ? Sun : Monitor;

  return (
    <aside className="hidden md:flex fixed inset-y-0 left-0 z-30 w-14 flex-col items-center border-r border-sidebar-border bg-sidebar/85 backdrop-blur-sm">
      <Link
        to="/"
        aria-label="Surefire"
        className="mt-4 mb-4 flex h-9 w-9 items-center justify-center opacity-90 transition-opacity hover:opacity-100"
      >
        <img
          src={`${import.meta.env.BASE_URL}surefire.svg`}
          alt="Surefire"
          className="size-5"
        />
      </Link>

      <nav className="flex flex-col items-center gap-1.5">
        {navItems.map((item) => {
          const active = isActive(location.pathname, item.url);
          return (
            <Tooltip key={item.title}>
              <TooltipTrigger asChild>
                <Link
                  to={item.url}
                  className={cn(
                    "group relative flex h-9 w-9 items-center justify-center rounded-md transition-colors",
                    active
                      ? "text-foreground bg-sidebar-accent"
                      : "text-muted-foreground hover:text-foreground hover:bg-sidebar-accent",
                  )}
                  aria-label={item.title}
                >
                  {active && (
                    <span
                      aria-hidden
                      className="absolute -left-3 top-1/2 h-5 w-0.5 -translate-y-1/2 rounded-full bg-accent-brand"
                    />
                  )}
                  <item.icon className="size-4.5" />
                </Link>
              </TooltipTrigger>
              <TooltipContent side="right">{item.title}</TooltipContent>
            </Tooltip>
          );
        })}
      </nav>

      <div className="mt-auto mb-3 flex flex-col items-center gap-1.5">
        <Tooltip>
          <TooltipTrigger asChild>
            <button
              type="button"
              onClick={onOpenSearch}
              className="flex h-9 w-9 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-foreground"
              aria-label="Search"
            >
              <Search className="size-4.5" />
            </button>
          </TooltipTrigger>
          <TooltipContent side="right">Search</TooltipContent>
        </Tooltip>
        <Tooltip>
          <TooltipTrigger asChild>
            <button
              type="button"
              onClick={onCycleTheme}
              className="flex h-9 w-9 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-foreground"
              aria-label="Theme"
            >
              <ThemeIcon className="size-4.5" />
            </button>
          </TooltipTrigger>
          <TooltipContent side="right">Theme</TooltipContent>
        </Tooltip>
        <Tooltip>
          <TooltipTrigger asChild>
            <a
              href="https://batary.dev/surefire"
              target="_blank"
              rel="noopener noreferrer"
              className="flex h-9 w-9 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-foreground"
              aria-label="Docs"
            >
              <BookOpen className="size-4.5" />
            </a>
          </TooltipTrigger>
          <TooltipContent side="right">Docs</TooltipContent>
        </Tooltip>
        <Tooltip>
          <TooltipTrigger asChild>
            <a
              href="https://github.com/sgbj/surefire"
              target="_blank"
              rel="noopener noreferrer"
              className="flex h-9 w-9 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-foreground"
              aria-label="GitHub"
            >
              <CodeXml className="size-4.5" />
            </a>
          </TooltipTrigger>
          <TooltipContent side="right">GitHub</TooltipContent>
        </Tooltip>
      </div>
    </aside>
  );
}

interface MobileRailProps extends RailProps {
  open: boolean;
  setOpen: (open: boolean) => void;
}

export function MobileRail({
  open,
  setOpen,
  theme,
  onCycleTheme,
  onOpenSearch,
}: MobileRailProps) {
  const location = useLocation();
  const ThemeIcon = theme === "dark" ? Moon : theme === "light" ? Sun : Monitor;

  useEffect(() => {
    setOpen(false);
  }, [location.pathname, setOpen]);

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetContent
        side="left"
        className="w-72 bg-sidebar/95 p-0 backdrop-blur-md"
      >
        <SheetHeader className="sr-only">
          <SheetTitle>Navigation</SheetTitle>
          <SheetDescription>
            Surefire dashboard navigation menu.
          </SheetDescription>
        </SheetHeader>
        <div className="flex h-full flex-col">
          <div className="flex items-center gap-3 border-b border-sidebar-border px-5 py-4">
            <img
              src={`${import.meta.env.BASE_URL}surefire.svg`}
              alt=""
              className="size-5"
            />
            <span className="text-base font-semibold tracking-tight">
              Surefire
            </span>
          </div>

          <nav className="flex flex-col gap-px p-3">
            {navItems.map((item) => {
              const active = isActive(location.pathname, item.url);
              return (
                <Link
                  key={item.title}
                  to={item.url}
                  className={cn(
                    "group flex items-center gap-3 rounded-md px-3 py-2.5 text-sm transition-colors",
                    active
                      ? "bg-sidebar-accent text-foreground"
                      : "text-muted-foreground hover:bg-sidebar-accent/60 hover:text-foreground",
                  )}
                >
                  <item.icon className="size-4 opacity-80" />
                  <span className={active ? "font-medium" : ""}>
                    {item.title}
                  </span>
                </Link>
              );
            })}
          </nav>

          <div className="mt-auto border-t border-sidebar-border p-3">
            <button
              type="button"
              onClick={() => {
                setOpen(false);
                onOpenSearch();
              }}
              className="flex w-full items-center gap-3 rounded-md px-3 py-2.5 text-sm text-muted-foreground transition-colors hover:bg-sidebar-accent/60 hover:text-foreground"
            >
              <Search className="size-4 opacity-80" />
              Search
            </button>
            <button
              type="button"
              onClick={onCycleTheme}
              className="flex w-full items-center gap-3 rounded-md px-3 py-2.5 text-sm text-muted-foreground transition-colors hover:bg-sidebar-accent/60 hover:text-foreground"
            >
              <ThemeIcon className="size-4 opacity-80" />
              Theme
            </button>
            <a
              href="https://batary.dev/surefire"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-3 rounded-md px-3 py-2.5 text-sm text-muted-foreground transition-colors hover:bg-sidebar-accent/60 hover:text-foreground"
            >
              <BookOpen className="size-4 opacity-80" />
              Docs
            </a>
            <a
              href="https://github.com/sgbj/surefire"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-3 rounded-md px-3 py-2.5 text-sm text-muted-foreground transition-colors hover:bg-sidebar-accent/60 hover:text-foreground"
            >
              <CodeXml className="size-4 opacity-80" />
              GitHub
            </a>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
}

interface TopBarProps {
  onOpenMobile: () => void;
}

export function NavTopBar({ onOpenMobile }: TopBarProps) {
  const location = useLocation();
  const breadcrumb = computeBreadcrumb(location.pathname);

  return (
    <header className="sticky top-0 z-20 flex h-14 items-center gap-3 border-b border-sidebar-border bg-sidebar/85 px-6 backdrop-blur-sm">
      <button
        type="button"
        onClick={onOpenMobile}
        className="md:hidden -ml-1 flex size-9 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
        aria-label="Menu"
      >
        <Menu className="size-5" />
      </button>

      <Link to="/" className="md:hidden flex items-center gap-2">
        <img
          src={`${import.meta.env.BASE_URL}surefire.svg`}
          alt=""
          className="size-5"
        />
      </Link>

      <div className="flex min-w-0 flex-1 items-center gap-3">
        <Breadcrumbs items={breadcrumb} />
        <TopBarBadgeHost className="flex items-center gap-2" />
      </div>

      <TopBarActionsHost className="flex items-center gap-2" />
    </header>
  );
}

interface BreadcrumbItem {
  label: string;
  to?: string;
}

function computeBreadcrumb(pathname: string): BreadcrumbItem[] {
  const trimmed = pathname.replace(/\/$/, "");
  if (!trimmed) return [{ label: "Dashboard" }];
  const parts = trimmed.split("/").filter(Boolean);
  if (parts.length === 0) return [{ label: "Dashboard" }];

  const items: BreadcrumbItem[] = [];
  const knownTop: Record<string, string> = {
    jobs: "Jobs",
    runs: "Runs",
    queues: "Queues",
    nodes: "Nodes",
  };
  const top = parts[0];
  items.push({
    label: knownTop[top] ?? top,
    to: parts.length === 1 ? undefined : `/${top}`,
  });
  if (parts.length > 1) {
    const tail = decodeURIComponent(parts.slice(1).join("/"));
    items.push({ label: tail });
  }
  return items;
}

function Breadcrumbs({ items }: { items: BreadcrumbItem[] }) {
  return (
    <nav
      aria-label="Breadcrumb"
      className="flex min-w-0 items-center gap-2 text-sm"
    >
      {items.map((item, i) => {
        const last = i === items.length - 1;
        return (
          <span
            key={i}
            className={cn("flex items-center gap-2", last && "min-w-0")}
          >
            {i > 0 && (
              <span className="shrink-0 text-muted-foreground/40">/</span>
            )}
            {item.to && !last ? (
              <Link
                to={item.to}
                className="shrink-0 text-muted-foreground hover:text-foreground transition-colors"
              >
                {item.label}
              </Link>
            ) : (
              <span
                className={cn(
                  last
                    ? "truncate font-medium text-foreground"
                    : "shrink-0 text-muted-foreground",
                )}
                title={item.label}
              >
                {item.label}
              </span>
            )}
          </span>
        );
      })}
    </nav>
  );
}
