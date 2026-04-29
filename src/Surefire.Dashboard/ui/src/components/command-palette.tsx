import {useEffect} from "react";
import {useNavigate} from "react-router";
import {useQuery} from "@tanstack/react-query";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command";
import {House, Layers, Play, Server, Workflow} from "lucide-react";
import {api} from "@/lib/api";

export function CommandPalette({
                                 open,
                                 setOpen,
                               }: {
  open: boolean;
  setOpen: (open: boolean) => void;
}) {
  const navigate = useNavigate();

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setOpen(!open);
      }
      if (e.key === "/" && !e.metaKey && !e.ctrlKey && !e.altKey) {
        const tag = (e.target as HTMLElement)?.tagName;
        if (
          tag === "INPUT" ||
          tag === "TEXTAREA" ||
          (e.target as HTMLElement)?.isContentEditable
        )
          return;
        e.preventDefault();
        setOpen(true);
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open, setOpen]);

  const {data: jobs} = useQuery({
    queryKey: ["jobs"],
    queryFn: () => api.getJobs(),
    enabled: open,
    staleTime: 30_000,
  });

  const {data: nodes} = useQuery({
    queryKey: ["nodes"],
    queryFn: () => api.getNodes(),
    enabled: open,
    staleTime: 30_000,
  });

  const go = (path: string) => {
    setOpen(false);
    navigate(path);
  };

  return (
    <CommandDialog
      open={open}
      onOpenChange={setOpen}
      title="Command palette"
      description="Navigate or search"
      className="bg-background/80 backdrop-blur-xl border-border/50 shadow-2xl"
    >
      <CommandInput placeholder="Search..."/>
      <CommandList className="max-h-85">
        <CommandEmpty>No results found.</CommandEmpty>
        <CommandGroup heading="Navigation">
          <CommandItem onSelect={() => go("/")}>
            <House className="size-4 opacity-60"/>
            Dashboard
          </CommandItem>
          <CommandItem onSelect={() => go("/jobs")}>
            <Workflow className="size-4 opacity-60"/>
            Jobs
          </CommandItem>
          <CommandItem onSelect={() => go("/runs")}>
            <Play className="size-4 opacity-60"/>
            Runs
          </CommandItem>
          <CommandItem onSelect={() => go("/queues")}>
            <Layers className="size-4 opacity-60"/>
            Queues
          </CommandItem>
          <CommandItem onSelect={() => go("/nodes")}>
            <Server className="size-4 opacity-60"/>
            Nodes
          </CommandItem>
        </CommandGroup>
        {jobs && jobs.length > 0 && (
          <>
            <CommandSeparator/>
            <CommandGroup heading="Jobs">
              {jobs.map((job) => (
                <CommandItem
                  key={job.name}
                  onSelect={() => go(`/jobs/${encodeURIComponent(job.name)}`)}
                >
                  <Workflow className="size-4 opacity-60"/>
                  <span className="truncate">{job.name}</span>
                  <span className="ml-auto text-xs text-muted-foreground/70">
                    {job.isContinuous
                      ? "Continuous"
                      : (job.cronExpression ?? "Manual")}
                  </span>
                </CommandItem>
              ))}
            </CommandGroup>
          </>
        )}
        {nodes && nodes.length > 0 && (
          <>
            <CommandSeparator/>
            <CommandGroup heading="Nodes">
              {nodes.map((node) => (
                <CommandItem
                  key={node.name}
                  onSelect={() => go(`/nodes/${encodeURIComponent(node.name)}`)}
                >
                  <Server className="size-4 opacity-60"/>
                  <span className="truncate">{node.name}</span>
                  <span className="ml-auto text-xs text-muted-foreground/70 tabular-nums">
                    {node.runningCount} running
                  </span>
                </CommandItem>
              ))}
            </CommandGroup>
          </>
        )}
      </CommandList>
    </CommandDialog>
  );
}
