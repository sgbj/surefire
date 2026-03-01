import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { CommandDialog, CommandInput, CommandList, CommandEmpty, CommandGroup, CommandItem, CommandSeparator } from '@/components/ui/command';
import { House, Workflow, Play, Server } from 'lucide-react';
import { api } from '@/lib/api';

export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const navigate = useNavigate();

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        setOpen(prev => !prev);
      }
      if (e.key === '/' && !e.metaKey && !e.ctrlKey && !e.altKey) {
        const tag = (e.target as HTMLElement)?.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || (e.target as HTMLElement)?.isContentEditable) return;
        e.preventDefault();
        setOpen(true);
      }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, []);

  const { data: jobs } = useQuery({
    queryKey: ['jobs'],
    queryFn: () => api.getJobs(),
    staleTime: 30_000,
  });

  const { data: nodes } = useQuery({
    queryKey: ['nodes'],
    queryFn: () => api.getNodes(),
    staleTime: 30_000,
  });

  const go = (path: string) => {
    setOpen(false);
    setSearch('');
    navigate(path);
  };

  return (
    <CommandDialog open={open} onOpenChange={setOpen} title="Command Palette" description="Navigate or search">
      <CommandInput placeholder="Search jobs, nodes..." value={search} onValueChange={setSearch} />
      <CommandList>
        <CommandEmpty>No results found.</CommandEmpty>
        <CommandGroup heading="Navigation">
          <CommandItem onSelect={() => go('/')}>
            <House className="h-4 w-4" />
            Dashboard
          </CommandItem>
          <CommandItem onSelect={() => go('/jobs')}>
            <Workflow className="h-4 w-4" />
            Jobs
          </CommandItem>
          <CommandItem onSelect={() => go('/runs')}>
            <Play className="h-4 w-4" />
            Runs
          </CommandItem>
          <CommandItem onSelect={() => go('/nodes')}>
            <Server className="h-4 w-4" />
            Nodes
          </CommandItem>
        </CommandGroup>
        {jobs && jobs.length > 0 && (
          <>
            <CommandSeparator />
            <CommandGroup heading="Jobs">
              {jobs.map(job => (
                <CommandItem key={job.name} onSelect={() => go(`/jobs/${encodeURIComponent(job.name)}`)}>
                  <Workflow className="h-4 w-4" />
                  <span>{job.name}</span>
                  {job.cronExpression && <span className="ml-auto text-xs text-muted-foreground">{job.cronExpression}</span>}
                </CommandItem>
              ))}
            </CommandGroup>
          </>
        )}
        {nodes && nodes.length > 0 && (
          <>
            <CommandSeparator />
            <CommandGroup heading="Nodes">
              {nodes.map(node => (
                <CommandItem key={node.name} onSelect={() => go(`/nodes/${encodeURIComponent(node.name)}`)}>
                  <Server className="h-4 w-4" />
                  <span>{node.name}</span>
                  <span className="ml-auto text-xs text-muted-foreground">{node.status === 0 ? 'Online' : 'Offline'}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          </>
        )}
      </CommandList>
    </CommandDialog>
  );
}
