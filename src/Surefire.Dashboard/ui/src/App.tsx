import { BrowserRouter, Routes, Route, Link } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Layout } from "@/components/layout";
import { DashboardPage } from "@/pages/dashboard";
import { JobsPage } from "@/pages/jobs";
import { JobDetailPage } from "@/pages/job-detail";
import { RunsPage } from "@/pages/runs";
import { RunDetailPage } from "@/pages/run-detail";
import { NodesPage } from "@/pages/nodes";
import { NodeDetailPage } from "@/pages/node-detail";
import { QueuesPage } from "@/pages/queues";

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: false } },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <BrowserRouter
          basename={new URL(document.baseURI).pathname.replace(/\/$/, "")}
        >
          <Routes>
            <Route element={<Layout />}>
              <Route index element={<DashboardPage />} />
              <Route path="jobs" element={<JobsPage />} />
              <Route path="jobs/:name" element={<JobDetailPage />} />
              <Route path="runs" element={<RunsPage />} />
              <Route path="runs/:id" element={<RunDetailPage />} />
              <Route path="queues" element={<QueuesPage />} />
              <Route path="nodes" element={<NodesPage />} />
              <Route path="nodes/:name" element={<NodeDetailPage />} />
              <Route
                path="*"
                element={
                  <div className="space-y-3">
                    <h2 className="text-xl font-semibold tracking-tight">
                      Page not found
                    </h2>
                    <p className="text-sm text-muted-foreground">
                      The requested dashboard route does not exist.
                    </p>
                    <Link
                      to="/"
                      className="text-sm font-medium text-primary hover:underline"
                    >
                      Go to dashboard
                    </Link>
                  </div>
                }
              />
            </Route>
          </Routes>
        </BrowserRouter>
      </TooltipProvider>
      <Toaster />
    </QueryClientProvider>
  );
}
