import {BrowserRouter, Link, Route, Routes} from "react-router";
import {QueryClient, QueryClientProvider} from "@tanstack/react-query";
import {Toaster} from "@/components/ui/sonner";
import {TooltipProvider} from "@/components/ui/tooltip";
import {Layout} from "@/components/layout";
import {ErrorBoundary} from "@/components/error-boundary";
import {DashboardPage} from "@/pages/dashboard";
import {JobsPage} from "@/pages/jobs";
import {JobDetailPage} from "@/pages/job-detail";
import {RunsPage} from "@/pages/runs";
import {RunDetailPage} from "@/pages/run-detail";
import {NodesPage} from "@/pages/nodes";
import {NodeDetailPage} from "@/pages/node-detail";
import {QueuesPage} from "@/pages/queues";

const queryClient = new QueryClient({
  defaultOptions: {queries: {retry: 1, refetchOnWindowFocus: false}},
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <BrowserRouter
          basename={new URL(document.baseURI).pathname.replace(/\/$/, "")}
        >
          <ErrorBoundary>
            <Routes>
              <Route element={<Layout/>}>
                <Route index element={<DashboardPage/>}/>
                <Route path="jobs" element={<JobsPage/>}/>
                <Route path="jobs/:name" element={<JobDetailPage/>}/>
                <Route path="runs" element={<RunsPage/>}/>
                <Route path="runs/:id" element={<RunDetailPage/>}/>
                <Route path="queues" element={<QueuesPage/>}/>
                <Route path="nodes" element={<NodesPage/>}/>
                <Route path="nodes/:name" element={<NodeDetailPage/>}/>
                <Route
                  path="*"
                  element={
                    <div className="max-w-prose space-y-4 py-12">
                      <div className="eyebrow flex items-center text-accent-brand">
                                                404 · not found
                      </div>
                      <h2 className="text-3xl font-semibold tracking-tight">
                        This route doesn't exist.
                      </h2>
                      <p className="text-muted-foreground">
                        The requested dashboard page does not exist.
                      </p>
                      <Link
                        to="/"
                        className="inline-flex items-center gap-2 text-accent-brand hover:underline eyebrow"
                      >
                        ← back to console
                      </Link>
                    </div>
                  }
                />
              </Route>
            </Routes>
          </ErrorBoundary>
        </BrowserRouter>
      </TooltipProvider>
      <Toaster/>
    </QueryClientProvider>
  );
}
