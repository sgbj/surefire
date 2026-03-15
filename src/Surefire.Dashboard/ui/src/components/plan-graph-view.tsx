import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router";
import {
  ReactFlow,
  type Node,
  type Edge,
  type NodeProps,
  type Viewport,
  Handle,
  Position,
  useReactFlow,
  useOnViewportChange,
  ReactFlowProvider,
  type ColorMode,
} from "@xyflow/react";
import ELK, { type ElkNode } from "elkjs/lib/elk.bundled.js";
import { StatusBadge } from "@/components/status-badge";
import { formatDuration } from "@/lib/format";
import { JobStatusLabels, type JobRun } from "@/lib/api";
import "@xyflow/react/dist/style.css";

// ── Types ──────────────────────────────────────────────────────────────

interface PlanStep {
  type: string;
  id: string;
  name?: string;
  dependsOn: string[];
  jobName?: string;
  fallbackJobName?: string;
  sourceStepId?: string;
  template?: PlanGraph;
  isStreaming?: boolean;
  sourceStepIds?: string[];
  branches?: { conditionStepId: string; branch: PlanGraph }[];
  elseBranch?: PlanGraph;
  keyStepId?: string;
  cases?: { value: string; branch: PlanGraph }[];
  defaultBranch?: PlanGraph;
  planName?: string;
  signalName?: string;
  operatorType?: string;
}

interface PlanGraph {
  steps: PlanStep[];
  outputStepId?: string;
}

interface StepNodeData {
  step: PlanStep;
  run?: JobRun;
  isOutput: boolean;
  isHighlighted: boolean;
  isTemplate: boolean;
  templateLabel?: string;
  forEachStats?: ForEachStats;
  [key: string]: unknown;
}

interface ForEachStats {
  total: number;
  byStatus: Record<number, number>;
}

type StepNode = Node<StepNodeData>;

// ── Layout ─────────────────────────────────────────────────────────────

const elk = new ELK();

const NODE_WIDTH = 220;
const NODE_HEIGHT = 60;
const FOREACH_NODE_HEIGHT = 80;

async function computeLayout(
  nodes: StepNode[],
  edges: Edge[],
): Promise<Map<string, { x: number; y: number }>> {
  const elkGraph: ElkNode = {
    id: "root",
    layoutOptions: {
      "elk.algorithm": "layered",
      "elk.direction": "RIGHT",
      "elk.spacing.nodeNode": "40",
      "elk.layered.spacing.nodeNodeBetweenLayers": "60",
      "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
      "elk.edgeRouting": "POLYLINE",
    },
    children: nodes.map((n) => ({
      id: n.id,
      width: NODE_WIDTH,
      height:
        n.data.step.type === "forEach" ? FOREACH_NODE_HEIGHT : NODE_HEIGHT,
    })),
    edges: edges.map((e) => ({
      id: e.id,
      sources: [e.source],
      targets: [e.target],
    })),
  };

  const layout = await elk.layout(elkGraph);

  const positions = new Map<string, { x: number; y: number }>();
  for (const child of layout.children ?? []) {
    positions.set(child.id, { x: child.x ?? 0, y: child.y ?? 0 });
  }
  return positions;
}

// ── Status colors for node borders ─────────────────────────────────────

const statusBorderColor: Record<number, string> = {
  0: "var(--status-pending)",
  1: "var(--status-running)",
  2: "var(--status-completed)",
  3: "var(--status-failed)",
  4: "var(--status-cancelled)",
  5: "var(--status-dead-letter)",
  6: "var(--status-skipped)",
};

const stepTypeLabel: Record<string, string> = {
  run: "Job",
  stream: "Stream",
  forEach: "ForEach",
  whenAll: "WhenAll",
  if: "If",
  switch: "Switch",
  signal: "Signal",
  input: "Input",
  operator: "Operator",
};

// ── Custom Node ────────────────────────────────────────────────────────

function StepNodeComponent({ data }: NodeProps<StepNode>) {
  const {
    step,
    run,
    isOutput,
    isHighlighted,
    isTemplate,
    templateLabel,
    forEachStats,
  } = data;

  const borderColor = run
    ? (statusBorderColor[run.status] ?? "var(--border)")
    : "var(--border)";

  const label =
    step.name ??
    step.jobName ??
    step.planName ??
    step.signalName ??
    step.operatorType ??
    step.id;
  const typeLabel = templateLabel
    ? `${stepTypeLabel[step.type] ?? step.type} · ${templateLabel}`
    : (stepTypeLabel[step.type] ?? step.type);

  return (
    <>
      <Handle
        type="target"
        position={Position.Left}
        className="!w-2 !h-2 !bg-border !border-background"
      />
      <div
        className={`rounded-lg border-2 px-3 py-2 shadow-sm text-card-foreground transition-colors ${isTemplate ? "bg-card/60" : "bg-card"}`}
        style={{
          borderColor,
          borderStyle: isTemplate ? "dashed" : "solid",
          width: NODE_WIDTH,
          minHeight:
            step.type === "forEach" ? FOREACH_NODE_HEIGHT : NODE_HEIGHT,
          ...(isHighlighted
            ? {
                boxShadow: `0 0 0 3px color-mix(in oklch, ${borderColor !== "var(--border)" ? borderColor : "var(--primary)"} 40%, transparent)`,
              }
            : {}),
        }}
      >
        <div className="flex items-center gap-1.5 justify-between">
          <span
            className="text-[13px] font-medium truncate leading-tight"
            title={label}
          >
            {label}
          </span>
          {isOutput && (
            <span className="text-[10px] text-muted-foreground border rounded px-1 shrink-0">
              out
            </span>
          )}
        </div>
        <div className="flex items-center gap-1.5 mt-1">
          <span className="text-[11px] text-muted-foreground">{typeLabel}</span>
          {run && (
            <>
              <StatusBadge status={run.status} />
              {run.startedAt && (
                <span className="text-[11px] text-muted-foreground tabular-nums">
                  {formatDuration(run.startedAt, run.completedAt ?? undefined)}
                </span>
              )}
            </>
          )}
        </div>
        {forEachStats && forEachStats.total > 0 && (
          <ForEachProgress stats={forEachStats} />
        )}
      </div>
      <Handle
        type="source"
        position={Position.Right}
        className="!w-2 !h-2 !bg-border !border-background"
      />
    </>
  );
}

function ForEachProgress({ stats }: { stats: ForEachStats }) {
  const completed = stats.byStatus[2] ?? 0;
  const failed = (stats.byStatus[3] ?? 0) + (stats.byStatus[5] ?? 0);
  const running = stats.byStatus[1] ?? 0;
  const pending = stats.byStatus[0] ?? 0;
  const skipped = stats.byStatus[6] ?? 0;
  const cancelled = stats.byStatus[4] ?? 0;

  const segments = [
    { count: completed, color: "var(--status-completed)" },
    { count: running, color: "var(--status-running)" },
    { count: failed, color: "var(--status-failed)" },
    { count: cancelled, color: "var(--status-cancelled)" },
    { count: skipped, color: "var(--status-skipped)" },
    { count: pending, color: "var(--status-pending)" },
  ].filter((s) => s.count > 0);

  const parts: string[] = [];
  if (completed > 0) parts.push(`${completed} done`);
  if (running > 0) parts.push(`${running} running`);
  if (failed > 0) parts.push(`${failed} failed`);
  if (pending > 0) parts.push(`${pending} pending`);
  if (cancelled > 0) parts.push(`${cancelled} cancelled`);
  if (skipped > 0) parts.push(`${skipped} skipped`);

  return (
    <div className="mt-1.5">
      <div className="flex h-1.5 rounded-full overflow-hidden bg-muted">
        {segments.map((seg, i) => (
          <div
            key={i}
            style={{
              width: `${(seg.count / stats.total) * 100}%`,
              backgroundColor: seg.color,
            }}
          />
        ))}
      </div>
      <div className="text-[10px] text-muted-foreground mt-0.5 tabular-nums">
        {parts.join(" \u00b7 ")} ({stats.total})
      </div>
    </div>
  );
}

// ── Graph building ─────────────────────────────────────────────────────

/** Build the structural graph (nodes + edges) from the plan definition.
 *  This is pure structure — no run data — so it only changes when the plan
 *  graph JSON changes, not on every poll tick.
 *  Expands ForEach/If/Switch templates inline so the full plan shape is visible. */
function buildStructure(graph: PlanGraph): {
  nodes: StepNode[];
  edges: Edge[];
} {
  const nodes: StepNode[] = [];
  const edges: Edge[] = [];
  const edgeKeys = new Set<string>();

  const addEdge = (source: string, target: string, suffix: string) => {
    const key = `${source}->${target}`;
    if (edgeKeys.has(key)) return;
    edgeKeys.add(key);
    edges.push({
      id: `${key}:${suffix}`,
      source,
      target,
      animated: false,
    });
  };

  // Collect all top-level step IDs so we can detect cross-graph refs in templates
  const topLevelStepIds = new Set(graph.steps.map((s) => s.id));

  function addTemplateSteps(
    template: PlanGraph,
    parentStepId: string,
    label: string,
    branchKey?: string,
  ) {
    // Template steps use "{parentStepId}[{branchKey}].{templateStepId}" IDs
    // branchKey disambiguates when multiple branches share the same template step IDs
    // (e.g. Switch cases each have their own sub-PlanBuilder starting from step_0)
    const prefix = branchKey ? `${parentStepId}[${branchKey}]` : parentStepId;
    const templateStepIds = new Set(template.steps.map((s) => s.id));

    for (const tStep of template.steps) {
      const nodeId = `${prefix}.${tStep.id}`;
      nodes.push({
        id: nodeId,
        type: "step",
        position: { x: 0, y: 0 },
        data: {
          step: tStep,
          isOutput: false,
          isHighlighted: false,
          isTemplate: true,
          templateLabel: label,
        },
      });

      // Edges from template deps
      let hasInternalDep = false;
      if (tStep.dependsOn.length > 0) {
        for (const depId of tStep.dependsOn) {
          if (templateStepIds.has(depId) && depId !== tStep.id) {
            // Dep is a different step within this template — scope it
            addEdge(`${prefix}.${depId}`, nodeId, "tdep");
            hasInternalDep = true;
          } else if (topLevelStepIds.has(depId)) {
            // Cross-graph ref to a parent step — connect directly
            addEdge(depId, nodeId, "txdep");
          } else {
            // Fallback: scope it (shouldn't happen normally)
            addEdge(`${prefix}.${depId}`, nodeId, "tdep");
            hasInternalDep = true;
          }
        }
      }

      // Template entry points (no internal deps) connect from the parent control-flow step
      if (!hasInternalDep) {
        addEdge(parentStepId, nodeId, "troot");
      }

      // Implicit edges within template
      if (tStep.type === "operator" && tStep.sourceStepId) {
        if (templateStepIds.has(tStep.sourceStepId)) {
          addEdge(`${prefix}.${tStep.sourceStepId}`, nodeId, "topsrc");
        } else if (topLevelStepIds.has(tStep.sourceStepId)) {
          addEdge(tStep.sourceStepId, nodeId, "topsrc");
        }
      }
    }
  }

  for (const step of graph.steps) {
    nodes.push({
      id: step.id,
      type: "step",
      position: { x: 0, y: 0 },
      data: {
        step,
        isOutput: step.id === graph.outputStepId,
        isHighlighted: false,
        isTemplate: false,
      },
    });

    for (const depId of step.dependsOn) {
      addEdge(depId, step.id, "dep");
    }

    // Implicit edges for control-flow steps (not always in dependsOn)
    if (step.type === "forEach" && step.sourceStepId) {
      addEdge(step.sourceStepId, step.id, "src");
      if (step.template) {
        addTemplateSteps(step.template, step.id, "per item");
      }
    }
    if (step.type === "if") {
      if (step.branches) {
        for (let i = 0; i < step.branches.length; i++) {
          const b = step.branches[i];
          addEdge(b.conditionStepId, step.id, "cond");
          addTemplateSteps(b.branch, step.id, "then", `then${i}`);
        }
      }
      if (step.elseBranch) {
        addTemplateSteps(step.elseBranch, step.id, "else", "else");
      }
    }
    if (step.type === "switch") {
      if (step.keyStepId) addEdge(step.keyStepId, step.id, "key");
      if (step.cases) {
        for (const c of step.cases) {
          addTemplateSteps(c.branch, step.id, c.value, c.value);
        }
      }
      if (step.defaultBranch) {
        addTemplateSteps(step.defaultBranch, step.id, "default", "default");
      }
    }
    if (step.type === "whenAll" && step.sourceStepIds) {
      for (const sourceId of step.sourceStepIds) {
        addEdge(sourceId, step.id, "src");
      }
    }
    if (step.type === "operator" && step.sourceStepId) {
      addEdge(step.sourceStepId, step.id, "opsrc");
    }
  }

  return { nodes, edges };
}

/** Apply run data to structural nodes — updates status, duration, ForEach stats.
 *  Called on every poll tick but doesn't change graph structure.
 *  Engine and frontend use the same branch-keyed ID convention:
 *  ForEach: "stepId[i].templateStepId", If: "stepId[then0].id" / "stepId[else].id",
 *  Switch: "stepId[caseValue].id" / "stepId[default].id" */
function applyRunData(
  nodes: StepNode[],
  edges: Edge[],
  graph: PlanGraph,
  stepRuns: JobRun[],
  highlightStepId?: string,
): { nodes: StepNode[]; edges: Edge[] } {
  // Index runs by planStepId — keep the latest run per step (retries replace earlier attempts)
  const runsByStepId = new Map<string, JobRun>();
  for (const run of stepRuns) {
    if (!run.planStepId) continue;
    runsByStepId.set(run.planStepId, run);
  }

  // Compute ForEach iteration stats.
  // Iteration runs have PlanStepId = "{forEachId}[{index}].{templateStepId}",
  // so we match runs whose stepId starts with the ForEach step's ID prefix.
  const forEachStats = new Map<string, ForEachStats>();
  const forEachStepIds: string[] = [];
  for (const step of graph.steps) {
    if (step.type === "forEach" && step.template) {
      forEachStepIds.push(step.id);
    }
  }
  if (forEachStepIds.length > 0) {
    // Build prefix matchers: "forEachId[" matches iteration runs
    const prefixes = forEachStepIds.map((id) => ({ id, prefix: `${id}[` }));
    for (const { id, prefix } of prefixes) {
      // Deduplicate retries: keep only the latest run per mangled step ID.
      // stepRuns is ordered by createdAt ascending, so last write wins = latest attempt.
      const latestByStepId = new Map<string, JobRun>();
      for (const run of stepRuns) {
        if (run.planStepId?.startsWith(prefix)) {
          latestByStepId.set(run.planStepId, run);
        }
      }
      const byStatus: Record<number, number> = {};
      let total = 0;
      for (const run of latestByStepId.values()) {
        byStatus[run.status] = (byStatus[run.status] ?? 0) + 1;
        total++;
      }
      if (total > 0) forEachStats.set(id, { total, byStatus });
    }
  }

  const updatedNodes = nodes.map((node) => ({
    ...node,
    data: {
      ...node.data,
      run: runsByStepId.get(node.id),
      isHighlighted: highlightStepId === node.id,
      forEachStats: node.data.isTemplate
        ? undefined
        : forEachStats.get(node.id),
    },
  }));

  // Animate edges leading to running steps
  const updatedEdges = edges.map((edge) => {
    const targetRun = runsByStepId.get(edge.target);
    return targetRun?.status === 1
      ? { ...edge, animated: true }
      : edge.animated
        ? { ...edge, animated: false }
        : edge;
  });

  return { nodes: updatedNodes, edges: updatedEdges };
}

// ── Viewport cache (persists across re-mounts within the same session) ──

const viewportCache = new Map<string, Viewport>();

// ── Edge style ─────────────────────────────────────────────────────────

const defaultEdgeOptions = {
  style: { stroke: "var(--border)", strokeWidth: 1.5 },
  type: "smoothstep" as const,
};

// ── Viewport persistence ────────────────────────────────────────────────

function ViewportManager({
  layoutReady,
  planRunId,
}: {
  layoutReady: boolean;
  planRunId?: string;
}) {
  const { fitView, setViewport } = useReactFlow();
  const initializedRef = useRef(false);

  // Save viewport on every change
  useOnViewportChange({
    onChange: useCallback(
      (viewport: Viewport) => {
        if (planRunId && initializedRef.current) {
          viewportCache.set(planRunId, viewport);
        }
      },
      [planRunId],
    ),
  });

  // Restore or fit on first layout
  useEffect(() => {
    if (layoutReady && !initializedRef.current) {
      const timer = setTimeout(() => {
        const cached = planRunId ? viewportCache.get(planRunId) : undefined;
        if (cached) {
          setViewport(cached, { duration: 0 });
        } else {
          fitView({ padding: 0.15, duration: 200 });
        }
        initializedRef.current = true;
      }, 50);
      return () => clearTimeout(timer);
    }
  }, [layoutReady, fitView, setViewport, planRunId]);

  return null;
}

// ── Node types (stable reference — defined outside component) ──────────

const nodeTypes = { step: StepNodeComponent };

// ── Main component ─────────────────────────────────────────────────────

function PlanGraphInner({
  planGraph,
  stepRuns,
  highlightStepId,
  planRunId,
}: {
  planGraph: string;
  stepRuns: JobRun[];
  highlightStepId?: string;
  planRunId?: string;
}) {
  const navigate = useNavigate();
  const [positions, setPositions] = useState<Map<
    string,
    { x: number; y: number }
  > | null>(null);

  const graph = useMemo<PlanGraph | null>(() => {
    try {
      return JSON.parse(planGraph);
    } catch {
      return null;
    }
  }, [planGraph]);

  // Structure only changes when the plan graph JSON changes (not on poll ticks)
  const structure = useMemo(
    () => (graph ? buildStructure(graph) : null),
    [graph],
  );

  // Compute layout only when structure changes
  useEffect(() => {
    if (!structure || structure.nodes.length === 0) return;
    let cancelled = false;
    computeLayout(structure.nodes, structure.edges).then((pos) => {
      if (!cancelled) setPositions(pos);
    });
    return () => {
      cancelled = true;
    };
  }, [structure]);

  // Apply positions + run data on every update (cheap — no layout recomputation)
  const { nodes, edges } = useMemo(() => {
    if (!structure || !positions || !graph) return { nodes: [], edges: [] };

    const positioned = structure.nodes.map((node) => ({
      ...node,
      position: positions.get(node.id) ?? { x: 0, y: 0 },
    }));

    return applyRunData(
      positioned,
      structure.edges,
      graph,
      stepRuns,
      highlightStepId,
    );
  }, [structure, positions, graph, stepRuns, highlightStepId]);

  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: StepNode) => {
      const runId = node.data.run?.id;
      if (runId) navigate(`/runs/${runId}`);
    },
    [navigate],
  );

  const colorMode = useColorMode();

  if (!structure || nodes.length === 0) return null;

  return (
    <div
      className="overflow-hidden"
      style={{ height: Math.min(500, Math.max(200, nodes.length * 50 + 80)) }}
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        defaultEdgeOptions={defaultEdgeOptions}
        onNodeClick={onNodeClick}
        fitView
        minZoom={0.3}
        maxZoom={1.5}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        panOnDrag
        zoomOnScroll
        colorMode={colorMode}
        style={{ background: "transparent" }}
      >
        <ViewportManager layoutReady={positions !== null} planRunId={planRunId} />
      </ReactFlow>
    </div>
  );
}

function useColorMode(): ColorMode {
  const [mode, setMode] = useState<ColorMode>(() =>
    document.documentElement.classList.contains("dark") ? "dark" : "light",
  );

  useEffect(() => {
    const observer = new MutationObserver(() => {
      setMode(
        document.documentElement.classList.contains("dark") ? "dark" : "light",
      );
    });
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class"],
    });
    return () => observer.disconnect();
  }, []);

  return mode;
}

// ── Legend ──────────────────────────────────────────────────────────────

function PlanLegend({ stepRuns }: { stepRuns: JobRun[] }) {
  const statuses = useMemo(() => {
    const counts: Record<number, number> = {};
    for (const r of stepRuns) {
      counts[r.status] = (counts[r.status] ?? 0) + 1;
    }
    return Object.entries(counts)
      .map(([s, count]) => ({ status: Number(s), count }))
      .sort((a, b) => a.status - b.status);
  }, [stepRuns]);

  if (statuses.length === 0) return null;

  return (
    <div className="flex items-center gap-3 text-[11px] text-muted-foreground/70">
      {statuses.map(({ status, count }) => (
        <span key={status} className="flex items-center gap-1">
          <span
            className="size-2 rounded-full"
            style={{ backgroundColor: statusBorderColor[status] }}
          />
          {count} {JobStatusLabels[status]?.toLowerCase() ?? "unknown"}
        </span>
      ))}
    </div>
  );
}

// ── Exported wrapper ───────────────────────────────────────────────────

export function PlanGraphView({
  planGraph,
  stepRuns,
  highlightStepId,
  planRunId,
}: {
  planGraph: string;
  stepRuns: JobRun[];
  highlightStepId?: string;
  planRunId?: string;
}) {
  const stepCount = useMemo(() => {
    try {
      return (JSON.parse(planGraph) as PlanGraph).steps.length;
    } catch {
      return 0;
    }
  }, [planGraph]);

  return (
    <div className="overflow-hidden rounded-md border">
      <div className="flex items-center justify-between h-10 px-2 border-b bg-muted/30 backdrop-blur-sm">
        <span className="text-sm text-muted-foreground">
          Plan graph (
          {stepRuns.length > 0
            ? `${stepRuns.length} run${stepRuns.length !== 1 ? "s" : ""}`
            : `${stepCount} step${stepCount !== 1 ? "s" : ""}`}
          )
        </span>
        <PlanLegend stepRuns={stepRuns} />
      </div>
      <ReactFlowProvider>
        <PlanGraphInner
          planGraph={planGraph}
          stepRuns={stepRuns}
          highlightStepId={highlightStepId}
          planRunId={planRunId}
        />
      </ReactFlowProvider>
    </div>
  );
}
