import { useCallback, useContext, useEffect, useMemo, useState } from "react";
import LayersOutlinedIcon from "@mui/icons-material/LayersOutlined";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import { Containers } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/Containers";
import { PodDetail } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodDetails";
import { PodFocusControls } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodFocusControls";
import {
  InspectTarget,
  PodFleetItem,
  PodsProps,
} from "../../../../../../../types/declarations/pods";
import { BufferInfo } from "../../../../../../../types/declarations/pipeline";
import {
  getDefaultContainerName,
  PodViewStateOptions,
  usePodViewState,
} from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/usePodViewState";
import { VertexDetailsContext } from "../../index";
import {
  appendDaemonSample,
  buildVertexHealth,
  DaemonSample,
  isUnhealthyPod,
  readDaemonRates,
} from "./goldenSignalsModel";
import { PodFleet } from "./PodFleet";
import { buildPodFleetItems, PodFleetFilter } from "./podFleetModel";
import { PodInspector } from "./PodInspector";
import { useVertexHealthData } from "./useVertexHealthData";
import { VertexHealthStrip } from "./VertexHealthStrip";

import "./style.css";

const BETA_SELECTION: PodViewStateOptions = {
  autoSelectFirstPod: false,
  defaultContainer: "numa",
};

export interface PodViewNextProps extends PodsProps {
  /** Daemon processing rates and pendings: one object for MonoVertex, a map by vertex for pipelines. */
  vertexMetrics?: any;
  buffers?: BufferInfo[];
  recentErrorCount?: number;
  vertexSpec?: any;
}

/** failingContainer prefers a container that is not Running, then the one with the most restarts. */
export function failingContainer(item: PodFleetItem): string | undefined {
  const containers = Object.entries(item.runtime?.containerDetailsMap || {});
  const notRunning = containers.find(([, info]) => info?.state && info.state !== "Running");
  if (notRunning) return notRunning[0];
  const restarted = containers
    .filter(([, info]) => (info?.restartCount || 0) > 0)
    .sort(([, left], [, right]) => (right?.restartCount || 0) - (left?.restartCount || 0));
  return restarted[0]?.[0];
}

/**
 * PodViewNext owns the Beta Pod View presentation while preserving the shared
 * v1 selection, metrics, and virtualized-log behavior from Classic Pod View.
 */
export function PodViewNext(props: PodViewNextProps) {
  const active = props.active !== false;
  const {
    pods,
    podsErr,
    loading,
    podRuntimeByName,
    podRuntimeError,
    selectedPod,
    selectedContainer,
    selectedRuntime,
    selectPodFromFleet,
    selectPodFromSearch,
    selectPodWithContainer,
    selectContainer,
  } = usePodViewState(props, { ...BETA_SELECTION, active });
  const { openMetrics, openVertexTab } = useContext(VertexDetailsContext);
  const [filter, setFilter] = useState<PodFleetFilter>("all");
  const { namespaceId, pipelineId, vertexId, type, vertexMetrics, buffers, recentErrorCount, vertexSpec } =
    props;
  const { queries, series, health, scaleStatus, watermarkEdges, metricsEnabled, now } =
    useVertexHealthData({ namespaceId, pipelineId, vertexId, type, active });

  const fleetItems = useMemo(
    () => buildPodFleetItems(pods, podRuntimeByName),
    [pods, podRuntimeByName]
  );
  const daemon = useMemo(
    () => readDaemonRates(vertexMetrics, type, vertexId),
    [vertexMetrics, type, vertexId]
  );
  const vertexKey = `${namespaceId}:${pipelineId}:${vertexId}:${type}`;
  const [daemonHistory, setDaemonHistory] = useState<DaemonSample[]>([]);
  useEffect(() => {
    if (!active) return;
    setDaemonHistory((history) => {
      const sameVertex = history.length === 0 || history[0]?.vertexKey === vertexKey;
      const current = sameVertex ? history : [];
      const next = appendDaemonSample(
        current,
        { at: now, vertexKey, rate1m: daemon.rate1m, pending: daemon.pending },
        now
      );
      return next === current ? history : next;
    });
  }, [active, daemon.pending, daemon.rate1m, now, vertexKey]);
  const vertexHealth = useMemo(
    () =>
      buildVertexHealth({
        type,
        vertexId,
        metricsEnabled,
        now,
        daemon,
        daemonHistory,
        series,
        queries,
        buffers,
        watermarkEdges,
        health,
        scaleStatus,
        scale: vertexSpec?.scale,
        fleetItems,
        recentErrorCount,
      }),
    [
      buffers,
      daemon,
      daemonHistory,
      fleetItems,
      health,
      metricsEnabled,
      scaleStatus,
      now,
      queries,
      recentErrorCount,
      series,
      type,
      vertexId,
      vertexSpec,
      watermarkEdges,
    ]
  );

  const handleInspect = useCallback(
    (target: InspectTarget) => {
      if (target.kind === "metrics") {
        openMetrics({ panelId: `${target.metric}-panel`, presets: target.presets });
        return;
      }
      if (target.kind === "tab") {
        openVertexTab(target.tab);
        return;
      }
      const affected = fleetItems.find(isUnhealthyPod);
      if (!affected) return;
      setFilter("critical");
      const container = failingContainer(affected);
      if (container) {
        selectPodWithContainer(affected.pod, container);
      } else {
        selectPodFromFleet(affected.pod);
      }
    },
    [fleetItems, openMetrics, openVertexTab, selectPodFromFleet, selectPodWithContainer]
  );

  if (!active) {
    return null;
  }

  if (podsErr) {
    return (
      <div className="pod-view-next" data-testid="pod-view-beta">
        <Box className="pod-view-next-state" data-testid="pods-error">
          Failed to get pods details
        </Box>
      </div>
    );
  }

  // The pod list is the fleet. Metrics can fail or arrive later without
  // hiding pods that are already known, and an unset list is not an empty one.
  if (pods === undefined || (loading && pods.length === 0)) {
    return (
      <div className="pod-view-next" data-testid="pod-view-beta">
        <Box className="pod-view-next-state" data-testid="pods-loading">
          Loading pods view... <CircularProgress size={16} />
        </Box>
      </div>
    );
  }

  if (!pods.length) {
    return (
      <div className="pod-view-next" data-testid="pod-view-beta">
        <Box className="pod-view-next-state" data-testid="pods-empty">
          No pods found for this vertex
        </Box>
      </div>
    );
  }

  const activeContainer =
    selectedContainer ||
    getDefaultContainerName(selectedPod, BETA_SELECTION.defaultContainer) ||
    "";
  const focusControls = selectedPod ? (
    <PodFocusControls
      pods={pods}
      selectedPod={selectedPod}
      selectedContainer={activeContainer}
      onPodSelect={selectPodFromSearch}
      onContainerSelect={selectContainer}
    />
  ) : undefined;

  return (
    <div className="pod-view-next" data-testid="pod-view-beta">
      <div className="pod-view-next-scroll">
        <VertexHealthStrip health={vertexHealth} onInspect={handleInspect} />
        <PodFleet
          items={fleetItems}
          filter={filter}
          onFilterChange={setFilter}
          runtimeByName={podRuntimeByName}
          selectedPod={selectedPod}
          onPodSelect={selectPodFromFleet}
          vertexId={vertexId}
          isMono={type === "monoVertex"}
        />
        {podRuntimeError && (
          <p className="pod-view-next-runtime-warning" role="status">
            Pod runtime details are temporarily unavailable. Logs remain available.
          </p>
        )}
        {selectedPod ? (
          <div className="pod-view-next-workspace">
            <section className="pod-view-next-logs">
              <div className="pod-view-next-containers">
                <span>Container</span>
                <Containers
                  pod={selectedPod}
                  containerName={activeContainer}
                  handleContainerClick={selectContainer}
                />
              </div>
              <div className="pod-view-next-log-card">
                <PodDetail
                  namespaceId={props.namespaceId}
                  pipelineId={props.pipelineId}
                  type={props.type}
                  containerName={activeContainer}
                  pod={selectedPod}
                  vertexId={props.vertexId}
                  focusControls={focusControls}
                />
              </div>
            </section>
            <PodInspector
              namespaceId={props.namespaceId}
              pipelineId={props.pipelineId}
              vertexId={props.vertexId}
              type={props.type}
              pod={selectedPod}
              containerName={activeContainer}
              runtime={selectedRuntime}
            />
          </div>
        ) : (
          <section className="pod-view-next-empty" data-testid="pod-view-unselected">
            <div className="pod-view-next-empty-icon" aria-hidden="true">
              <LayersOutlinedIcon />
            </div>
            <p className="pod-view-next-empty-title">Select a pod</p>
            <p className="pod-view-next-empty-subtitle">
              Click a chip above to inspect logs and metrics
            </p>
          </section>
        )}
      </div>
    </div>
  );
}
