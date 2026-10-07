import LayersOutlinedIcon from "@mui/icons-material/LayersOutlined";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import { Containers } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/Containers";
import { PodDetail } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodDetails";
import { PodFocusControls } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodFocusControls";
import { PodsProps } from "../../../../../../../types/declarations/pods";
import { usePodViewState } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/usePodViewState";
import { PodFleet } from "./PodFleet";
import { PodInspector } from "./PodInspector";

import "./style.css";

/**
 * PodViewNext owns the Beta Pod View presentation while preserving the shared
 * v1 selection, metrics, and virtualized-log behavior from Classic Pod View.
 */
export function PodViewNext(props: PodsProps) {
  const {
    pods,
    podsDetails,
    podsErr,
    loading,
    podRuntimeByName,
    podRuntimeError,
    selectedPod,
    selectedContainer,
    selectedRuntime,
    selectedPodDetails,
    selectPodFromFleet,
    selectPodFromSearch,
    selectContainer,
  } = usePodViewState(props);

  if (loading) {
    return (
      <div className="pod-view-next" data-testid="pod-view-beta">
        <Box className="pod-view-next-state" data-testid="pods-loading">
          Loading pods view... <CircularProgress size={16} />
        </Box>
      </div>
    );
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

  if (!pods?.length) {
    return (
      <div className="pod-view-next" data-testid="pod-view-beta">
        <Box className="pod-view-next-state" data-testid="pods-empty">
          No pods found for this vertex
        </Box>
      </div>
    );
  }

  const activeContainer =
    selectedContainer || selectedPod?.containers?.[0] || "";
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
        <PodFleet
          pods={pods}
          detailsByName={podsDetails}
          runtimeByName={podRuntimeByName}
          selectedPod={selectedPod}
          onPodSelect={selectPodFromFleet}
        />
        {podRuntimeError && (
          <p className="pod-view-next-runtime-warning" role="status">
            Pod runtime details are temporarily unavailable. Logs and resource usage remain available.
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
              podDetails={selectedPodDetails}
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
