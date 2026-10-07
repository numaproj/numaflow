import { ReactNode, useContext, useMemo } from "react";
import { AppContext } from "../../../../../../../App";
import { MetricsModalWrapper } from "../../../../../MetricsModalWrapper";
import {
  getPodContainerUsePercentages,
} from "../../../../../../../utils";
import {
  ContainerInfoProps,
  Pod,
  PodDetail,
  PodRuntimeInfo,
} from "../../../../../../../types/declarations/pods";
import { AppContextProps } from "../../../../../../../types/declarations/app";
import {
  CONTAINER_CPU_UTILIZATION,
  CONTAINER_MEMORY_UTILIZATION,
  POD_CPU_UTILIZATION,
  POD_MEMORY_UTILIZATION,
} from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodDetails/partials/Metrics/utils/constants";

import { podStatusTone } from "./podFleetModel";

import "./podInspector.css";

interface PodInspectorProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  type: string;
  pod: Pod;
  podDetails?: PodDetail;
  containerName?: string;
  runtime?: PodRuntimeInfo;
}

const displayQuantity = (value?: string, suffix?: string) => value || `—${suffix || ""}`;

function InspectorRow({
  label,
  children,
  danger = false,
}: {
  label: string;
  children: ReactNode;
  danger?: boolean;
}) {
  return (
    <div className="pod-inspector-row">
      <span>{label}</span>
      <strong className={danger ? "pod-inspector-danger" : undefined}>{children}</strong>
    </div>
  );
}

/** PodInspector presents selected pod and container runtime data without owning fetch or selection state. */
export function PodInspector({
  namespaceId,
  pipelineId,
  vertexId,
  type,
  pod,
  podDetails,
  containerName,
  runtime,
}: PodInspectorProps) {
  const { disableMetricsCharts } = useContext<AppContextProps>(AppContext);
  const containerInfo: ContainerInfoProps | undefined = containerName
    ? runtime?.containerDetailsMap?.[containerName]
    : undefined;
  const usage = useMemo(
    () =>
      containerName && podDetails
        ? getPodContainerUsePercentages(pod, podDetails, containerName)
        : {},
    [containerName, pod, podDetails]
  );
  const containerUsage = containerName
    ? podDetails?.containerMap.get(containerName)
    : undefined;
  const containerSpec = containerName
    ? pod.containerSpecMap.get(containerName)
    : undefined;
  const restartCount = Object.values(runtime?.containerDetailsMap || {}).reduce(
    (count, item) => count + (item?.restartCount || 0),
    0
  );

  return (
    <aside className="pod-inspector" data-testid="pod-inspector">
      <section className="pod-inspector-card">
        <div className="pod-inspector-heading">
          <span>Pod Overview</span>
          <span
            className={`pod-inspector-status pod-inspector-status--${podStatusTone(
              runtime?.status
            )}`}
          >
            {runtime?.status || "Unknown"}
          </span>
        </div>
        <p className="pod-inspector-name" title={pod.name}>{pod.name}</p>
        <dl>
          <InspectorRow label="Container">{containerName || "—"}</InspectorRow>
          <InspectorRow label="State">{containerInfo?.state || "Unknown"}</InspectorRow>
          <InspectorRow label="Restarts" danger={restartCount > 0}>{restartCount}</InspectorRow>
          {runtime?.reason && (
            <InspectorRow label="Reason" danger>{runtime.reason}</InspectorRow>
          )}
        </dl>
      </section>

      {containerInfo?.lastTerminationReason && (
        <section className="pod-inspector-card pod-inspector-card--termination">
          <div className="pod-inspector-heading">Last Termination</div>
          <p>{containerInfo.lastTerminationReason}</p>
          {containerInfo.lastTerminationMessage && (
            <p className="pod-inspector-muted">{containerInfo.lastTerminationMessage}</p>
          )}
          {containerInfo.lastTerminationExitCode !== undefined &&
            containerInfo.lastTerminationExitCode !== null && (
              <InspectorRow label="Exit code" danger>
                {containerInfo.lastTerminationExitCode}
              </InspectorRow>
            )}
        </section>
      )}

      <section className="pod-inspector-card">
        <div className="pod-inspector-heading">Resources</div>
        <dl>
          <InspectorRow label="CPU">
            <MetricsModalWrapper
              disableMetricsCharts={disableMetricsCharts}
              namespaceId={namespaceId}
              pipelineId={pipelineId}
              vertexId={vertexId}
              type={type}
              metricDisplayName={CONTAINER_CPU_UTILIZATION}
              value={`${displayQuantity(containerUsage?.cpu)} / ${displayQuantity(containerSpec?.cpu)}`}
              pod={pod}
            />
          </InspectorRow>
          <InspectorRow label="Memory">
            <MetricsModalWrapper
              disableMetricsCharts={disableMetricsCharts}
              namespaceId={namespaceId}
              pipelineId={pipelineId}
              vertexId={vertexId}
              type={type}
              metricDisplayName={CONTAINER_MEMORY_UTILIZATION}
              value={`${displayQuantity(containerUsage?.memory)} / ${displayQuantity(containerSpec?.memory)}`}
              pod={pod}
            />
          </InspectorRow>
          <InspectorRow label="CPU usage">
            {usage.cpuPercent === undefined ? "—" : `${usage.cpuPercent.toFixed(1)}%`}
          </InspectorRow>
          <InspectorRow label="Memory usage">
            {usage.memoryPercent === undefined ? "—" : `${usage.memoryPercent.toFixed(1)}%`}
          </InspectorRow>
        </dl>
      </section>

      <section className="pod-inspector-card">
        <div className="pod-inspector-heading">Quick Metrics</div>
        <dl>
          <InspectorRow label="Pod CPU">
            <MetricsModalWrapper
              disableMetricsCharts={disableMetricsCharts}
              namespaceId={namespaceId}
              pipelineId={pipelineId}
              vertexId={vertexId}
              type={type}
              metricDisplayName={POD_CPU_UTILIZATION}
              value={runtime?.totalCPU || "—"}
              pod={pod}
            />
          </InspectorRow>
          <InspectorRow label="Pod memory">
            <MetricsModalWrapper
              disableMetricsCharts={disableMetricsCharts}
              namespaceId={namespaceId}
              pipelineId={pipelineId}
              vertexId={vertexId}
              type={type}
              metricDisplayName={POD_MEMORY_UTILIZATION}
              value={runtime?.totalMemory || "—"}
              pod={pod}
            />
          </InspectorRow>
        </dl>
      </section>
    </aside>
  );
}
