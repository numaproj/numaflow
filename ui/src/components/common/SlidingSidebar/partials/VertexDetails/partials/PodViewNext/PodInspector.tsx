import { ReactNode, useContext } from "react";
import { AppContext } from "../../../../../../../App";
import { MetricsModalWrapper } from "../../../../../MetricsModalWrapper";
import { ago, resourceRequestPercent } from "../../../../../../../utils";
import {
  ContainerInfoProps,
  Pod,
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
  containerName?: string;
  runtime?: PodRuntimeInfo;
}

const EMPTY = "—";

/** formatUsage renders `usage / request (percent)`, omitting the percent when either side is unknown. */
const formatUsage = (used?: string, requested?: string, percent?: number) => {
  const base = `${used || EMPTY} / ${requested || EMPTY}`;
  return percent === undefined ? base : `${base} (${percent.toFixed(2)}%)`;
};

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
      <strong
        className={danger ? "pod-inspector-danger" : undefined}
        title={typeof children === "string" ? children : undefined}
      >
        {children}
      </strong>
    </div>
  );
}

function StatusPill({ status }: { status?: string }) {
  return (
    <span
      className={`pod-inspector-status pod-inspector-status--${podStatusTone(status)}`}
    >
      {status || "Unknown"}
    </span>
  );
}

/**
 * PodInspector scopes runtime details to two cards: the selected pod as a whole
 * and the selected container chip. It does not own fetch or selection state.
 */
export function PodInspector({
  namespaceId,
  pipelineId,
  vertexId,
  type,
  pod,
  containerName,
  runtime,
}: PodInspectorProps) {
  const { disableMetricsCharts } = useContext<AppContextProps>(AppContext);
  const metricsProps = {
    disableMetricsCharts,
    namespaceId,
    pipelineId,
    vertexId,
    type,
    pod,
  };
  const containerInfo: ContainerInfoProps | undefined = containerName
    ? runtime?.containerDetailsMap?.[containerName]
    : undefined;
  const containerUsage = containerName
    ? runtime?.containerDetailsMap?.[containerName]
    : undefined;
  const cpuPercent = resourceRequestPercent(
    containerUsage?.totalCPU,
    containerUsage?.requestedCPU
  );
  const memoryPercent = resourceRequestPercent(
    containerUsage?.totalMemory,
    containerUsage?.requestedMemory
  );
  const podRestarts = Object.values(runtime?.containerDetailsMap || {}).reduce(
    (count, item) => count + (item?.restartCount || 0),
    0
  );
  const containerRestarts = containerInfo?.restartCount ?? 0;
  const exitCode = containerInfo?.lastTerminationExitCode;

  return (
    <aside className="pod-inspector" data-testid="pod-inspector">
      <section className="pod-inspector-card" data-testid="pod-overview">
        <div className="pod-inspector-heading">
          <span>Pod Overview</span>
          <StatusPill status={runtime?.status} />
        </div>
        <dl>
          <InspectorRow label="Name">{pod.name}</InspectorRow>
          <InspectorRow label="Restarts" danger={podRestarts > 0}>
            {podRestarts}
          </InspectorRow>
          <InspectorRow label="CPU">
            {runtime?.totalCPU ? (
              <MetricsModalWrapper
                {...metricsProps}
                metricDisplayName={POD_CPU_UTILIZATION}
                value={runtime.totalCPU}
              />
            ) : (
              EMPTY
            )}
          </InspectorRow>
          <InspectorRow label="Memory">
            {runtime?.totalMemory ? (
              <MetricsModalWrapper
                {...metricsProps}
                metricDisplayName={POD_MEMORY_UTILIZATION}
                value={runtime.totalMemory}
              />
            ) : (
              EMPTY
            )}
          </InspectorRow>
          {runtime?.reason && (
            <InspectorRow label="Reason" danger>{runtime.reason}</InspectorRow>
          )}
          {runtime?.message && (
            <InspectorRow label="Message" danger>{runtime.message}</InspectorRow>
          )}
        </dl>
      </section>

      <section className="pod-inspector-card" data-testid="container-overview">
        <div className="pod-inspector-heading">
          <span>Container Overview</span>
          <StatusPill status={containerInfo?.state} />
        </div>
        <dl>
          <InspectorRow label="Name">{containerName || EMPTY}</InspectorRow>
          <InspectorRow label="Last Started">
            {containerInfo?.lastStartedAt
              ? ago(new Date(containerInfo.lastStartedAt), 2)
              : EMPTY}
          </InspectorRow>
          <InspectorRow label="CPU">
            <MetricsModalWrapper
              {...metricsProps}
              metricDisplayName={CONTAINER_CPU_UTILIZATION}
              value={formatUsage(
                containerUsage?.totalCPU,
                containerUsage?.requestedCPU,
                cpuPercent
              )}
            />
          </InspectorRow>
          <InspectorRow label="Memory">
            <MetricsModalWrapper
              {...metricsProps}
              metricDisplayName={CONTAINER_MEMORY_UTILIZATION}
              value={formatUsage(
                containerUsage?.totalMemory,
                containerUsage?.requestedMemory,
                memoryPercent
              )}
            />
          </InspectorRow>
          <InspectorRow label="Restarts" danger={containerRestarts > 0}>
            {containerRestarts}
          </InspectorRow>
          {containerInfo?.waitingReason && (
            <InspectorRow label="Waiting Reason" danger>
              {containerInfo.waitingReason}
            </InspectorRow>
          )}
          {containerInfo?.waitingMessage && (
            <InspectorRow label="Waiting Message" danger>
              {containerInfo.waitingMessage}
            </InspectorRow>
          )}
          {containerInfo?.lastTerminationReason && (
            <InspectorRow label="Last Termination" danger>
              {containerInfo.lastTerminationReason}
            </InspectorRow>
          )}
          {containerInfo?.lastTerminationMessage && (
            <InspectorRow label="Termination Message" danger>
              {containerInfo.lastTerminationMessage}
            </InspectorRow>
          )}
          {exitCode !== undefined && exitCode !== null && (
            <InspectorRow label="Exit Code" danger>{exitCode}</InspectorRow>
          )}
        </dl>
      </section>
    </aside>
  );
}
