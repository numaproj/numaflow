import React, { useContext } from "react";
import Box from "@mui/material/Box";
import { MetricsModalWrapper } from "../../../../../../../../../../../../common/MetricsModalWrapper";
import {
  ago,
  resourceRequestPercent,
} from "../../../../../../../../../../../../../utils";
import { PodInfoProps } from "../../../../../../../../../../../../../types/declarations/pods";
import {
  CONTAINER_CPU_UTILIZATION,
  CONTAINER_MEMORY_UTILIZATION,
  POD_CPU_UTILIZATION,
  POD_MEMORY_UTILIZATION,
} from "../Metrics/utils/constants";
import { AppContextProps } from "../../../../../../../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../../../../../../../App";

import "./style.css";

export function ContainerInfo({
  namespaceId,
  pipelineId,
  vertexId,
  type,
  pod,
  containerName,
  containerInfo,
  podSpecificInfo,
}: PodInfoProps) {
  const { disableMetricsCharts } = useContext<AppContextProps>(AppContext);

  const cpuPercentValue = resourceRequestPercent(
    containerInfo?.totalCPU,
    containerInfo?.requestedCPU
  );
  const memoryPercentValue = resourceRequestPercent(
    containerInfo?.totalMemory,
    containerInfo?.requestedMemory
  );
  const usedCPU = containerInfo?.totalCPU || "?";
  const specCPU = containerInfo?.requestedCPU || "?";
  const cpuPercent =
    cpuPercentValue === undefined ? "unavailable" : `${cpuPercentValue.toFixed(2)}%`;
  const usedMem = containerInfo?.totalMemory || "?";
  const specMem = containerInfo?.requestedMemory || "?";
  const memPercent =
    memoryPercentValue === undefined
      ? "unavailable"
      : `${memoryPercentValue.toFixed(2)}%`;

  return (
    <Box
      sx={{
        display: "flex",
        height: "100%",
        width: "100%",
      }}
    >
      <Box
        data-testid="containerInfo"
        sx={{
          display: "flex",
          height: "100%",
          width: "100%",
        }}
      >
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            height: "100%",
            width: "100%",
            justifyContent: "space-evenly",
          }}
        >
          {/*container info*/}
          <Box className={"category-title"}>Container Info</Box>

          <Box className={"outer-box"}>
            <Box className={"inner-box-title"}>Name</Box>
            <Box className={"inner-box-value"}>{containerName}</Box>
          </Box>

          <Box className={"outer-box"}>
            <Box className={"inner-box-title"}>Status</Box>
            <Box className={"inner-box-value"}>
              {containerInfo?.state || "Unknown"}
            </Box>
          </Box>

          <Box className={"outer-box"}>
            <Box className={"inner-box-title"}>Last Started At</Box>
            <Box className={"inner-box-value"}>
              {containerInfo?.lastStartedAt
                ? ago(new Date(containerInfo.lastStartedAt), 2)
                : "N/A"}
            </Box>
          </Box>

          <Box className={"outer-box"}>
            <Box className={"inner-box-title"}>CPU</Box>
            <Box className={"inner-box-value"}>
              <MetricsModalWrapper
                disableMetricsCharts={disableMetricsCharts}
                namespaceId={namespaceId}
                pipelineId={pipelineId}
                vertexId={vertexId}
                type={type}
                metricDisplayName={CONTAINER_CPU_UTILIZATION}
                value={`${usedCPU} / ${specCPU} (${cpuPercent})`}
                pod={pod}
              />
            </Box>
          </Box>

          <Box className={"outer-box"}>
            <Box className={"inner-box-title"}>Memory</Box>
            <Box className={"inner-box-value"}>
              <MetricsModalWrapper
                disableMetricsCharts={disableMetricsCharts}
                namespaceId={namespaceId}
                pipelineId={pipelineId}
                vertexId={vertexId}
                type={type}
                metricDisplayName={CONTAINER_MEMORY_UTILIZATION}
                value={`${usedMem} / ${specMem} (${memPercent})`}
                pod={pod}
              />
            </Box>
          </Box>

          <Box className={"outer-box"}>
            <Box className={"inner-box-title"}>Restart Count</Box>
            <Box className={"inner-box-value"}>
              {containerInfo?.restartCount ?? "Unknown"}
            </Box>
          </Box>

          {containerInfo?.lastTerminationReason && (
            <Box className={"outer-box"}>
              <Box className={"inner-box-title"}>Last Termination Reason</Box>
              <Box className={"inner-box-value"}>
                {containerInfo?.lastTerminationReason}
              </Box>
            </Box>
          )}

          {containerInfo?.lastTerminationMessage && (
            <Box className={"outer-box"}>
              <Box className={"inner-box-title"}>Last Termination Message</Box>
              <Box className={"inner-box-value"}>
                {containerInfo?.lastTerminationMessage}
              </Box>
            </Box>
          )}

          {containerInfo?.lastTerminationExitCode !== null &&
            containerInfo?.lastTerminationExitCode != undefined && (
              <Box className={"outer-box"}>
                <Box className={"inner-box-title"}>Exit Code</Box>
                <Box className={"inner-box-value"}>
                  {containerInfo?.lastTerminationExitCode}
                </Box>
              </Box>
            )}

          {containerInfo?.waitingReason && (
            <Box className={"outer-box"}>
              <Box className={"inner-box-title"}>Waiting Reason</Box>
              <Box className={"inner-box-value"}>
                {containerInfo?.waitingReason}
              </Box>
            </Box>
          )}

          {containerInfo?.waitingMessage && (
            <Box className={"outer-box"}>
              <Box className={"inner-box-title"}>Waiting Message</Box>
              <Box className={"inner-box-value"}>
                {containerInfo?.waitingMessage}
              </Box>
            </Box>
          )}

          {/*pod info*/}
          <Box className={"category-title"} sx={{ mt: "1.5rem" }}>
            Pod Info
          </Box>

          <Box className={"outer-box"}>
            <Box className={"inner-box-title"}>Name</Box>
            <Box className={"inner-box-value"}>
              {pod?.name?.slice(0, pod.name?.lastIndexOf("-"))}
            </Box>
          </Box>

          <Box className={"outer-box"}>
            <Box className={"inner-box-title"}>Status</Box>
            <Box className={"inner-box-value"}>
              {podSpecificInfo?.status || "Unknown"}
            </Box>
          </Box>

          <Box className={"outer-box"}>
            <Box className={"inner-box-title"}>Restart Count</Box>
            <Box className={"inner-box-value"}>
              {podSpecificInfo?.restartCount ?? "Unknown"}
            </Box>
          </Box>

          {podSpecificInfo?.totalCPU && (
            <Box className={"outer-box"}>
              <Box className={"inner-box-title"}>CPU</Box>
              <Box className={"inner-box-value"}>
                <MetricsModalWrapper
                  disableMetricsCharts={disableMetricsCharts}
                  namespaceId={namespaceId}
                  pipelineId={pipelineId}
                  vertexId={vertexId}
                  type={type}
                  metricDisplayName={POD_CPU_UTILIZATION}
                  value={podSpecificInfo?.totalCPU}
                  pod={pod}
                />
              </Box>
            </Box>
          )}

          {podSpecificInfo?.totalMemory && (
            <Box className={"outer-box"}>
              <Box className={"inner-box-title"}>Memory</Box>
              <Box className={"inner-box-value"}>
                <MetricsModalWrapper
                  disableMetricsCharts={disableMetricsCharts}
                  namespaceId={namespaceId}
                  pipelineId={pipelineId}
                  vertexId={vertexId}
                  type={type}
                  metricDisplayName={POD_MEMORY_UTILIZATION}
                  value={podSpecificInfo?.totalMemory}
                  pod={pod}
                />
              </Box>
            </Box>
          )}

          {podSpecificInfo?.reason && (
            <Box className={"outer-box"}>
              <Box className={"inner-box-title"}>Reason</Box>
              <Box className={"inner-box-value"}>{podSpecificInfo?.reason}</Box>
            </Box>
          )}

          {podSpecificInfo?.message && (
            <Box className={"outer-box"}>
              <Box className={"inner-box-title"}>Message</Box>
              <Box className={"inner-box-value"}>
                {podSpecificInfo?.message}
              </Box>
            </Box>
          )}
        </Box>
      </Box>
    </Box>
  );
}
