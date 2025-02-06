import React, { useContext } from "react";
import Box from "@mui/material/Box";
import { MetricsModalWrapper } from "../../../../../../../../../../../../common/MetricsModalWrapper";
import { getPodContainerUsePercentages } from "../../../../../../../../../../../../../utils";
import { PodInfoProps } from "../../../../../../../../../../../../../types/declarations/pods";
import { AppContextProps } from "../../../../../../../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../../../../../../../App";

import "./style.css";

export function ContainerInfo({
  namespaceId,
  pipelineId,
  vertexId,
  type,
  pod,
  podDetails,
  containerName,
  containerInfo,
  podSpecificInfo,
}: PodInfoProps) {
  const { disableMetricsCharts } = useContext<AppContextProps>(AppContext);

  const resourceUsage = getPodContainerUsePercentages(
    pod,
    podDetails,
    containerName
  );

  // CPU
  let usedCPU: string | undefined =
    podDetails?.containerMap instanceof Map
      ? podDetails?.containerMap?.get(containerName)?.cpu
      : undefined;
  let specCPU: string | undefined =
    pod?.containerSpecMap instanceof Map
      ? pod?.containerSpecMap?.get(containerName)?.cpu
      : undefined;
  if (!usedCPU) {
    usedCPU = "?";
  } else if (usedCPU.endsWith("n")) {
    usedCPU = `${(parseFloat(usedCPU) / 1e6).toFixed(2)}m`;
  }
  if (!specCPU) {
    specCPU = "?";
  }
  let cpuPercent = "unavailable";
  if (resourceUsage?.cpuPercent) {
    cpuPercent = `${resourceUsage.cpuPercent?.toFixed(2)}%`;
  }
  // Memory
  let usedMem: string | undefined =
    podDetails?.containerMap instanceof Map
      ? podDetails?.containerMap?.get(containerName)?.memory
      : undefined;
  let specMem: string | undefined =
    pod?.containerSpecMap instanceof Map
      ? pod?.containerSpecMap?.get(containerName)?.memory
      : undefined;
  if (!usedMem) {
    usedMem = "?";
  } else if (usedMem.endsWith("Ki")) {
    usedMem = `${(parseFloat(usedMem) / 1024).toFixed(2)}Mi`;
  }
  if (!specMem) {
    specMem = "?";
  }
  let memPercent = "unavailable";
  if (resourceUsage?.memoryPercent) {
    memPercent = `${resourceUsage.memoryPercent.toFixed(2)}%`;
  }

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
              {containerInfo?.lastStartedAt || "N/A"}
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
                metricName={"namespace_app_container_cpu_utilization"}
                value={`${usedCPU} / ${specCPU} (${cpuPercent})`}
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
                metricName={"namespace_app_container_memory_utilization"}
                value={`${usedMem} / ${specMem} (${memPercent})`}
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
                  metricName={"namespace_pod_cpu_utilization"}
                  value={podSpecificInfo?.totalCPU}
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
                  metricName={"namespace_pod_memory_utilization"}
                  value={podSpecificInfo?.totalMemory}
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
