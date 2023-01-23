import Box from "@mui/material/Box";
import { Pod, PodDetail } from "../../../utils/models/pods";
import { getPodContainerUsePercentages } from "../../../utils";

import "./PodInfo.css";

export interface PodInfoProps {
  pod: Pod;
  podDetail: PodDetail;
  containerName: string;
}

const podInfoSx = {
  display: "flex",
  flexDirection: "row",
};

const podDataRowSx = {
  display: "flex",
  flexDirection: "row",
  paddingBottom: "10px",
};

const podDataColumnSx = {
  display: "flex",
  flexDirection: "column",
  paddingBottom: "10px",
  width: "30%",
};

const podDataRowTagSx = {
  width: "25%",
};

export function PodInfo({ pod, podDetail, containerName }: PodInfoProps) {
  const resourceUsage = getPodContainerUsePercentages(
    pod,
    podDetail,
    containerName
  );

  // CPU
  let usedCPU: string | undefined =
    podDetail.containerMap.get(containerName)?.cpu;
  let specCPU: string | undefined =
    pod.containerSpecMap.get(containerName)?.cpu;
  if (!usedCPU) {
    usedCPU = "?";
  }
  if (!specCPU) {
    specCPU = "?";
  }
  let cpuPercent = "unavailable";
  if (resourceUsage.cpuPercent) {
    cpuPercent = `${resourceUsage.cpuPercent.toFixed(2)}%`;
  }
  // Memory
  let usedMem: string | undefined =
    podDetail.containerMap.get(containerName)?.memory;
  let specMem: string | undefined =
    pod.containerSpecMap.get(containerName)?.memory;
  if (!usedMem) {
    usedMem = "?";
  }
  if (!specMem) {
    specMem = "?";
  }
  let memPercent = "unavailable";
  if (resourceUsage.memoryPercent) {
    memPercent = `${resourceUsage.memoryPercent.toFixed(2)}%`;
  }
  const podName = podDetail.name.slice(0, podDetail.name.lastIndexOf('-'));

  return (
    <Box data-testid="podInfo" sx={podInfoSx}>
      <Box sx={podDataColumnSx}>
        <Box sx={podDataRowSx}>
          <Box sx={podDataRowTagSx}>Name:</Box>
          <Box>{podName}</Box>
        </Box>
      </Box>
      <Box sx={podDataColumnSx}>
        <Box sx={podDataRowSx}>
          <Box sx={podDataRowTagSx}>CPU %:</Box>
          <Box>{cpuPercent}</Box>
        </Box>
        <Box sx={podDataRowSx}>
          <Box sx={podDataRowTagSx}>CPU:</Box>
          <Box>{`${usedCPU} / ${specCPU}`}</Box>
        </Box>
      </Box>
      <Box sx={podDataColumnSx}>
        <Box sx={podDataRowSx}>
          <Box sx={podDataRowTagSx}>Memory %:</Box>
          <Box>{memPercent}</Box>
        </Box>
        <Box sx={podDataRowSx}>
          <Box sx={podDataRowTagSx}>Memory:</Box>
          <Box>{`${usedMem} / ${specMem}`}</Box>
        </Box>
      </Box>
    </Box>
  );
}
