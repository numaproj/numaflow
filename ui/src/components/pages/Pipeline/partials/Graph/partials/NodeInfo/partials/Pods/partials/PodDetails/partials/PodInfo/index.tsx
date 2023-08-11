import Box from "@mui/material/Box";
import { getPodContainerUsePercentages } from "../../../../../../../../../../../../../utils";
import { PodInfoProps } from "../../../../../../../../../../../../../types/declarations/pods";

const podInfoSx = {
  display: "flex",
  flexDirection: "row",
};

const podDataRowSx = {
  display: "flex",
  flexDirection: "row",
  paddingBottom: "0.625rem",
};

const podDataColumnSx = {
  display: "flex",
  flexDirection: "column",
  paddingBottom: "0.625rem",
  width: "30%",
};

const podDataRowTagSx = {
  width: "25%",
};

export function PodInfo({ pod, podDetails, containerName }: PodInfoProps) {
  const resourceUsage = getPodContainerUsePercentages(
    pod,
    podDetails,
    containerName
  );

  // CPU
  let usedCPU: string | undefined =
    podDetails?.containerMap?.get(containerName)?.cpu;
  let specCPU: string | undefined =
    pod?.containerSpecMap?.get(containerName)?.cpu;
  if (!usedCPU) {
    usedCPU = "?";
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
    podDetails?.containerMap?.get(containerName)?.memory;
  let specMem: string | undefined =
    pod?.containerSpecMap?.get(containerName)?.memory;
  if (!usedMem) {
    usedMem = "?";
  }
  if (!specMem) {
    specMem = "?";
  }
  let memPercent = "unavailable";
  if (resourceUsage?.memoryPercent) {
    memPercent = `${resourceUsage.memoryPercent.toFixed(2)}%`;
  }
  const podName = pod?.name?.slice(0, pod?.name?.lastIndexOf("-"));

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
