import React from "react";
import Box from "@mui/material/Box";
import Table from "@mui/material/Table";
import TableRow from "@mui/material/TableRow";
import TableCell from "@mui/material/TableCell";
import TableBody from "@mui/material/TableBody";
import TableContainer from "@mui/material/TableContainer";
import { getPodContainerUsePercentages } from "../../../../../../../../../../../../../utils";
import { PodInfoProps } from "../../../../../../../../../../../../../types/declarations/pods";

export function PodInfo({ pod, podDetails, containerName }: PodInfoProps) {
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
  const podName = pod?.name?.slice(0, pod?.name?.lastIndexOf("-"));

  return (
    <Box sx={{ padding: "1.6rem" }}>
      <Box sx={{ fontWeight: 600 }}>Container Info</Box>
      <Box
        data-testid="podInfo"
        sx={{
          display: "flex",
          flexDirection: "column",
          height: "100%",
          color: "#DCDCDC",
        }}
      >
        <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
          <Table stickyHeader>
            <TableBody>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Pod</TableCell>
                <TableCell>{podName}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Container</TableCell>
                <TableCell>{containerName}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>CPU %</TableCell>
                <TableCell>{cpuPercent}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>CPU</TableCell>
                <TableCell>{`${usedCPU} / ${specCPU}`}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>MEMORY %</TableCell>
                <TableCell>{memPercent}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>MEMORY</TableCell>
                <TableCell>{`${usedMem} / ${specMem}`}</TableCell>
              </TableRow>
            </TableBody>
          </Table>
        </TableContainer>
      </Box>
    </Box>
  );
}
