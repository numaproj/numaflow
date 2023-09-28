import React from "react";
import Box from "@mui/material/Box";
import Table from "@mui/material/Table";
import TableRow from "@mui/material/TableRow";
import TableCell from "@mui/material/TableCell";
import TableBody from "@mui/material/TableBody";
import TableContainer from "@mui/material/TableContainer";
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
    <Box
      data-testid="podInfo"
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        color: "#DCDCDC"
      }}
    >
      <TableContainer sx={{ maxHeight: "37.5rem", backgroundColor: "#FFF" }}>
        <Table stickyHeader>
          <TableBody>
            <TableRow>
              <TableCell sx={{ fontWeight: 600 }}>Name</TableCell>
              <TableCell>{podName}</TableCell>
            </TableRow>
            <TableRow>
              <TableCell sx={{ fontWeight: 600 }}>CPU %</TableCell>
              <TableCell>{cpuPercent}</TableCell>
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
  );
}
