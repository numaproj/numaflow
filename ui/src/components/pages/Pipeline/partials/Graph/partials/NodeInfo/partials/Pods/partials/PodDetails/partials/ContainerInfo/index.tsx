import React from "react";
import Box from "@mui/material/Box";
import Table from "@mui/material/Table";
import TableRow from "@mui/material/TableRow";
import TableCell from "@mui/material/TableCell";
import TableBody from "@mui/material/TableBody";
import TableContainer from "@mui/material/TableContainer";
import { calculateCPUPercent, calculateMemoryPercent } from "../../../../../../../../../../../../../utils";
import { PodInfoProps } from "../../../../../../../../../../../../../types/declarations/pods";



export function ContainerInfo({
  containerName,
  containerInfo,
}: PodInfoProps) {

  let cpuPercent = "unavailable";
  if (containerInfo?.TotalCPU && containerInfo?.RequestedCPU){
    cpuPercent = calculateCPUPercent(containerInfo?.TotalCPU,containerInfo?.RequestedCPU)
  }

  let memPercent = "unavailable";
  if (containerInfo?.TotalMemory && containerInfo?.RequestedMemory){
    memPercent = calculateMemoryPercent(containerInfo?.TotalMemory,containerInfo?.RequestedMemory)
  }

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
                <TableCell sx={{ fontWeight: 600 }}>Name</TableCell>
                <TableCell>{containerName}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Status</TableCell>
                <TableCell>{containerInfo?.State}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>CPU %</TableCell>
                <TableCell>{cpuPercent}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>CPU</TableCell>
                <TableCell>{`${containerInfo?.TotalCPU} / ${containerInfo?.RequestedCPU ? containerInfo?.RequestedCPU : "?"}`}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Memory %</TableCell>
                <TableCell>{memPercent}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Memory</TableCell>
                <TableCell>{`${containerInfo?.TotalMemory} / ${containerInfo?.RequestedMemory ? containerInfo?.RequestedMemory : "?"}`}</TableCell>
              </TableRow>
              <TableRow>
                <TableCell sx={{ fontWeight: 600 }}>Restart Count </TableCell>
                <TableCell>{containerInfo?.RestartCount || "0"}</TableCell>
              </TableRow>

              {containerInfo?.LastTerminationReason && (
                <TableRow>
                  <TableCell sx={{ fontWeight: 600 }}>
                    Last Termination Reason
                  </TableCell>
                  <TableCell>
                    {containerInfo?.LastTerminationReason}
                  </TableCell>
                </TableRow>
              )}
              {containerInfo?.LastTerminationMessage && (
                <TableRow>
                  <TableCell sx={{ fontWeight: 600 }}>
                    Last Termination Message
                  </TableCell>
                  <TableCell>
                    {containerInfo?.LastTerminationMessage}
                  </TableCell>
                </TableRow>
              )}

              {containerInfo?.WaitingReason && (
                <TableRow>
                  <TableCell sx={{ fontWeight: 600 }}>Waiting Reason</TableCell>
                  <TableCell>{containerInfo?.WaitingReason}</TableCell>
                </TableRow>

              )}

              {containerInfo?.WaitingMessage && (
                <TableRow>
                  <TableCell sx={{ fontWeight: 600 }}>Waiting Message</TableCell>
                  <TableCell>{containerInfo?.WaitingMessage}</TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Box>
    </Box>
  );
}
