import React from "react";
import Box from "@mui/material/Box";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";

export interface BuffersProps {
  buffers: any[];
}

export function Buffers({ buffers }: BuffersProps) {
  if (!buffers) {
    return <div>{`No resources found.`}</div>;
  }

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      <TableContainer sx={{ maxHeight: "37.5rem", backgroundColor: "#FFF" }}>
        <Table stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell>Partition</TableCell>
              <TableCell>isFull</TableCell>
              <TableCell>AckPending</TableCell>
              <TableCell>Pending</TableCell>
              <TableCell>Buffer Length</TableCell>
              <TableCell>Buffer Usage</TableCell>
              <TableCell>Total Pending Messages</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {!buffers.length && (
              <TableRow>
                <TableCell colSpan={7} align="center">
                  No buffer information found
                </TableCell>
              </TableRow>
            )}
            {!!buffers.length &&
              buffers.map((buffer, idx) => {
                let isFull;
                if (buffer?.isFull) {
                  isFull = "yes";
                } else {
                  isFull = "no";
                }
                let bufferUsage = "";
                if (typeof buffer?.bufferUsage !== "undefined") {
                  bufferUsage = (buffer?.bufferUsage * 100).toFixed(2);
                }
                return (
                  <TableRow key={`node-buffer-info-${idx}`}>
                    <TableCell>{buffer?.bufferName}</TableCell>
                    <TableCell data-testid="isFull">{isFull}</TableCell>
                    <TableCell data-testid="ackPending">
                      {buffer?.ackPendingCount}
                    </TableCell>
                    <TableCell data-testid="pending">
                      {buffer?.pendingCount}
                    </TableCell>
                    <TableCell data-testid="bufferLength">
                      {buffer?.bufferLength}
                    </TableCell>
                    <TableCell data-testid="usage">{bufferUsage}%</TableCell>
                    <TableCell data-testid="totalMessages">
                      {buffer?.totalMessages}
                    </TableCell>
                  </TableRow>
                );
              })}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
}
