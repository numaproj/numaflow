import React from "react";
import Box from "@mui/material/Box";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";

import { BufferInfo } from "../../../../../types/declarations/pipeline";

import "./style.css";

export interface EdgeDetailsProps {
  namespaceId?: string;
  pipelineId?: string;
  edgeId: string;
  from?: string;
  to?: string;
  watermarks?: (string | number)[];
  /** The buffers owned by this edge, one per partition. */
  buffers?: BufferInfo[];
}

const partitionOf = (bufferName: string) =>
  bufferName.substring(bufferName.lastIndexOf("-") + 1);

const percent = (value: number) =>
  isNaN(value) ? "-" : `${(value * 100).toFixed(2)}%`;

export function EdgeDetails({
  edgeId,
  watermarks,
  buffers,
}: EdgeDetailsProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      <Box
        sx={{
          display: "flex",
          flexDirection: "row",
        }}
      >
        <span className="edge-details-header-text">{`${edgeId} Edge`}</span>
      </Box>
      <TableContainer
        sx={{
          maxHeight: "60rem",
          backgroundColor: "#FFF",
          marginTop: "1.6rem",
        }}
      >
        <Table stickyHeader>
          <TableHead>
            <TableRow>
              <TableCell>Partition</TableCell>
              <TableCell>Watermark timestamp</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {(!watermarks || !watermarks.length) && (
              <TableRow>
                <TableCell colSpan={2} align="center">
                  No watermarks found
                </TableCell>
              </TableRow>
            )}
            {!!watermarks &&
              !!watermarks.length &&
              watermarks.map((watermark: string | number, index: number) => (
                <TableRow key={index}>
                  <TableCell>{index}</TableCell>
                  <TableCell>
                    {Number(watermark) < 0
                      ? watermark
                      : `${watermark} (${new Date(
                          Number(watermark)
                        ).toISOString()})`}
                  </TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
      <span
        className="edge-details-header-text"
        style={{ marginTop: "2.4rem" }}
      >
        Buffers
      </span>
      <TableContainer
        sx={{
          maxHeight: "60rem",
          backgroundColor: "#FFF",
          marginTop: "1.6rem",
        }}
      >
        <Table stickyHeader data-testid="edge-buffers">
          <TableHead>
            <TableRow>
              <TableCell>Partition</TableCell>
              <TableCell>Buffer</TableCell>
              <TableCell>isFull</TableCell>
              <TableCell>AckPending</TableCell>
              <TableCell>Pending</TableCell>
              <TableCell>Buffer Length</TableCell>
              <TableCell>Buffer Usage</TableCell>
              <TableCell>Usage Limit</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {(!buffers || !buffers.length) && (
              <TableRow>
                <TableCell colSpan={8} align="center">
                  No buffers found
                </TableCell>
              </TableRow>
            )}
            {!!buffers &&
              buffers.map((buffer: BufferInfo) => (
                <TableRow key={buffer.bufferName}>
                  <TableCell>{partitionOf(buffer.bufferName)}</TableCell>
                  <TableCell>{buffer.bufferName}</TableCell>
                  <TableCell>{buffer.isFull ? "yes" : "no"}</TableCell>
                  <TableCell>{buffer.ackPendingCount}</TableCell>
                  <TableCell>{buffer.pendingCount}</TableCell>
                  <TableCell>{buffer.bufferLength}</TableCell>
                  <TableCell>{percent(Number(buffer.bufferUsage))}</TableCell>
                  <TableCell>
                    {percent(Number(buffer.bufferUsageLimit))}
                  </TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
}
