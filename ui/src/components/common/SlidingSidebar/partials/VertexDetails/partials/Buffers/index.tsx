import React, { useContext } from "react";
import Box from "@mui/material/Box";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import { MetricsModalWrapper } from "../../../../../MetricsModalWrapper";
import { VERTEX_PENDING_MESSAGES } from "../../../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodDetails/partials/Metrics/utils/constants";
import { AppContextProps } from "../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../App";

export interface BuffersProps {
  buffers: any[];
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  type: string;
}

export function Buffers({
  buffers,
  namespaceId,
  pipelineId,
  vertexId,
  type,
}: BuffersProps) {
  if (!buffers) {
    return <div>{`No resources found.`}</div>;
  }
  const { disableMetricsCharts } = useContext<AppContextProps>(AppContext);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      <TableContainer sx={{ maxHeight: "60rem", backgroundColor: "#FFF" }}>
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
                      <MetricsModalWrapper
                        disableMetricsCharts={disableMetricsCharts}
                        namespaceId={namespaceId}
                        pipelineId={pipelineId}
                        vertexId={vertexId}
                        type={type}
                        metricDisplayName={VERTEX_PENDING_MESSAGES}
                        value={buffer?.totalMessages}
                      />
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
