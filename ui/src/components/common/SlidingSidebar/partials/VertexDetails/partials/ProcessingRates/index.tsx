import React, { useEffect, useState } from "react";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import { PipelineVertexMetrics } from "../../../../../../../types/declarations/pipeline";
import { usePiplelineVertexMetricsFetch } from "../../../../../../../utils/fetchWrappers/piplelineVertexMetricsFetch";

import "./style.css";

export interface ProcessingRatesProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
}

export function ProcessingRates({
  namespaceId,
  pipelineId,
  vertexId,
}: ProcessingRatesProps) {
  const [vertexMetric, setVertexMetric] = useState<PipelineVertexMetrics | undefined>();
  const { data, loading, error } = usePiplelineVertexMetricsFetch({
    namespace: namespaceId,
    pipeline: pipelineId,
  });

  useEffect(() => {
    if (!data) {
      return;
    }
    setVertexMetric(data.find((metric) => metric.vertexId === vertexId));
  }, [data, vertexId]);

  if (loading) {
    return (
      <Box sx={{ display: "flex", justifyContent: "center", marginTop: "1rem" }}>
        <CircularProgress />
      </Box>
    );
  }
  if (error) {
    return <div>{`Error loading processing rates: ${error}`}</div>;
  }
  if (!data || !vertexMetric) {
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
              <TableCell>1m</TableCell>
              <TableCell>5m</TableCell>
              <TableCell>15m</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {!vertexMetric.metrics.length && (
              <TableRow>
                <TableCell colSpan={4} align="center">
                  No metrics found
                </TableCell>
              </TableRow>
            )}
            {!!vertexMetric.metrics.length &&
              vertexMetric.metrics.map((metric) => (
                <TableRow key={metric.partition}>
                  <TableCell>{metric.partition}</TableCell>
                  <TableCell>{metric.oneM}</TableCell>
                  <TableCell>{metric.fiveM}</TableCell>
                  <TableCell>{metric.fifteenM}</TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
}
