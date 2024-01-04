import React, { useEffect, useState } from "react";
import Box from "@mui/material/Box";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import { PipelineVertexMetric } from "../../../../../../../types/declarations/pipeline";

import "./style.css";

export interface ProcessingRatesProps {
  vertexId: string;
  pipelineId: string;
  vertexMetrics: any;
}

export function ProcessingRates({
  vertexMetrics,
  pipelineId,
  vertexId,
}: ProcessingRatesProps) {
  const [foundRates, setFoundRates] = useState<PipelineVertexMetric[]>([]);

  useEffect(() => {
    if (!vertexMetrics || !pipelineId || !vertexId) {
      return;
    }
    const vertexData = vertexMetrics[vertexId];
    if (!vertexData) {
      return;
    }
    const rates: PipelineVertexMetric[] = [];
    vertexData.forEach((item: any, index: number) => {
      if (item.pipeline !== pipelineId || !item.processingRates) {
        return; // continue
      }
      rates.push({
        partition: index,
        oneM: item.processingRates["1m"]
          ? item.processingRates["1m"].toFixed(2)
          : 0,
        fiveM: item.processingRates["5m"]
          ? item.processingRates["5m"].toFixed(2)
          : 0,
        fifteenM: item.processingRates["15m"]
          ? item.processingRates["15m"].toFixed(2)
          : 0,
      });
    });
    setFoundRates(rates);
  }, [vertexMetrics, pipelineId, vertexId]);

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
            {!foundRates.length && (
              <TableRow>
                <TableCell colSpan={4} align="center">
                  No metrics found
                </TableCell>
              </TableRow>
            )}
            {!!foundRates.length &&
              foundRates.map((metric) => (
                <TableRow key={metric.partition}>
                  <TableCell>{metric.partition}</TableCell>
                  <TableCell>{metric.oneM}/sec</TableCell>
                  <TableCell>{metric.fiveM}/sec</TableCell>
                  <TableCell>{metric.fifteenM}/sec</TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
}
