import React, { useContext, useEffect, useState } from "react";
import Box from "@mui/material/Box";
import TableContainer from "@mui/material/TableContainer";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import { MetricsModalWrapper } from "../../../../../MetricsModalWrapper";
import { PipelineVertexMetric } from "../../../../../../../types/declarations/pipeline";
import { AppContextProps } from "../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../App";

import "./style.css";

export interface ProcessingRatesProps {
  vertexId: string;
  namespaceId: string;
  pipelineId: string;
  type: string;
  vertexMetrics: any;
}

const RATE_LABELS: string[] = ["1m", "5m", "15m"];
const RATE_LABELS_MAP: { [k: string]: string } = {
  "1m": "oneM",
  "5m": "fiveM",
  "15m": "fifteenM",
};

export function ProcessingRates({
  vertexMetrics,
  namespaceId,
  pipelineId,
  type,
  vertexId,
}: ProcessingRatesProps) {
  const { disableMetricsCharts } = useContext<AppContextProps>(AppContext);
  const [foundRates, setFoundRates] = useState<PipelineVertexMetric[]>([]);

  useEffect(() => {
    if (!vertexMetrics || !pipelineId || !vertexId) {
      return;
    }
    const vertexData =
      type === "monoVertex" ? [vertexMetrics] : vertexMetrics[vertexId];
    if (!vertexData) return;

    const rates: PipelineVertexMetric[] = [];
    vertexData.forEach((item: any, index: number) => {
      const key = type === "monoVertex" ? "monoVertex" : "pipeline";
      if (item?.[key] !== pipelineId || !item.processingRates) {
        return; // continue
      }
      rates.push({
        partition: index,
        oneM: item.processingRates["1m"]?.toFixed(2) || 0,
        fiveM: item.processingRates["5m"]?.toFixed(2) || 0,
        fifteenM: item.processingRates["15m"]?.toFixed(2) || 0,
      });
    });
    setFoundRates(rates);
  }, [vertexMetrics, pipelineId, vertexId]);

  const formatRate = (rate?: number): string => {
    return rate !== undefined && rate >= 0 ? `${rate}/sec` : "Not Available";
  };

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
              {type !== "monoVertex" && <TableCell>Partition</TableCell>}
              {RATE_LABELS.map((label: string) => (
                <TableCell key={label}>{label}</TableCell>
              ))}
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
              foundRates.map((metric: any) => (
                <TableRow key={metric.partition}>
                  {type !== "monoVertex" && (
                    <TableCell>{metric.partition}</TableCell>
                  )}
                  {RATE_LABELS?.map((label: string) => (
                    <TableCell key={label}>
                      <MetricsModalWrapper
                        disableMetricsCharts={disableMetricsCharts}
                        namespaceId={namespaceId}
                        pipelineId={pipelineId}
                        vertexId={vertexId}
                        type={type}
                        metricName={
                          type === "monoVertex"
                            ? "monovtx_read_total"
                            : "forwarder_data_read_total"
                        }
                        value={formatRate(metric?.[RATE_LABELS_MAP[label]])}
                        presets={{ duration: label }}
                      />
                    </TableCell>
                  ))}
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
}
