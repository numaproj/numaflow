import React, { useState } from "react";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import { Accordion, AccordionDetails, AccordionSummary } from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";

import LineChartComponent from "./partials/LineChart";
import { useMetricsDiscoveryDataFetch } from "../../../../../../../../../../../../../utils/fetchWrappers/metricsDiscoveryDataFetch";
import { dimensionReverseMap, metricNameMap } from "./utils/constants";

import "./style.css";

export interface MetricsProps {
  namespaceId: string;
  pipelineId: string;
  type: string;
  vertexId?: string;
}

export function Metrics({ namespaceId, pipelineId, type }: MetricsProps) {
  const {
    metricsDiscoveryData: discoveredMetrics,
    error: discoveredMetricsError,
    loading: discoveredMetricsLoading,
  } = useMetricsDiscoveryDataFetch({
    objectType: dimensionReverseMap[type],
  });

  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const handleAccordionChange =
    (panel: string) => (_: any, isExpanded: boolean) => {
      setExpanded((prevExpanded) => {
        const newExpanded = new Set(prevExpanded);
        isExpanded ? newExpanded.add(panel) : newExpanded.delete(panel);
        return newExpanded;
      });
    };

  if (discoveredMetricsLoading) {
    return (
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          height: "100%",
        }}
      >
        <CircularProgress />
      </Box>
    );
  }

  if (discoveredMetricsError) {
    return (
      <Box sx={{ mt: "2rem", ml: "2rem", fontSize: "1.6rem" }}>
        Failed to discover metrics for the {type}: {discoveredMetricsError}
      </Box>
    );
  }

  if (discoveredMetrics == undefined) return <Box>No metrics found</Box>;

  return (
    <Box sx={{ height: "100%" }}>
      {discoveredMetrics?.data?.map((metric: any) => {
        const panelId = `${metric?.metric_name}-panel`;
        return (
          <Accordion
            expanded={expanded.has(panelId)}
            onChange={handleAccordionChange(panelId)}
            key={panelId}
          >
            <AccordionSummary
              expandIcon={<ExpandMoreIcon />}
              aria-controls={`${metric?.metric_name}-content`}
              id={`${metric?.metric_name}-header`}
            >
              <Box sx={{ textTransform: "capitalize" }}>
                {metricNameMap[metric?.metric_name] || metric?.metric_name}
              </Box>
            </AccordionSummary>
            <AccordionDetails>
              {expanded?.has(panelId) && (
                <LineChartComponent
                  namespaceId={namespaceId}
                  pipelineId={pipelineId}
                  type={type}
                  metric={metric}
                />
              )}
            </AccordionDetails>
          </Accordion>
        );
      })}
    </Box>
  );
}
