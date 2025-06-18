import React, { Dispatch, SetStateAction, useContext } from "react";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Tooltip,
  Typography,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import LineChartComponent from "./partials/LineChart";
import { useMetricsDiscoveryDataFetch } from "../../../../../../../../../../../../../utils/fetchWrappers/metricsDiscoveryDataFetch";
import {
  dimensionReverseMap,
  VERTEX_PENDING_MESSAGES,
  UDF_READ_PROCESSING_RATE,
  UDF_WRITE_PROCESSING_RATE,
  UDF_PROCESSING_TIME_LATENCY,
  VERTEX_PROCESSING_TIME_LATENCY,
  UDF_DROP_TOTAL,
  UDF_ERROR_TOTAL,
} from "./utils/constants";
import {
  VertexDetailsContext,
  VertexDetailsContextProps,
} from "../../../../../../../../../../../../common/SlidingSidebar/partials/VertexDetails";
import { Pod } from "../../../../../../../../../../../../../types/declarations/pods";

import "./style.css";

export interface MetricsProps {
  namespaceId: string;
  pipelineId: string;
  type: string;
  vertexId?: string;
  metricDisplayName?: string;
  setMetricsFound?: Dispatch<SetStateAction<boolean>>;
  presets?: any;
  pod?: Pod;
}

export function Metrics({
  namespaceId,
  pipelineId,
  type,
  vertexId,
  metricDisplayName,
  pod,
  setMetricsFound,
  presets,
}: MetricsProps) {
  const {
    metricsDiscoveryData: discoveredMetrics,
    error: discoveredMetricsError,
    loading: discoveredMetricsLoading,
  } = useMetricsDiscoveryDataFetch({
    objectType: dimensionReverseMap[type],
  });

  const {
    expanded,
    setExpanded,
    presets: presetsFromContext,
    setPresets,
  } = useContext<VertexDetailsContextProps>(VertexDetailsContext);

  const handleAccordionChange =
    (panel: string) => (_: any, isExpanded: boolean) => {
      setPresets(undefined);
      setExpanded((prevExpanded) => {
        const newExpanded = new Set(prevExpanded);
        isExpanded ? newExpanded.add(panel) : newExpanded.delete(panel);
        return newExpanded;
      });
    };

  if (discoveredMetricsLoading) {
    return (
      <Box className={"metrics-discover-metrics-loading"}>
        <CircularProgress />
      </Box>
    );
  }

  if (discoveredMetricsError) {
    return (
      <Box className={"metrics-discover-metrics-error"}>
        Failed to discover metrics for the {type}: {discoveredMetricsError}
      </Box>
    );
  }

  if (discoveredMetrics == undefined) return <Box>No metrics found</Box>;

  if (metricDisplayName) {
    const discoveredMetric = discoveredMetrics?.data?.find(
      (m: any) => m?.display_name === metricDisplayName
    );
    if (discoveredMetric) {
      if (setMetricsFound)
        setTimeout(() => {
          setMetricsFound(true);
        }, 100);
      return (
        <LineChartComponent
          namespaceId={namespaceId}
          pipelineId={pipelineId}
          type={type}
          metric={discoveredMetric}
          vertexId={vertexId}
          presets={presets}
          fromModal
          pod={pod}
        />
      );
    } else {
      if (setMetricsFound) setMetricsFound(false);
      return (
        <Box className={"metrics-discover-metrics-not-found"}>
          No metrics found
        </Box>
      );
    }
  }

  return (
    <Box sx={{ height: "100%" }}>
      {discoveredMetrics?.data?.map((metric: any) => {
        if (
          type === "source" &&
          metric?.display_name === VERTEX_PENDING_MESSAGES
        )
          return null;

        if (
          type !== "udf" &&
          [
            UDF_READ_PROCESSING_RATE,
            UDF_WRITE_PROCESSING_RATE,
            UDF_PROCESSING_TIME_LATENCY,
            UDF_DROP_TOTAL,
            UDF_ERROR_TOTAL,
          ].includes(metric?.display_name)
        )
          return null;

        if (
          type === "udf" &&
          metric?.display_name === VERTEX_PROCESSING_TIME_LATENCY
        )
          return null;

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
              <Box className={"metrics-accordion-summary"}>
                {metric?.display_name || metric?.metric_name}
                <Tooltip
                  title={
                    <Typography className={"metrics-accordion-summary-tooltip"}>
                      {metric?.metric_description ||
                        metric?.display_name ||
                        metric?.metric_name}
                    </Typography>
                  }
                  arrow
                  placement={"top-start"}
                >
                  <Box>
                    <InfoOutlinedIcon sx={{ cursor: "pointer" }} />
                  </Box>
                </Tooltip>
              </Box>
            </AccordionSummary>
            <AccordionDetails>
              {expanded?.has(panelId) && (
                <LineChartComponent
                  namespaceId={namespaceId}
                  pipelineId={pipelineId}
                  type={type}
                  metric={metric}
                  vertexId={vertexId}
                  presets={presetsFromContext}
                  pod={pod}
                />
              )}
            </AccordionDetails>
          </Accordion>
        );
      })}
    </Box>
  );
}
