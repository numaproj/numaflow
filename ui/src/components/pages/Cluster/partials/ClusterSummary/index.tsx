import React, { useMemo, useState, useCallback } from "react";
import Box from "@mui/material/Box";
import { ClusterSummaryData } from "../../../../../utils/fetchWrappers/clusterSummaryFetch";
import { StatusBar } from "../../../../common/StatusBar";
import circleCheck from "../../../../../images/checkmark-circle.png";
import circleDash from "../../../../../images/circle-dash.png";

import "./style.css";

export interface ClusterSummaryProps {
  data: ClusterSummaryData;
}

const separator = <div className="cluster-summary-separator" />;

const sectionContainerStyle = {
  display: "flex",
  flexDirection: "column",
  alignSelf: "flex-start",
};

const getStatusComponent = (
  active: number,
  inActive: number,
  healthy: number,
  warning: number,
  critical: number
) => {
  return (
    <Box sx={{ display: "flex", flexDirection: "row", marginTop: "0.3125rem" }}>
      <Box sx={{ display: "flex", flexDirection: "row" }}>
        <Box sx={{ display: "flex", flexDirection: "column" }}>
          <img
            src={circleCheck}
            alt="active"
            className={"cluster-summary-active-logo"}
          />
          <img
            src={circleDash}
            alt="inactive"
            className={"cluster-summary-active-logo"}
          />
        </Box>
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            marginLeft: "0.3125rem",
          }}
        >
          <span className="cluster-summary-small-count">{active}</span>
          <span className="cluster-summary-small-count">{inActive}</span>
        </Box>
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            marginLeft: "0.3125rem",
          }}
        >
          <span className="cluster-summary-active-text">Active</span>
          <span className="cluster-summary-active-text">Non-Active</span>
        </Box>
      </Box>
      <Box
        sx={{
          marginTop: "0.3125rem",
          marginLeft: "1rem",
        }}
      >
        <StatusBar healthy={healthy} warning={warning} critical={critical} />
      </Box>
    </Box>
  );
};

export function ClusterSummary({ data }: ClusterSummaryProps) {
  const pipelinesStatus = useMemo(() => {
    return getStatusComponent(
      data.pipelinesActiveCount,
      data.pipelinesInactiveCount,
      data.pipelinesHealthyCount,
      data.pipelinesWarningCount,
      data.pipelinesCriticalCount
    );
  }, [data]);

  const isbsStatus = useMemo(() => {
    return getStatusComponent(
      data.isbsActiveCount,
      data.isbsInactiveCount,
      data.isbsHealthyCount,
      data.isbsWarningCount,
      data.isbsCriticalCount
    );
  }, [data]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
        width: "100%",
        marginLeft: "2rem",
        flexWrap: "wrap",
        justifyContent: "space-around",
      }}
    >
      <Box sx={sectionContainerStyle}>
        <span className="cluster-summary-title">Namespaces</span>
        <span className="cluster-summary-large-count">
          {data.namespacesCount}
        </span>
      </Box>
      {separator}
      <Box sx={sectionContainerStyle}>
        <span className="cluster-summary-title">Pipelines</span>
        <span className="cluster-summary-large-count">
          {data.pipelinesCount}
        </span>
      </Box>
      {separator}
      <Box sx={sectionContainerStyle}>
        <span className="cluster-summary-title">Pipelines Status</span>
        {pipelinesStatus}
      </Box>
      {separator}
      <Box sx={sectionContainerStyle}>
        <span className="cluster-summary-title">ISB Services</span>
        <span className="cluster-summary-large-count">{data.isbsCount}</span>
      </Box>
      {separator}
      <Box sx={sectionContainerStyle}>
        <span className="cluster-summary-title">ISB Services Status</span>
        {isbsStatus}
      </Box>
    </Box>
  );
}
