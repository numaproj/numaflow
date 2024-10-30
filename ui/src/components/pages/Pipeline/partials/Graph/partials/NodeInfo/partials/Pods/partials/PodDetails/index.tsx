import React, { useContext, useState } from "react";
import Box from "@mui/material/Box";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import { Metrics } from "./partials/Metrics";
import { PodLogs } from "./partials/PodLogs";
import { PodDetailProps } from "../../../../../../../../../../../types/declarations/pods";
import { AppContextProps } from "../../../../../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../../../../../App";

import "./style.css";

const headerSx = {
  marginBottom: "1rem",
  fontWeight: "bold",
};

const LOGS_TAB_INDEX = 0;
const METRICS_TAB_INDEX = 1;

export function PodDetail({
  namespaceId,
  pipelineId,
  type,
  containerName,
  pod,
}: PodDetailProps) {
  if (!pod) return null;

  const { disableMetricsCharts } = useContext<AppContextProps>(AppContext);

  const [selectedTab, setSelectedTab] = useState<number>(0);
  const handleTabChange = (_: any, newValue: number) => {
    setSelectedTab(newValue);
  };

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        width: "100%",
        marginTop: "1.6rem",
      }}
    >
      <Tabs
        value={selectedTab}
        onChange={handleTabChange}
        aria-label="Pods Details Tabs"
      >
        <Tab
          className={
            selectedTab === LOGS_TAB_INDEX
              ? "vertex-details-tab-selected"
              : "vertex-details-tab"
          }
          label="Logs"
          data-testid="logs-tab"
        />
        {!disableMetricsCharts && type === "monoVertex" && (
          <Tab
            className={
              selectedTab === METRICS_TAB_INDEX
                ? "vertex-details-tab-selected"
                : "vertex-details-tab"
            }
            label="Metrics"
            data-testid="metrics-tab"
          />
        )}
      </Tabs>
      <div
        className="vertex-details-tab-panel"
        role="tabpanel"
        hidden={selectedTab !== LOGS_TAB_INDEX}
      >
        {selectedTab === LOGS_TAB_INDEX && (
          <Box sx={{ p: "1.6rem" }}>
            <Box sx={headerSx}>Container Logs</Box>
            <PodLogs
              namespaceId={namespaceId}
              podName={pod.name}
              containerName={containerName}
            />
          </Box>
        )}
      </div>
      {!disableMetricsCharts && type === "monoVertex" && (
        <div
          className="vertex-details-tab-panel"
          role="tabpanel"
          hidden={selectedTab !== METRICS_TAB_INDEX}
        >
          {selectedTab === METRICS_TAB_INDEX && (
            <Metrics
              namespaceId={namespaceId}
              pipelineId={pipelineId}
              type={type}
            />
          )}
        </div>
      )}
    </Box>
  );
}
