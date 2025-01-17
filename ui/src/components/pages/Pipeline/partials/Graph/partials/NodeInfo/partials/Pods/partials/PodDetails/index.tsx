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
  height: "2rem",
  marginBottom: "1rem",
  fontWeight: 600,
  fontSize: "2rem",
};

const LOGS_TAB_INDEX = 0;
const METRICS_TAB_INDEX = 1;

export function PodDetail({
  namespaceId,
  pipelineId,
  type,
  containerName,
  pod,
  podDetails,
  vertexId
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
        height: "100%",
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
        {!disableMetricsCharts && (
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
          <Box
            sx={{
              p: "1.6rem",
              height: "calc(100% - 3.2rem)",
            }}
          >
            <Box sx={headerSx}>Container Logs</Box>
            <Box sx={{ height: "calc(100% - 3rem)" }}>
              <PodLogs
                namespaceId={namespaceId}
                podName={pod.name}
                containerName={containerName}
                type={type}
              />
            </Box>
          </Box>
        )}
      </div>
      {!disableMetricsCharts && (
        <div
          className="vertex-details-tab-panel"
          role="tabpanel"
          hidden={selectedTab !== METRICS_TAB_INDEX}
        >
          {selectedTab === METRICS_TAB_INDEX && (
            <Box
              sx={{
                p: "1.6rem",
                height: "calc(100% - 10rem)",
                overflow: "scroll",
              }}
            >
              <Metrics
                namespaceId={namespaceId}
                pipelineId={pipelineId}
                type={type}
                vertexId={vertexId}
                podDetails={podDetails}
              />
            </Box>
          )}
        </div>
      )}
    </Box>
  );
}
