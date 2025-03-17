import React, { useContext } from "react";
import Box from "@mui/material/Box";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import { Metrics } from "./partials/Metrics";
import { PodLogs } from "./partials/PodLogs";
import { Errors } from "./partials/Errors";
import { PodDetailProps } from "../../../../../../../../../../../types/declarations/pods";
import { AppContextProps } from "../../../../../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../../../../../App";
import {
  VertexDetailsContext,
  VertexDetailsContextProps,
} from "../../../../../../../../../../common/SlidingSidebar/partials/VertexDetails";

import "./style.css";

const headerSx = {
  height: "2rem",
  marginBottom: "1rem",
  fontWeight: 600,
  fontSize: "2rem",
};

const LOGS_TAB_INDEX = 0;
const ERRORS_TAB_INDEX = 1;
const METRICS_TAB_INDEX = 2;

export function PodDetail({
  namespaceId,
  pipelineId,
  type,
  containerName,
  pod,
  vertexId,
}: PodDetailProps) {
  if (!pod) return null;

  const { disableMetricsCharts } = useContext<AppContextProps>(AppContext);

  const { podsViewTab, setPodsViewTab } =
    useContext<VertexDetailsContextProps>(VertexDetailsContext);
  const handleTabChange = (_: any, newValue: number) => {
    setPodsViewTab(newValue);
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
        value={podsViewTab}
        onChange={handleTabChange}
        aria-label="Pods Details Tabs"
      >
        <Tab
          className={
            podsViewTab === LOGS_TAB_INDEX
              ? "vertex-details-tab-selected"
              : "vertex-details-tab"
          }
          label="Logs"
          data-testid="logs-tab"
        />
        <Tab
          className={
            podsViewTab === ERRORS_TAB_INDEX
              ? "vertex-details-tab-selected"
              : "vertex-details-tab"
          }
          label="Errors"
          data-testid="errors-tab"
        />
        {!disableMetricsCharts && (
          <Tab
            className={
              podsViewTab === METRICS_TAB_INDEX
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
        hidden={podsViewTab !== LOGS_TAB_INDEX}
      >
        {podsViewTab === LOGS_TAB_INDEX && (
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
      <div
        className="vertex-details-tab-panel"
        role="tabpanel"
        hidden={podsViewTab !== ERRORS_TAB_INDEX}
      >
        {podsViewTab === ERRORS_TAB_INDEX && (
          <Box
            sx={{
              p: "1.6rem",
              height: "calc(100% - 3rem)",
            }}
          >
            <Errors containers={pod?.containers} />
          </Box>
        )}
      </div>
      {!disableMetricsCharts && (
        <div
          className="vertex-details-tab-panel"
          role="tabpanel"
          hidden={podsViewTab !== METRICS_TAB_INDEX}
        >
          {podsViewTab === METRICS_TAB_INDEX && (
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
                pod={pod}
              />
            </Box>
          )}
        </div>
      )}
    </Box>
  );
}
