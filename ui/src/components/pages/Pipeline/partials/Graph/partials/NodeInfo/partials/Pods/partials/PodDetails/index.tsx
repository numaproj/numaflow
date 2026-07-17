import React from "react";
import Box from "@mui/material/Box";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import { PodLogs } from "./partials/PodLogs";
import { PodDetailProps } from "../../../../../../../../../../../types/declarations/pods";

import "./style.css";

const headerSx = {
  height: "2rem",
  marginBottom: "1rem",
  fontWeight: 600,
  fontSize: "2rem",
};

const LOGS_TAB_INDEX = 0;

export function PodDetail({
  namespaceId,
  type,
  containerName,
  pod,
}: PodDetailProps) {
  if (!pod) return null;

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
        value={LOGS_TAB_INDEX}
        aria-label="Pods Details Tabs"
      >
        <Tab
          className="vertex-details-tab-selected"
          label="Logs"
          data-testid="logs-tab"
        />
      </Tabs>
      <div className="vertex-details-tab-panel" role="tabpanel">
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
      </div>
    </Box>
  );
}
