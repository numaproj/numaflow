import React from "react";
import Box from "@mui/material/Box";
import { IconsStatusMap, StatusString } from "../../../../../utils";

import "./style.css";

export function PipelineStatus({ status, healthStatus }) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        marginTop: "0.375rem",
        alignItems: "center",
        flexGrow: 1,
        justifyContent: "center",
      }}
    >
      <Box sx={{ width: "fit-content" }}>
        <span className={"pipeline-status-title"}>STATUS</span>
        <Box
          sx={{ display: "flex", flexDirection: "row", marginTop: "0.3125rem" }}
        >
          <Box sx={{ display: "flex", flexDirection: "row" }}>
            <Box sx={{ display: "flex", flexDirection: "column" }}>
              <img
                src={IconsStatusMap[status]}
                alt={status}
                className={"pipeline-logo"}
              />
              <img
                src={IconsStatusMap[healthStatus]}
                alt={healthStatus}
                className={"pipeline-logo"}
              />
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "0.3125rem",
              }}
            >
              <span className="pipeline-logo-text">{StatusString[status]}</span>
              <span className="pipeline-logo-text">
                {StatusString[healthStatus]}
              </span>
            </Box>
          </Box>
        </Box>
      </Box>
    </Box>
  );
}
