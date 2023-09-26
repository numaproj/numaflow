import React from "react";
import Box from "@mui/material/Box";
import circleCheck from "../../../../../images/checkmark-circle.png";
import heartFill from "../../../../../images/heart-fill.png";

import "./style.css"

export function PipelineStatus({status, healthStatus}) {
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
        <span className="pipeline-status-title">Status</span>
        <Box
          sx={{ display: "flex", flexDirection: "row", marginTop: "0.3125rem" }}
        >
          <Box sx={{ display: "flex", flexDirection: "row" }}>
            <Box sx={{ display: "flex", flexDirection: "column" }}>
              <img
                src={circleCheck}
                alt="active"
                className={"summary-statuses-active-logo"}
              />
              <img
                src={heartFill}
                alt="healthy"
                className={"summary-statuses-active-logo"}
              />
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "0.3125rem",
              }}
            >
              <span className="summary-statuses-active-text">{status}</span>
              <span className="summary-statuses-active-text">
                {healthStatus}
              </span>
            </Box>
          </Box>
        </Box>
      </Box>
    </Box>
  )
}
