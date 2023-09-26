import React from "react";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import circleCheck from "../../../../../images/checkmark-circle.png";
import heartFill from "../../../../../images/heart-fill.png";

import './style.css'

export function PipelineISBStatus() {
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
      <Box sx={{ display: "flex", flexDirection: "row" }}>
        <Box sx={{ width: "fit-content", marginRight: "1em" }}>
          <span className="pipeline-status-title">ISB Status</span>
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
              <span className="summary-statuses-active-text">Running</span>
              <span className="summary-statuses-active-text">
                    Warning
                  </span>
            </Box>
          </Box>
        </Box>
        <Box sx={{ width: "fit-content" }}>
          <span className="pipeline-status-title">ISB SUMMARY</span>
          <Box sx={{ display: "flex", flexDirection: "row" }}>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "0.3125rem",
              }}
            >
              <span className="isb-status-subtitle">ISB Type: </span>
              <span className="isb-status-subtitle">
                    ISB Size:
                  </span>
              <span className="isb-status-subtitle">
                    ISB Services:
                  </span>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "0.3125rem",
              }}
            >
              <div className="isb-status-text"><Chip label="Chip Filled" /></div>
              <span className="isb-status-text">Large</span>
              <span className="isb-status-text">Service name</span>
            </Box>
          </Box>
        </Box>
      </Box>
    </Box>
  )
}
