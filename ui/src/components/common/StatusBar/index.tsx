import React from "react";
import Box from "@mui/material/Box";

import "./style.css";

export interface StatusBarProps {
  healthy: number;
  warning: number;
  critical: number;
}

export function StatusBar({ healthy, warning, critical }: StatusBarProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
      }}
    >
      <div>
        <div className="status-bar">
          <div className="status-bar-tick status-bar-warn-tick" />
          <div className="status-bar-tick status-bar-critical-tick" />
        </div>
      </div>
      <Box sx={{ display: "flex", flexDirection: "row" }}>
        <Box sx={{ display: "flex", flexDirection: "column" }}>
          <span className="status-bar-status-value">{healthy}</span>
          <span className="status-bar-status-text">Healthy</span>
        </Box>
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            marginLeft: "15.1rem",
          }}
        >
          <span className="status-bar-status-value">{warning}</span>
          <span className="status-bar-status-text">Warning</span>
        </Box>
        <Box
          sx={{
            display: "flex",
            flexDirection: "column",
            marginLeft: "0.8rem",
          }}
        >
          <span className="status-bar-status-value">{critical}</span>
          <span className="status-bar-status-text">Critical</span>
        </Box>
      </Box>
    </Box>
  );
}
