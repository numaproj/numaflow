import React from "react";
import Box from "@mui/material/Box";
import { StatusBar } from "../../../StatusBar";
import circleCheck from "../../../../../images/checkmark-circle.png";
import circleDash from "../../../../../images/circle-dash.png";

import "./style.css";

export interface SummaryStatusesProps {
  title: string;
  active: number;
  inActive: number;
  healthy: number;
  warning: number;
  critical: number;
}

export function SummaryStatuses({
  title,
  active,
  inActive,
  healthy,
  warning,
  critical,
}: SummaryStatusesProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        alignSelf: "flex-start",
      }}
    >
      <span className="summary-statuses-title">{title}</span>
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
              src={circleDash}
              alt="inactive"
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
            <span className="summary-statuses-count">{active}</span>
            <span className="summary-statuses-count">{inActive}</span>
          </Box>
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              marginLeft: "0.3125rem",
            }}
          >
            <span className="summary-statuses-active-text">Active</span>
            <span className="summary-statuses-active-text">Non-Active</span>
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
    </Box>
  );
}
