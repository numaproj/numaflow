import React from "react";
import Box from "@mui/material/Box";
import { StatusBar } from "../../../StatusBar";
import { Help } from "../../../Help";
import circleCheck from "../../../../../images/checkmark-circle.png";
import circleDash from "../../../../../images/circle-dash.png";
import { ACTIVE, INACTIVE } from "../../../../../utils";

import "./style.css";

export interface SummaryStatusesProps {
  title: string;
  active: number;
  inActive: number;
  healthy: number;
  warning: number;
  critical: number;
  activeText?: string;
  inAcitveText?: string;
  tooltip?: any;
  linkComponent?: React.ReactNode;
}

export function SummaryStatuses({
  title,
  active,
  inActive,
  healthy,
  warning,
  critical,
  activeText = ACTIVE,
  inAcitveText = INACTIVE,
  tooltip,
  linkComponent,
}: SummaryStatusesProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "row",
        alignItems: "center",
        flexGrow: 1,
        justifyContent: "center",
      }}
    >
      <Box
        sx={{
          width: "fit-content",
          flexGrow: 1,
          alignItems: "center",
          justifyContent: "center",
          paddingLeft: "1.6rem",
        }}
      >
        <span className="summary-statuses-title">{title}</span>
        <Box
          sx={{ display: "flex", flexDirection: "row", marginTop: "0.5rem" }}
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
                marginLeft: "0.5rem",
              }}
            >
              <span className="summary-statuses-count">{active}</span>
              <span className="summary-statuses-count">{inActive}</span>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "0.5rem",
              }}
            >
              <span className="summary-statuses-active-text">{activeText}</span>
              <span className="summary-statuses-active-text">
                {inAcitveText}
              </span>
            </Box>
          </Box>
          <Box
            sx={{
              marginTop: "0.5rem",
              marginLeft: "1.6rem",
            }}
          >
            <StatusBar
              healthy={healthy}
              warning={warning}
              critical={critical}
            />
          </Box>
          {linkComponent && (
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "2.4rem",
              }}
            >
              {linkComponent}
            </Box>
          )}
        </Box>
      </Box>
      {tooltip && (
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            alignItems: "right",
            justifyContent: "flex-end",
            paddingRight: "0.32rem",
            marginRight: "1.6rem",
            height: "100%",
          }}
        >
          <Help tooltip={tooltip} />
        </Box>
      )}
    </Box>
  );
}
