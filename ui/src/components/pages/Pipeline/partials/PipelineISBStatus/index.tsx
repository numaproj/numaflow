import React from "react";
import Box from "@mui/material/Box";
import {
  IconsStatusMap,
  ISBStatusString,
  UNKNOWN,
} from "../../../../../utils";

import "./style.css";

export function PipelineISBStatus({ isbData }) {
  const isbStatus = isbData?.isbService?.status?.phase || UNKNOWN;
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
        <Box sx={{ width: "fit-content", marginRight: "2em", flexGrow: 1 }}>
          <span className="pipeline-status-title">ISB SERVICES STATUS</span>
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              marginTop: "0.3125rem",
            }}
          >
            <Box sx={{ display: "flex", flexDirection: "column" }}>
              <img
                src={IconsStatusMap[isbStatus]}
                alt="Status"
                className={"pipeline-logo"}
              />
              <img
                src={IconsStatusMap[isbData?.status]}
                alt="Health"
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
              <span className="pipeline-logo-text">
                {ISBStatusString[isbStatus]}
              </span>
              <span className="pipeline-logo-text">
                {ISBStatusString[isbData?.status]}
              </span>
            </Box>
          </Box>
        </Box>
      </Box>
    </Box>
  );
}
