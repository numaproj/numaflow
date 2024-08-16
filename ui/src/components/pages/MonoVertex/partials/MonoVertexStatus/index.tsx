import React from "react";
import Box from "@mui/material/Box";
import { IconsStatusMap, StatusString } from "../../../../../utils";

import "./style.css";

export interface MonoVertexStatusProps {
  status: any;
}

export function MonoVertexStatus({ status }: MonoVertexStatusProps) {
  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        marginTop: "0.6rem",
        flexGrow: 1,
        paddingLeft: "1.6rem",
      }}
    >
      <Box sx={{ width: "fit-content" }}>
        <span className={"pipeline-status-title"}>STATUS</span>
        <Box
          sx={{ display: "flex", flexDirection: "row", marginTop: "0.5rem" }}
        >
          <Box sx={{ display: "flex", flexDirection: "row" }}>
            <Box sx={{ display: "flex", flexDirection: "column" }}>
              <img
                src={IconsStatusMap[status]}
                alt={status}
                className={"pipeline-logo"}
              />
              <img
                src={IconsStatusMap["healthy"]}
                alt={"healthy"}
                className={"pipeline-logo"}
              />
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "0.5rem",
              }}
            >
              <span className="pipeline-logo-text">{StatusString[status]}</span>

              <span className="pipeline-logo-text">
                {StatusString["healthy"]}
              </span>
            </Box>
          </Box>
        </Box>
      </Box>
    </Box>
  );
}
