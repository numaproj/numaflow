import React from "react";
import Box from "@mui/material/Box";
import {
  IconsStatusMap,
  HTMLlTooltip,
  StatusString,
} from "../../../../../utils";

import "./style.css";

export interface MonoVertexStatusProps {
  status: any;
  healthStatus: string;
  healthData: any;
}

export function MonoVertexStatus({
  status,
  healthStatus,
  healthData,
}: MonoVertexStatusProps) {
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
                src={IconsStatusMap[healthStatus]}
                alt={healthStatus}
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
              <HTMLlTooltip
                title={
                  <Box>
                    <Box className={"health-tooltip-text"}>
                      <Box style={{ fontWeight: 600 }}>
                        Resource Health (
                        {healthData.resourceHealthStatus.toUpperCase()}) :
                      </Box>
                      <Box>{healthData.resourceHealthMessage}</Box>
                    </Box>
                    <Box className={"health-tooltip-text"}>
                      <Box style={{ fontWeight: 600 }}>
                        Data Flow Health (
                        {healthData.dataHealthStatus.toUpperCase()}) :
                      </Box>
                      <Box>{healthData.dataHealthMessage}</Box>
                    </Box>
                  </Box>
                }
                placement="right-end"
                arrow
              >
                <span className="pipeline-logo-text">
                  {StatusString[healthStatus]}
                </span>
              </HTMLlTooltip>
            </Box>
          </Box>
        </Box>
      </Box>
    </Box>
  );
}
