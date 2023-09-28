import React from "react";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import { IconsStatusMap } from "../../../../../utils";

import "./style.css";

export function PipelineISBStatus({ isbData }) {
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
                src={IconsStatusMap[isbData?.isbService?.status?.phase]}
                alt={IconsStatusMap[isbData?.isbService?.status?.phase]}
                className={"pipeline-logo"}
              />
              <img
                src={IconsStatusMap[isbData?.status]}
                alt={isbData?.status}
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
                {isbData?.isbService?.status?.phase}
              </span>
              <span className="pipeline-logo-text">{isbData?.status}</span>
            </Box>
          </Box>
        </Box>
        <Box sx={{ width: "fit-content" }}>
          <span className="pipeline-status-title">ISB Summary</span>
          <Box sx={{ display: "flex", flexDirection: "row" }}>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "0.3125rem",
              }}
            >
              <div className="pipeline-summary-text">
                <span className="pipeline-summary-subtitle">ISB Type: </span>
              </div>
              <div className="pipeline-summary-text">
                <span className="pipeline-summary-subtitle">ISB Size: </span>
              </div>
              <div className="pipeline-summary-text">
                <span className="pipeline-summary-subtitle">
                  ISB Services:{" "}
                </span>
              </div>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "0.3125rem",
              }}
            >
              <div className="isb-status-text">
                <Chip
                  label={isbData?.isbService?.status?.type}
                  sx={{ height: "20px" }}
                />
              </div>
              <div className="isb-status-text">
                <span>
                  {
                    isbData?.isbService?.spec[isbData?.isbService?.status?.type]
                      ?.replicas
                  }
                </span>
              </div>
              <div className="isb-status-text">
                <span>{isbData?.name}</span>
              </div>
            </Box>
          </Box>
        </Box>
      </Box>
    </Box>
  );
}
