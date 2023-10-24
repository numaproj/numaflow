import React from "react";
import Box from "@mui/material/Box";
import {
  GetISBType,
  UNKNOWN,
} from "../../../../../utils";

export function PipelineISBSummaryStatus({ isbData }) {
  const isbType = GetISBType(isbData?.isbService?.spec) || UNKNOWN;
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
          <span className="pipeline-status-title">ISB SERVICES SUMMARY</span>
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
              marginTop: "0.3125rem",
            }}
          >
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "0.3125rem",
              }}
            >
              <div className="pipeline-summary-text">
                <span className="pipeline-summary-subtitle">Name: </span>
              </div>
              <div className="pipeline-summary-text">
                <span className="pipeline-summary-subtitle">Type: </span>
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
                <span>{isbData?.name}</span>
              </div>
              <div className="isb-status-text">
                <span>{isbType}</span>
              </div>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "2rem",
              }}
            >
              <div className="pipeline-summary-text">
                <span className="pipeline-summary-subtitle">
                  <div className="pipeline-summary-text">
                    <span className="pipeline-summary-subtitle">Size: </span>
                    <span>
                      {isbType && isbData?.isbService?.spec[isbType]
                        ? isbData?.isbService?.spec[isbType].replicas
                        : UNKNOWN}
                    </span>
                  </div>
                </span>
              </div>
            </Box>
          </Box>
        </Box>
      </Box>
    </Box>
  );
}
