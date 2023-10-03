import React from "react";
import Box from "@mui/material/Box";
import {IconsStatusMap, ISBStatusString} from "../../../../../utils";

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
                {ISBStatusString[isbData?.isbService?.status?.phase]}
              </span>
              <span className="pipeline-logo-text">{ISBStatusString[isbData?.status]}</span>
            </Box>
          </Box>
        </Box>
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
                <span>{isbData?.isbService?.status?.type}</span>
                {/*<Chip*/}
                {/*  label={isbData?.isbService?.status?.type}*/}
                {/*  sx={{ height: "20px" }}*/}
                {/*/>*/}
              </div>
            </Box>
            <Box
              sx={{
                display: "flex",
                flexDirection: "column",
                marginLeft: "2rem"
              }}
            >
              <div className="pipeline-summary-text">
                <span className="pipeline-summary-subtitle">
                  <div className="pipeline-summary-text">
                    <span className="pipeline-summary-subtitle">Size: </span>
                    <span>
                      {
                        isbData?.isbService?.spec[
                          isbData?.isbService?.status?.type
                        ]?.replicas
                      }
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
