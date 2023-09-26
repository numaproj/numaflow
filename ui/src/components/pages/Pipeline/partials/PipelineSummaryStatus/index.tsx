import React from "react";
import Box from "@mui/material/Box";
import circleCheck from "../../../../../images/checkmark-circle.png";
import heartFill from "../../../../../images/heart-fill.png";

import "./style.css"

export function PipelineSummaryStatus({pipeline}) {
  console.log(pipeline)
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
      <Box sx={{ width: "fit-content" }}>
        <span className="pipeline-status-title">Summary</span>
        <Box
          sx={{ display: "flex", flexDirection: "row", marginTop: "0.3125rem" }}
        >
          <Box sx={{ display: "flex", flexDirection: "column", marginRight: ".5rem" }}>
            <div className="pipeline-summary-text"><span className="pipeline-summary-subtitle">Created Timestamp: </span>{pipeline?.metadata?.creationTimestamp}</div>
            <div className="pipeline-summary-text"><span className="pipeline-summary-subtitle">Last Updated Timestamp: </span>{pipeline?.status?.lastUpdated}</div>
            <div className="pipeline-summary-text"><span className="pipeline-summary-subtitle">Last Refresh: </span>2023-12-07T02:02:00Z</div>
          </Box>
          <Box sx={{ display: "flex", flexDirection: "column" }}>
            <div className="pipeline-summary-text"><span className="pipeline-summary-subtitle">Maximum lag: </span> 10 sec.</div>
            <div className="pipeline-summary-text"><span className="pipeline-summary-subtitle">K8s Events</span></div>
            <div className="pipeline-summary-text"><span className="pipeline-summary-subtitle">Pipeline Specs</span></div>
          </Box>
        </Box>
      </Box>
    </Box>
  )
}
