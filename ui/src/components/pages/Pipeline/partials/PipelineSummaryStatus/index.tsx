import React, { useCallback, useContext } from "react";
import Box from "@mui/material/Box";
import { useParams } from "react-router-dom";
import { SidebarType } from "../../../../common/SlidingSidebar";
import { AppContextProps } from "../../../../../types/declarations/app";
import { AppContext } from "../../../../../App";

import "./style.css";

export function PipelineSummaryStatus({ pipeline }) {
  const { namespaceId, pipelineId } = useParams();
  const { setSidebarProps } = useContext<AppContextProps>(AppContext);
  const handleK8sEventsClick = useCallback(() => {
    if (!namespaceId || !setSidebarProps) {
      return;
    }
    setSidebarProps({
      type: SidebarType.NAMESPACE_K8s,
      k8sEventsProps: { namespaceId },
    });
  }, [namespaceId, setSidebarProps]);

  const handleSpecClick = useCallback(() => {
    if (!namespaceId || !setSidebarProps) {
      return;
    }
    setSidebarProps({
      type: SidebarType.PIPELINE_SPEC,
      pipelineSpecProps: { spec: pipeline.spec },
    });
  }, [namespaceId, setSidebarProps, pipeline]);
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
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              marginRight: "1rem",
            }}
          >
            <div className="pipeline-summary-text">
              <span className="pipeline-summary-subtitle">Created On: </span>
            </div>
            <div className="pipeline-summary-text">
              <span className="pipeline-summary-subtitle">
                Last Updated On:{" "}
              </span>
            </div>
            <div className="pipeline-summary-text">
              <span className="pipeline-summary-subtitle">Maximum lag: </span>
            </div>
            {/*<div className="pipeline-summary-text">*/}
            {/*  <span className="pipeline-summary-subtitle">Last Refresh: </span>*/}
            {/*  2023-12-07T02:02:00Z*/}
            {/*</div>*/}
          </Box>
          <Box
            sx={{
              display: "flex",
              flexDirection: "column",
              marginRight: "4rem",
            }}
          >
            <div className="pipeline-summary-text">
              <span>{pipeline?.metadata?.creationTimestamp}</span>
            </div>
            <div className="pipeline-summary-text">
              <span>{pipeline?.status?.lastUpdated}</span>
            </div>
            <div className="pipeline-summary-text">
              <span>10 sec.</span>
            </div>
            {/*<div className="pipeline-summary-text">*/}
            {/*  2023-12-07T02:02:00Z*/}
            {/*</div>*/}
          </Box>
          <Box sx={{ display: "flex", flexDirection: "column" }}>
            <div className="pipeline-summary-text">
              <span className="pipeline-summary-subtitle">
                <div
                  className="pipeline-onclick-events"
                  onClick={handleK8sEventsClick}
                >
                  K8s Events
                </div>
              </span>
            </div>
            <div className="pipeline-summary-text">
              <span className="pipeline-summary-subtitle">
                <div
                  className="pipeline-onclick-events"
                  onClick={handleSpecClick}
                >
                  Spec
                </div>
              </span>
            </div>
          </Box>
        </Box>
      </Box>
    </Box>
  );
}
