import React, { useContext, useEffect, useState } from "react";
import Box from "@mui/material/Box";
import Tab from "@mui/material/Tab";
import Tabs from "@mui/material/Tabs";
import { Metrics } from "./partials/Metrics";
import { PodLogs } from "./partials/PodLogs";
import { Errors } from "./partials/Errors";
import { useErrorsFetch } from "../../../../../../../../../../../utils/fetchWrappers/errorsFetch";
import { PodDetailProps } from "../../../../../../../../../../../types/declarations/pods";
import { AppContextProps } from "../../../../../../../../../../../types/declarations/app";
import { AppContext } from "../../../../../../../../../../../App";
import {
  VertexDetailsContext,
  VertexDetailsContextProps,
} from "../../../../../../../../../../common/SlidingSidebar/partials/VertexDetails";

import "./style.css";

const headerSx = {
  height: "2rem",
  marginBottom: "1rem",
  fontWeight: 600,
  fontSize: "2rem",
};

const LOGS_TAB_INDEX = 0;
const ERRORS_TAB_INDEX = 1;
const METRICS_TAB_INDEX = 2;

export function PodDetail({
  namespaceId,
  pipelineId,
  type,
  containerName,
  pod,
  vertexId,
}: PodDetailProps) {
  if (!pod) return null;
  const [replica, setReplica] = useState<number>(0);
  const [errorsCount, setErrorsCount] = useState<number>(0);

  useEffect(() => {
    try {
      const podNameSplit = pod?.name?.split("-");
      const replicaIndex = podNameSplit?.length - 2;
      const replicaNumber =
        replicaIndex >= 0 ? Number(podNameSplit[replicaIndex]) : NaN;
      setReplica(isNaN(replicaNumber) ? 0 : replicaNumber);
    } catch (error) {
      setReplica(0);
    }
  }, [pod]);

  const { addError, disableMetricsCharts } =
    useContext<AppContextProps>(AppContext);

  const { podsViewTab, setPodsViewTab } =
    useContext<VertexDetailsContextProps>(VertexDetailsContext);
  const handleTabChange = (_: any, newValue: number) => {
    setPodsViewTab(newValue);
  };

  const { data: errorsDetailsData } = useErrorsFetch({
    namespaceId,
    pipelineId,
    vertexId,
    replica,
    type,
    addError,
  });

  useEffect(() => {
    const count = errorsDetailsData?.length || 0;
    setErrorsCount(count);
  }, [errorsDetailsData]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        width: "100%",
        height: "100%",
      }}
    >
      <Tabs
        value={podsViewTab}
        onChange={handleTabChange}
        aria-label="Pods Details Tabs"
      >
        <Tab
          className={
            podsViewTab === LOGS_TAB_INDEX
              ? "vertex-details-tab-selected"
              : "vertex-details-tab"
          }
          label="Logs"
          data-testid="logs-tab"
        />
        <Tab
          className={
            podsViewTab === ERRORS_TAB_INDEX
              ? "vertex-details-tab-selected"
              : "vertex-details-tab"
          }
          label={
            <Box className={"errors-tab-title"}>
              <Box>Errors</Box>
              {errorsCount > 0 && (
                <Box className={"errors-tab-title-count"}>{errorsCount}</Box>
              )}
            </Box>
          }
          data-testid="errors-tab"
        />
        {!disableMetricsCharts && (
          <Tab
            className={
              podsViewTab === METRICS_TAB_INDEX
                ? "vertex-details-tab-selected"
                : "vertex-details-tab"
            }
            label="Metrics"
            data-testid="metrics-tab"
          />
        )}
      </Tabs>
      <div
        className="vertex-details-tab-panel"
        role="tabpanel"
        hidden={podsViewTab !== LOGS_TAB_INDEX}
      >
        {podsViewTab === LOGS_TAB_INDEX && (
          <Box
            sx={{
              p: "1.6rem",
              height: "calc(100% - 3.2rem)",
            }}
          >
            <Box sx={headerSx}>Container Logs</Box>
            <Box sx={{ height: "calc(100% - 3rem)" }}>
              <PodLogs
                namespaceId={namespaceId}
                podName={pod.name}
                containerName={containerName}
                type={type}
              />
            </Box>
          </Box>
        )}
      </div>
      <div
        className="vertex-details-tab-panel"
        role="tabpanel"
        hidden={podsViewTab !== ERRORS_TAB_INDEX}
      >
        {podsViewTab === ERRORS_TAB_INDEX && (
          <Box
            sx={{
              p: "1.6rem",
              height: "calc(100% - 5rem)",
              overflow: "scroll",
            }}
          >
            <Errors containers={pod?.containers} details={errorsDetailsData} />
          </Box>
        )}
      </div>
      {!disableMetricsCharts && (
        <div
          className="vertex-details-tab-panel"
          role="tabpanel"
          hidden={podsViewTab !== METRICS_TAB_INDEX}
        >
          {podsViewTab === METRICS_TAB_INDEX && (
            <Box
              sx={{
                p: "1.6rem",
                height: "calc(100% - 10rem)",
                overflow: "scroll",
              }}
            >
              <Metrics
                namespaceId={namespaceId}
                pipelineId={pipelineId}
                type={type}
                vertexId={vertexId}
                pod={pod}
              />
            </Box>
          )}
        </div>
      )}
    </Box>
  );
}
