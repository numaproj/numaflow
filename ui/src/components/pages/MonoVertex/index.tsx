import React, { useCallback, useContext, useMemo, createContext } from "react";
import { useLocation } from "react-router-dom";
import CircularProgress from "@mui/material/CircularProgress";
import Box from "@mui/material/Box";
import Graph from "../Pipeline/partials/Graph";
import {
  SummaryPageLayout,
  SummarySection,
  SummarySectionType,
} from "../../common/SummaryPageLayout";
import { useMonoVertexSummaryFetch } from "../../../utils/fetchWrappers/monoVertexFetch";
import { useMonoVertexViewFetch } from "../../../utils/fetcherHooks/monoVertexViewFetch";
import { useMonoVertexHealthFetch } from "../../../utils/fetchWrappers/monoVertexHealthFetch";
import { MonoVertexStatus } from "./partials/MonoVertexStatus";
import { MonoVertexSummaryStatus } from "./partials/MonoVertexSummaryStatus";
import { AppContextProps } from "../../../types/declarations/app";
import { AppContext } from "../../../App";
import { ErrorDisplay } from "../../common/ErrorDisplay";
import { GetConsolidatedHealthStatus, UNKNOWN } from "../../../utils";
import { SidebarType } from "../../common/SlidingSidebar";

import "./style.css";

export interface MonoVertexProps {
  namespaceId?: string;
}

export const GeneratorColorContext = createContext<Map<string, string>>(
  new Map()
);

// TODO add health status + processing rate once implemented
export function MonoVertex({ namespaceId: nsIdProp }: MonoVertexProps) {
  const location = useLocation();
  const query = new URLSearchParams(location.search);
  const pipelineId = query.get("pipeline") || "";
  const nsIdParam = query.get("namespace") || "";
  const namespaceId = nsIdProp || nsIdParam;
  const { addError, setSidebarProps } = useContext<AppContextProps>(AppContext);
  const {
    data,
    loading: summaryLoading,
    error,
    refresh: summaryRefresh,
  } = useMonoVertexSummaryFetch({ namespaceId, pipelineId, addError });

  const {
    pipeline,
    vertices,
    pipelineErr,
    loading,
    refresh: graphRefresh,
  } = useMonoVertexViewFetch(namespaceId, pipelineId, addError);

  const {
    data: healthData,
    loading: healthLoading,
    error: healthError,
    refresh: healthRefresh,
  } = useMonoVertexHealthFetch({
    namespaceId,
    monoVertexId: pipelineId,
    addError,
  });

  const refresh = useCallback(() => {
    graphRefresh();
    summaryRefresh();
    healthRefresh();
  }, [graphRefresh, summaryRefresh, healthRefresh]);

  const handleK8sEventsClick = useCallback(() => {
    if (!namespaceId || !pipelineId || !setSidebarProps) {
      return;
    }
    const vertexMap = new Map<string, string[]>();
    setSidebarProps({
      type: SidebarType.NAMESPACE_K8s,
      k8sEventsProps: {
        namespaceId,
        pipelineId: `${pipelineId} (MonoVertex)`,
        headerText: "Pipeline K8s Events",
        vertexFilterOptions: vertexMap,
      },
    });
  }, [namespaceId, pipelineId, setSidebarProps, vertices]);

  const getMonoVertexHealth = useCallback(
    (monoVertexStatus: string) => {
      if (healthData) {
        const { resourceHealthStatus, dataHealthStatus } = healthData;
        return GetConsolidatedHealthStatus(
          monoVertexStatus,
          resourceHealthStatus,
          dataHealthStatus
        );
      }
      return UNKNOWN;
    },
    [healthData]
  );

  const summarySections: SummarySection[] = useMemo(() => {
    if (summaryLoading || healthLoading) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: (
            <CircularProgress
              key="pipeline-summary-spinner"
              data-testid={"pipeline-summary-loading"}
            />
          ),
        },
      ];
    }
    if (error || healthError) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: (
            <ErrorDisplay
              key="pipeline-summary-error"
              title="Error loading pipeline summary"
              message={error || healthError || ""}
            />
          ),
        },
      ];
    }
    if (!data) {
      return [];
    }
    const pipelineData = data?.monoVertexData;
    const pipelineStatus = pipelineData?.monoVertex?.status?.phase || UNKNOWN;
    const monoVertexHealthStatus = getMonoVertexHealth(pipelineStatus);
    return [
      // pipeline collection
      {
        type: SummarySectionType.CUSTOM,
        customComponent: (
          <MonoVertexStatus 
            status={pipelineStatus}
            healthStatus={monoVertexHealthStatus}
            healthData={healthData}
            key={"monoVertex-status"} 
          />
        ),
      },
      {
        type: SummarySectionType.CUSTOM,
        customComponent: (
          <MonoVertexSummaryStatus
            pipelineId={pipelineId}
            pipeline={pipelineData?.monoVertex}
            refresh={refresh}
            key={"pipeline-summary-status"}
          />
        ),
      },
      {
        type: SummarySectionType.CUSTOM,
        customComponent: (
          <div
            className="namespace-k8s-events"
            onClick={handleK8sEventsClick}
            data-testid={"pipeline-k8s-events"}
          >
            K8s Events
          </div>
        ),
      },
    ];
  }, [summaryLoading, error, data, pipelineId, refresh, healthData, healthLoading, healthError]);

  const content = useMemo(() => {
    if (pipelineErr) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            justifyContent: "space-between",
            margin: "0 1.6rem",
            height: "100%",
          }}
        >
          <Box
            sx={{
              display: "flex",
              flexGrow: 1,
              justifyContent: "center",
            }}
          >
            <ErrorDisplay
              title="Error loading pipeline"
              message={pipelineErr || ""}
            />
          </Box>
        </Box>
      );
    }
    if (loading) {
      return (
        <Box
          sx={{
            display: "flex",
            height: "100%",
            width: "100%",
            justifyContent: "center",
            alignItems: "center",
          }}
          data-testid={"pipeline-loading"}
        >
          <CircularProgress />
        </Box>
      );
    }
    return (
      <GeneratorColorContext.Provider value={new Map()}>
        <Graph
          data={{
            edges: [],
            vertices: vertices,
            pipeline: pipeline,
          }}
          namespaceId={namespaceId}
          pipelineId={pipelineId}
          type={"monoVertex"}
          refresh={refresh}
        />
      </GeneratorColorContext.Provider>
    );
  }, [
    pipelineErr,
    loading,
    vertices,
    pipeline,
    namespaceId,
    pipelineId,
    refresh,
  ]);

  return (
    <SummaryPageLayout
      excludeContentMargin={true}
      contentPadding={false}
      contentHideOverflow
      collapsable
      summarySections={summarySections}
      contentComponent={
        <Box data-testid={"pipeline"} sx={{ height: "100%" }}>
          {content}
        </Box>
      }
    />
  );
}
