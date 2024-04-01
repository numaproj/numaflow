import React, { useCallback, useContext, useMemo, createContext } from "react";
import { useLocation } from "react-router-dom";
import CircularProgress from "@mui/material/CircularProgress";
import Box from "@mui/material/Box";
import { usePipelineViewFetch } from "../../../utils/fetcherHooks/pipelineViewFetch";
import Graph from "./partials/Graph";
import {
  SummaryPageLayout,
  SummarySection,
  SummarySectionType,
} from "../../common/SummaryPageLayout";
import { usePipelineSummaryFetch } from "../../../utils/fetchWrappers/pipelineFetch";
import { usePipelineHealthFetch } from "../../../utils/fetchWrappers/pipelineHealthFetch";
import { PipelineStatus } from "./partials/PipelineStatus";
import { PipelineSummaryStatus } from "./partials/PipelineSummaryStatus";
import { PipelineISBStatus } from "./partials/PipelineISBStatus";
import { PipelineISBSummaryStatus } from "./partials/PipelineISBSummaryStatus";
import { AppContextProps } from "../../../types/declarations/app";
import { AppContext } from "../../../App";
import { ErrorDisplay } from "../../common/ErrorDisplay";
import { GetConsolidatedHealthStatus, UNKNOWN } from "../../../utils";
import { SidebarType } from "../../common/SlidingSidebar";

import "./style.css";

export interface PipelineProps {
  namespaceId?: string;
}

export const GeneratorColorContext = createContext<Map<string, string>>(
  new Map()
);

export function Pipeline({ namespaceId: nsIdProp }: PipelineProps) {
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
  } = usePipelineSummaryFetch({ namespaceId, pipelineId, addError });

  const {
    pipeline,
    vertices,
    edges,
    generatorToColorIdxMap,
    pipelineErr,
    buffersErr,
    loading,
    refresh: graphRefresh,
  } = usePipelineViewFetch(namespaceId, pipelineId, addError);

  const {
    data: healthData,
    loading: healthLoading,
    error: healthError,
    refresh: healthRefresh,
  } = usePipelineHealthFetch({ namespaceId, pipelineId, addError });

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
    if (vertices?.length) {
      vertexMap.set(
        pipelineId,
        vertices.map((v) => v.id)
      );
    }
    setSidebarProps({
      type: SidebarType.NAMESPACE_K8s,
      k8sEventsProps: {
        namespaceId,
        pipelineId,
        headerText: "Pipeline K8s Events",
        vertexFilterOptions: vertexMap,
      },
    });
  }, [namespaceId, pipelineId, setSidebarProps, vertices]);

  const getHealth = useCallback(
    (pipelineStatus: string) => {
      if (healthData) {
        const { resourceHealthStatus, dataHealthStatus } = healthData;
        return GetConsolidatedHealthStatus(
          pipelineStatus,
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
    const pipelineData = data?.pipelineData;
    const isbData = data?.isbData;
    const pipelineStatus = pipelineData?.pipeline?.status?.phase || UNKNOWN;
    const pipelineHealthStatus = getHealth(pipelineStatus);
    return [
      // pipeline collection
      {
        type: SummarySectionType.CUSTOM,
        customComponent: (
          <PipelineStatus
            status={pipelineStatus}
            healthStatus={pipelineHealthStatus}
            healthData={healthData}
            key={"pipeline-status"}
          />
        ),
      },
      {
        type: SummarySectionType.CUSTOM,
        customComponent: (
          <PipelineSummaryStatus
            pipelineId={pipelineId}
            pipeline={pipelineData?.pipeline}
            lag={pipelineData?.lag}
            refresh={refresh}
            key={"pipeline-summary-status"}
          />
        ),
      },
      {
        type: SummarySectionType.CUSTOM,
        customComponent: (
          <PipelineISBStatus isbData={isbData} key={"pipeline-isb-status"} />
        ),
      },
      {
        type: SummarySectionType.CUSTOM,
        customComponent: (
          <PipelineISBSummaryStatus
            isbData={isbData}
            key={"pipeline-isb-status"}
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
  }, [
    summaryLoading,
    error,
    data,
    pipelineId,
    refresh,
    healthData,
    healthLoading,
    healthError,
  ]);

  const content = useMemo(() => {
    if (pipelineErr || buffersErr) {
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
              message={pipelineErr || buffersErr || ""}
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
      <GeneratorColorContext.Provider value={generatorToColorIdxMap}>
        <Graph
          data={{
            edges: edges,
            vertices: vertices,
            pipeline: pipeline,
          }}
          namespaceId={namespaceId}
          pipelineId={pipelineId}
          refresh={refresh}
        />
      </GeneratorColorContext.Provider>
    );
  }, [
    generatorToColorIdxMap,
    pipelineErr,
    buffersErr,
    loading,
    edges,
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
