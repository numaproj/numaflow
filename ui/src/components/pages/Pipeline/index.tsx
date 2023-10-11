import React, { useContext, useMemo } from "react";
import { useParams } from "react-router-dom";
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
import { PipelineStatus } from "./partials/PipelineStatus";
import { PipelineSummaryStatus } from "./partials/PipelineSummaryStatus";
import { PipelineISBStatus } from "./partials/PipelineISBStatus";
import { AppContextProps } from "../../../types/declarations/app";
import { AppContext } from "../../../App";
import { ErrorDisplay } from "../../common/ErrorDisplay";
import { UNKNOWN } from "../../../utils";

import "./style.css";

export function Pipeline() {
  // TODO needs to be able to be given namespaceId from parent for NS only install
  const { namespaceId, pipelineId } = useParams();
  const { addError } = useContext<AppContextProps>(AppContext);
  const {
    data,
    loading: summaryLoading,
    error,
    refresh,
  } = usePipelineSummaryFetch({ namespaceId, pipelineId, addError });

  const summarySections: SummarySection[] = useMemo(() => {
    if (summaryLoading) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: <CircularProgress key="pipeline-summary-spinner" />,
        },
      ];
    }
    if (error) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: (
            <ErrorDisplay
              key="pipeline-summary-error"
              title="Error loading pipeline summary"
              message={error}
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
    return [
      // pipeline collection
      {
        type: SummarySectionType.COLLECTION,
        collectionSections: [
          {
            type: SummarySectionType.CUSTOM,
            customComponent: (
              <PipelineStatus
                status={pipelineStatus}
                healthStatus={pipelineData?.status}
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
              <PipelineISBStatus
                isbData={isbData}
                key={"pipeline-isb-status"}
              />
            ),
          },
        ],
      },
    ];
  }, [summaryLoading, error, data, pipelineId, refresh]);

  const { pipeline, vertices, edges, pipelineErr, buffersErr, loading } =
    usePipelineViewFetch(namespaceId, pipelineId, addError);

  const content = useMemo(() => {
    if (pipelineErr || buffersErr) {
      return (
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            justifyContent: "space-between",
            margin: "0 1rem",
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
        >
          <CircularProgress />
        </Box>
      );
    }
    return (
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
    );
  }, [
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
