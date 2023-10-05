import React, { useCallback, useContext, useEffect, useMemo } from "react";
import { useParams } from "react-router-dom";
import CircularProgress from "@mui/material/CircularProgress";
import Box from "@mui/material/Box";
import { usePipelineViewFetch } from "../../../utils/fetcherHooks/pipelineViewFetch";
import Graph from "./partials/Graph";
import { notifyError } from "../../../utils/error";
import {
  SummaryPageLayout,
  SummarySection,
  SummarySectionType,
} from "../../common/SummaryPageLayout";
import { usePipelineSummaryFetch } from "../../../utils/fetchWrappers/pipelineFetch";
import { PipelineStatus } from "./partials/PipelineStatus";
import { PipelineSummaryStatus } from "./partials/PipelineSummaryStatus";
import { PipelineISBStatus } from "./partials/PipelineISBStatus";
import { SidebarType } from "../../common/SlidingSidebar";
import { AppContextProps } from "../../../types/declarations/app";
import { AppContext } from "../../../App";
import { ErrorDisplay } from "../../common/ErrorDisplay";
import { UNKNOWN } from "../../../utils";
import noError from "../../../images/no-error.svg";

import "./style.css";

export function Pipeline() {
  // TODO needs to be able to be given namespaceId from parent for NS only install
  const { namespaceId, pipelineId } = useParams();
  const {
    data,
    loading: summaryLoading,
    error,
    refresh,
  } = usePipelineSummaryFetch({ namespaceId, pipelineId });

  const summarySections: SummarySection[] = useMemo(() => {
    if (summaryLoading) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: <CircularProgress />,
        },
      ];
    }
    if (error) {
      return [
        {
          type: SummarySectionType.CUSTOM,
          customComponent: (
            <ErrorDisplay
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

  const {
    pipeline,
    vertices,
    edges,
    pipelineErr,
    buffersErr,
    podsErr,
    metricsErr,
    watermarkErr,
    loading,
  } = usePipelineViewFetch(namespaceId, pipelineId);

  // This useEffect notifies about the errors while querying for the vertices of the pipeline
  useEffect(() => {
    if (pipelineErr) notifyError(pipelineErr);
  }, [pipelineErr]);

  // This useEffect notifies about the errors while querying for the edges of the pipeline
  useEffect(() => {
    if (buffersErr) notifyError(buffersErr);
  }, [buffersErr]);

  // This useEffect notifies about the errors while querying for the pod count of a given vertex
  useEffect(() => {
    if (podsErr) notifyError(podsErr);
  }, [podsErr]);

  // This useEffect notifies about the errors while querying for the metrics of a given vertex
  useEffect(() => {
    if (metricsErr) notifyError(metricsErr);
  }, [metricsErr]);

  // This useEffect notifies about the errors while querying for the watermark of the pipeline
  useEffect(() => {
    if (watermarkErr) notifyError(watermarkErr);
  }, [watermarkErr]);

  const { setSidebarProps } = useContext<AppContextProps>(AppContext);
  const handleError = useCallback(() => {
    setSidebarProps &&
      setSidebarProps({
        type: SidebarType.ERRORS,
        errorsProps: {
          errors: true,
        },
        slide: false,
      });
  }, [setSidebarProps]);

  const content = useMemo(() => {
    if (pipelineErr || buffersErr) {
      let errorString: string;
      if (pipelineErr && pipelineErr.length) {
        errorString = pipelineErr[0].error;
      } else if (buffersErr && buffersErr.length) {
        errorString = buffersErr[0].error;
      } else {
        errorString = "";
      }
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
              message={errorString}
            />
          </Box>
          <Box onClick={handleError} sx={{ cursor: "pointer", flexGrow: 0 }}>
            <img src={noError} width={22} height={24} alt={"error-status"} />
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
    handleError,
    refresh,
  ]);

  return (
    <SummaryPageLayout
      contentPadding={false}
      contentHideOverflow
      collapsable
      summarySections={summarySections}
      contentComponent={
        <Box
          data-testid={"pipeline"}
          sx={{ height: "100%" }}
        >
          {content}
        </Box>
      }
    />
  );
}
