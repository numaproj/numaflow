import React, { useCallback, useContext, useEffect, useMemo } from "react";
import { useParams } from "react-router-dom";
import CircularProgress from "@mui/material/CircularProgress";
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
import noError from "../../../images/no-error.svg";

import "./style.css";

export function Pipeline() {
  // TODO needs to be able to be given namespaceId from parent for NS only install
  const { namespaceId, pipelineId } = useParams();
  // TODO loading and error handling
  const {
    data,
    loading: pipelineSummaryLoading,
    error,
  } = usePipelineSummaryFetch({ namespaceId, pipelineId });

  const summarySections: SummarySection[] = useMemo(() => {
    if (!data) {
      return [];
    }
    const pipelineData = data?.pipelineData;
    const isbData = data?.isbData;
    return [
      // pipeline collection
      {
        type: SummarySectionType.COLLECTION,
        collectionSections: [
          {
            type: SummarySectionType.CUSTOM,
            customComponent: (
              <PipelineStatus
                status={pipelineData?.pipeline?.status?.phase}
                healthStatus={pipelineData?.status}
              />
            ),
          },
          {
            type: SummarySectionType.CUSTOM,
            customComponent: (
              <PipelineSummaryStatus pipeline={pipelineData?.pipeline} lag={pipelineData?.lag} />
            ),
          },
          {
            type: SummarySectionType.CUSTOM,
            customComponent: <PipelineISBStatus isbData={isbData} />,
          },
        ],
      },
    ];
  }, [data]);

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
    setSidebarProps({
      type: SidebarType.ERRORS,
      errorsProps: {
        errors: true,
      },
      slide: false,
    });
  }, [setSidebarProps]);

  if (pipelineErr || buffersErr) {
    return (
      <div
        style={{
          display: "flex",
          flexDirection: "row",
          justifyContent: "space-between",
          margin: "0 1rem",
        }}
      >
        <div>Error</div>
        <div onClick={handleError} style={{ cursor: "pointer" }}>
          <img src={noError} width={22} height={24} alt={"error-status"} />
        </div>
      </div>
    );
  }

  return (
    <SummaryPageLayout
      contentPadding={false}
      collapsable
      summarySections={summarySections}
      contentComponent={
        <div
          data-testid={"pipeline"}
          style={{ overflow: "scroll !important", height: "100%" }}
        >
          {!loading && (
            <Graph
              data={{
                edges: edges,
                vertices: vertices,
                pipeline: pipeline,
              }}
              namespaceId={namespaceId}
              pipelineId={pipelineId}
            />
          )}
          {loading && (
            <CircularProgress size={60} sx={{ mx: "47%", my: "15%" }} />
          )}
        </div>
      }
    />
  );
}
