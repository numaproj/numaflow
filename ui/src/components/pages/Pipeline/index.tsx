import React, {useEffect, useMemo} from "react";
import { useParams } from "react-router-dom";
import CircularProgress from "@mui/material/CircularProgress";
import { usePipelineViewFetch } from "../../../utils/fetcherHooks/pipelineViewFetch";
import Graph from "./partials/Graph";
import { notifyError } from "../../../utils/error";

import "./style.css";
import {SummaryPageLayout, SummarySection, SummarySectionType} from "../../common/SummaryPageLayout";
import Box from "@mui/material/Box";
import {usePipelineSummaryFetch} from "../../../utils/fetchWrappers/pipelineFetch";
import {PipelineStatus} from "./partials/PipelineStatus";
import {PipelineSummaryStatus} from "./partials/PipelineSummaryStatus";
import {PipelineISBStatus} from "./partials/PipelineISBStatus";

export function Pipeline() {
  // TODO needs to be able to be given namespaceId from parent for NS only install
  const { namespaceId, pipelineId } = useParams();
  const { data, loading: loadingNew, error } = usePipelineSummaryFetch({namespaceId,  pipelineId})

  const summarySections: SummarySection[] = useMemo(() => {
    if (!data) {
      return [];
    }
    return [
      // Pipelines collection
      {
        type: SummarySectionType.COLLECTION,
        collectionSections: [
          {
            type: SummarySectionType.CUSTOM,
            customComponent: <PipelineStatus status={data?.data?.pipeline?.status?.phase} healthStatus={data?.data?.status}/>,
          },
          {
            type: SummarySectionType.CUSTOM,
            customComponent: <PipelineSummaryStatus pipeline={data?.data?.pipeline} />,
          },
          {
            type: SummarySectionType.CUSTOM,
            customComponent: <PipelineISBStatus />,
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

  if (pipelineErr || buffersErr) {
    return <div>Error</div>;
  }

  return (
    <Box>
      <SummaryPageLayout
        summarySections={summarySections}
        contentComponent={
          (
          <div
            data-testid={"pipeline"}
            style={{ overflow: "scroll !important", height: "650px" }}
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
            {loading && <CircularProgress size={60} sx={{ mx: "47%", my: "15%" }} />}
          </div>
          )
        }
      />
    </Box>
  );
}
