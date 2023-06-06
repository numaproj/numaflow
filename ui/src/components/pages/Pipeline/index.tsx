import { useEffect } from "react";
import { useParams } from "react-router-dom";

import { usePipelineViewFetch } from "../../../utils/fetchWrappers/pipelineViewFetch";
import Graph from "../../pipeline/graph/Graph";
import { notifyError } from "../../../utils/error";

import "./style.css";

export function Pipeline() {
  const { namespaceId, pipelineId } = useParams();
  const {
    pipeline,
    buffers,
    vertices,
    edges,
    pipelineErr,
    buffersErr,
    podsErr,
    metricsErr,
    watermarkErr,
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
    <div data-testid={"pipeline"} style={{ overflow: "scroll !important" }}>
      {pipeline?.spec &&
        buffers.length > 0 &&
        edges.length > 0 &&
        vertices.length > 0 && (
          <Graph
            data={{
              ...pipeline.spec,
              edges: edges,
              vertices: vertices,
              pipeline: pipeline,
            }}
            namespaceId={namespaceId}
            pipelineId={pipelineId}
          />
        )}
    </div>
  );
}
