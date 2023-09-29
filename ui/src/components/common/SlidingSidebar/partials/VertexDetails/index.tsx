import React, { useCallback, useEffect, useMemo, useState } from "react";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import Box from "@mui/material/Box";
import { SpecEditor } from "./partials/SpecEditor";
import { ProcessingRates } from "./partials/ProcessingRates";
import { K8sEvents } from "../K8sEvents";
import { Buffers } from "./partials/Buffers";
import { Pods } from "../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods";
import sourceIcon from "../../../../../images/source_vertex.png";
import sinkIcon from "../../../../../images/sink_vertex.png";
import mapIcon from "../../../../../images/map_vertex.png";
import reducIcon from "../../../../../images/reduce_vertex.png";

import "./style.css";

const PODS_VIEW_TAB_INDEX = 0;
const SPEC_TAB_INDEX = 1;
const PROCESSING_RATES_TAB_INDEX = 2;
const K8S_EVENTS_TAB_INDEX = 3;
const BUFFERS_TAB_INDEX = 4;

export enum VertexType {
  SOURCE,
  SINK,
  MAP,
  REDUCE,
}

export interface VertexDetailsProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  vertexSpecs: any;
  vertexMetrics: any;
  buffers: any[];
  type: string;
}

export function VertexDetails({
  namespaceId,
  pipelineId,
  vertexId,
  vertexSpecs,
  vertexMetrics,
  buffers,
  type,
}: VertexDetailsProps) {
  const [vertexSpec, setVertexSpec] = useState<any>();
  const [vertexType, setVertexType] = useState<VertexType | undefined>();
  const [tabValue, setTabValue] = useState(PODS_VIEW_TAB_INDEX);

  // Find the vertex spec by id
  useEffect(() => {
    if (type === "source") {
      setVertexType(VertexType.SOURCE);
    } else if (type === "udf" && vertexSpecs?.udf?.groupBy) {
      setVertexType(VertexType.REDUCE);
    } else if (type === "udf") {
      setVertexType(VertexType.MAP);
    } else if (type === "sink") {
      setVertexType(VertexType.SINK);
    }
    setVertexSpec(vertexSpecs);
  }, [vertexSpecs, type]);

  const handleTabChange = useCallback(
    (event: React.SyntheticEvent, newValue: number) => {
      setTabValue(newValue);
    },
    []
  );

  const header = useMemo(() => {
    const headerContainerStyle = {
      display: "flex",
      flexDirection: "row",
      alignItems: "center",
    };
    const textClass = "vertex-details-header-text";
    switch (vertexType) {
      case VertexType.SOURCE:
        return (
          <Box sx={headerContainerStyle}>
            <img
              src={sourceIcon}
              alt="source vertex"
              className={"vertex-details-header-icon"}
            />
            <span className={textClass}>Input Vertex</span>
          </Box>
        );
      case VertexType.REDUCE:
        return (
          <Box sx={headerContainerStyle}>
            <img
              src={reducIcon}
              alt="reduce vertex"
              className={"vertex-details-header-icon"}
            />
            <span className={textClass}>Processor Vertex</span>
          </Box>
        );
      case VertexType.MAP:
        return (
          <Box sx={headerContainerStyle}>
            <img
              src={mapIcon}
              alt="map vertex"
              className={"vertex-details-header-icon"}
            />
            <span className={textClass}>Processor Vertex</span>
          </Box>
        );
      case VertexType.SINK:
        return (
          <Box sx={headerContainerStyle}>
            <img
              src={sinkIcon}
              alt="sink vertex"
              className={"vertex-details-header-icon"}
            />
            <span className={textClass}>Sink Vertex</span>
          </Box>
        );
      default:
        return (
          <Box sx={headerContainerStyle}>
            <span className={textClass}>Vertex</span>
          </Box>
        );
    }
  }, [vertexType]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      {header}
      <Box sx={{ marginTop: "1rem", borderBottom: 1, borderColor: "divider" }}>
        <Tabs
          className="vertex-details-tabs"
          value={tabValue}
          onChange={handleTabChange}
        >
          <Tab
            className={
              tabValue === PODS_VIEW_TAB_INDEX
                ? "vertex-details-tab-selected"
                : "vertex-details-tab"
            }
            label="Pods View"
          />
          <Tab
            className={
              tabValue === SPEC_TAB_INDEX
                ? "vertex-details-tab-selected"
                : "vertex-details-tab"
            }
            label="Spec"
          />
          <Tab
            className={
              tabValue === PROCESSING_RATES_TAB_INDEX
                ? "vertex-details-tab-selected"
                : "vertex-details-tab"
            }
            label="Processing Rates"
          />
          <Tab
            className={
              tabValue === K8S_EVENTS_TAB_INDEX
                ? "vertex-details-tab-selected"
                : "vertex-details-tab"
            }
            label="K8s Events"
          />
          {buffers && (
            <Tab
              className={
                tabValue === BUFFERS_TAB_INDEX
                  ? "vertex-details-tab-selected"
                  : "vertex-details-tab"
              }
              label="Buffers"
            />
          )}
        </Tabs>
      </Box>
      <div
        className="vertex-details-tab-panel"
        role="tabpanel"
        hidden={tabValue !== PODS_VIEW_TAB_INDEX}
      >
        {tabValue === PODS_VIEW_TAB_INDEX && (
          <Pods
            namespaceId={namespaceId}
            pipelineId={pipelineId}
            vertexId={vertexId}
          />
        )}
      </div>
      <div
        className="vertex-details-tab-panel"
        role="tabpanel"
        hidden={tabValue !== SPEC_TAB_INDEX}
      >
        {tabValue === SPEC_TAB_INDEX && (
          <Box sx={{ height: "100%" }}>
            <SpecEditor vertexId={vertexId} vertexSpec={vertexSpec} />
          </Box>
        )}
      </div>
      <div
        className="vertex-details-tab-panel"
        role="tabpanel"
        hidden={tabValue !== PROCESSING_RATES_TAB_INDEX}
      >
        {tabValue === PROCESSING_RATES_TAB_INDEX && (
          <ProcessingRates
            vertexId={vertexId}
            pipelineId={pipelineId}
            vertexMetrics={vertexMetrics}
          />
        )}
      </div>
      <div
        className="vertex-details-tab-panel"
        role="tabpanel"
        hidden={tabValue !== K8S_EVENTS_TAB_INDEX}
      >
        {tabValue === K8S_EVENTS_TAB_INDEX && (
          <K8sEvents namespaceId={namespaceId} excludeHeader square />
        )}
      </div>
      {buffers && (
        <div
          className="vertex-details-tab-panel"
          role="tabpanel"
          hidden={tabValue !== BUFFERS_TAB_INDEX}
        >
          {tabValue === BUFFERS_TAB_INDEX && <Buffers buffers={buffers} />}
        </div>
      )}
    </Box>
  );
}
