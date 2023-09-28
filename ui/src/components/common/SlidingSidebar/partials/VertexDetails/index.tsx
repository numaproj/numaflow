import React, { useCallback, useEffect, useMemo, useState } from "react";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import Box from "@mui/material/Box";
import { SpecEditor } from "./partials/SpecEditor";
import { ProcessingRates } from "./partials/ProcessingRates";
import { K8sEvents } from "../K8sEvents";

import "./style.css";

const PODS_VIEW_TAB_INDEX = 0;
const SPEC_TAB_INDEX = 1;
const PROCESSING_RATES_TAB_INDEX = 2;
const K8S_EVENTS_TAB_INDEX = 3;

export enum VertexType {
  SOURCE,
  SINK,
  PROCESSOR,
}

export interface VertexDetailsProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  vertexSpecs: any[];
}

export function VertexDetails({
  namespaceId,
  pipelineId,
  vertexId,
  vertexSpecs,
}: VertexDetailsProps) {
  const [vertexSpec, setVertexSpec] = useState<any>();
  const [vertexType, setVertexType] = useState<VertexType | undefined>();
  const [tabValue, setTabValue] = useState(PODS_VIEW_TAB_INDEX);

  // Find the vertex spec by id
  useEffect(() => {
    const foundSpec = vertexSpecs.find((spec) => spec.name === vertexId);
    if (foundSpec.source) {
      setVertexType(VertexType.SOURCE);
    } else if (foundSpec.udf) {
      setVertexType(VertexType.PROCESSOR);
    } else if (foundSpec.sink) {
      setVertexType(VertexType.SINK);
    }
    setVertexSpec(foundSpec);
  }, [vertexId, vertexSpecs]);

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
    };
    const textClass = "vertex-details-header-text";
    switch (vertexType) {
      case VertexType.SOURCE:
        return (
          <Box sx={headerContainerStyle}>
            <span className={textClass}>Input Vertex</span>
          </Box>
        );
      case VertexType.PROCESSOR:
        return (
          <Box sx={headerContainerStyle}>
            <span className={textClass}>Processor Vertex</span>
          </Box>
        );
      case VertexType.SINK:
        return (
          <Box sx={headerContainerStyle}>
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
        </Tabs>
      </Box>
      <div
        className="vertex-details-tab-panel"
        role="tabpanel"
        hidden={tabValue !== PODS_VIEW_TAB_INDEX}
      >
        {tabValue === PODS_VIEW_TAB_INDEX && <div>TODO PODS VIEW</div>}
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
            namespaceId={namespaceId}
            vertexId={vertexId}
            pipelineId={pipelineId}
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
    </Box>
  );
}
