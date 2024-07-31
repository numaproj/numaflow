import React, { useCallback, useEffect, useMemo, useState } from "react";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import Box from "@mui/material/Box";
import { VertexUpdate } from "./partials/VertexUpdate";
import { ProcessingRates } from "./partials/ProcessingRates";
import { K8sEvents } from "../K8sEvents";
import { Buffers } from "./partials/Buffers";
import { Pods } from "../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods";
import { SpecEditorModalProps } from "../..";
import { CloseModal } from "../CloseModal";
import sourceIcon from "../../../../../images/source.png";
import sinkIcon from "../../../../../images/sink.png";
import mapIcon from "../../../../../images/map.png";
import reducIcon from "../../../../../images/reduce.png";

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
  setModalOnClose?: (props: SpecEditorModalProps | undefined) => void;
  refresh: () => void;
}

export function VertexDetails({
  namespaceId,
  pipelineId,
  vertexId,
  vertexSpecs,
  vertexMetrics,
  buffers,
  type,
  setModalOnClose,
  refresh,
}: VertexDetailsProps) {
  const [vertexSpec, setVertexSpec] = useState<any>();
  const [vertexType, setVertexType] = useState<VertexType | undefined>();
  const [tabValue, setTabValue] = useState(PODS_VIEW_TAB_INDEX);
  const [updateModalOnClose, setUpdateModalOnClose] = useState<
    SpecEditorModalProps | undefined
  >();
  const [updateModalOpen, setUpdateModalOpen] = useState(false);
  const [targetTab, setTargetTab] = useState<number | undefined>();

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

  const handleTabChange = useCallback(
    (event: React.SyntheticEvent, newValue: number) => {
      if (tabValue === SPEC_TAB_INDEX && updateModalOnClose) {
        setTargetTab(newValue);
        setUpdateModalOpen(true);
      } else {
        setTabValue(newValue);
      }
    },
    [tabValue, updateModalOnClose]
  );

  const handleUpdateModalConfirm = useCallback(() => {
    // Close modal
    setUpdateModalOpen(false);
    // Clear modal on close
    setUpdateModalOnClose(undefined);
    setModalOnClose && setModalOnClose(undefined);
    // Change to tab requested
    setTabValue(targetTab || PODS_VIEW_TAB_INDEX);
  }, [targetTab]);

  const handleUpdateModalCancel = useCallback(() => {
    setUpdateModalOpen(false);
  }, []);

  const handleUpdateModalClose = useCallback(
    (props: SpecEditorModalProps | undefined) => {
      setUpdateModalOnClose(props);
      setModalOnClose && setModalOnClose(props);
    },
    [setModalOnClose]
  );

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
      }}
    >
      {header}
      <Box
        sx={{ marginTop: "1.6rem", borderBottom: 1, borderColor: "divider" }}
      >
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
            data-testid="pods-tab"
          />
          <Tab
            className={
              tabValue === SPEC_TAB_INDEX
                ? "vertex-details-tab-selected"
                : "vertex-details-tab"
            }
            label="Spec"
            data-testid="spec-tab"
          />
          <Tab
            className={
              tabValue === PROCESSING_RATES_TAB_INDEX
                ? "vertex-details-tab-selected"
                : "vertex-details-tab"
            }
            label="Processing Rates"
            data-testid="pr-tab"
          />
          <Tab
            className={
              tabValue === K8S_EVENTS_TAB_INDEX
                ? "vertex-details-tab-selected"
                : "vertex-details-tab"
            }
            label="K8s Events"
            data-testid="events-tab"
          />
          {buffers && (
            <Tab
              className={
                tabValue === BUFFERS_TAB_INDEX
                  ? "vertex-details-tab-selected"
                  : "vertex-details-tab"
              }
              label="Buffers"
              data-testid="buffers-tab"
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
            <VertexUpdate
              namespaceId={namespaceId}
              pipelineId={pipelineId}
              vertexId={vertexId}
              vertexSpec={vertexSpec}
              setModalOnClose={handleUpdateModalClose}
              refresh={refresh}
            />
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
          <K8sEvents
            namespaceId={namespaceId}
            pipelineId={pipelineId}
            vertexId={vertexId}
            excludeHeader
            square
          />
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
      {updateModalOnClose && updateModalOpen && (
        <CloseModal
          {...updateModalOnClose}
          onConfirm={handleUpdateModalConfirm}
          onCancel={handleUpdateModalCancel}
        />
      )}
    </Box>
  );
}
