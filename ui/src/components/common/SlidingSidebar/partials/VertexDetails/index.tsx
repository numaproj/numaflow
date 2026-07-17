import React, {
  createContext,
  Dispatch,
  SetStateAction,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import Tabs from "@mui/material/Tabs";
import Tab from "@mui/material/Tab";
import Box from "@mui/material/Box";
import { VertexUpdate } from "./partials/VertexUpdate";
import { ProcessingRates } from "./partials/ProcessingRates";
import { Errors } from "./partials/Errors";
import { K8sEvents } from "../K8sEvents";
import { Buffers } from "./partials/Buffers";
import { Pods } from "../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods";
import { Metrics } from "../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodDetails/partials/Metrics";
import { SpecEditorModalProps } from "../..";
import { CloseModal } from "../CloseModal";
import { AppContext } from "../../../../../App";
import { useErrorsFetch } from "../../../../../utils/fetchWrappers/errorsFetch";
import { AppContextProps } from "../../../../../types/declarations/app";
import {
  ContainerError,
  Pod,
  ReplicaErrors,
} from "../../../../../types/declarations/pods";
import { BufferInfo } from "../../../../../types/declarations/pipeline";
import sourceIcon from "../../../../../images/source.png";
import sinkIcon from "../../../../../images/sink.png";
import mapIcon from "../../../../../images/map.png";
import reduceIcon from "../../../../../images/reduce.png";
import monoVertexIcon from "../../../../../images/monoVertex.svg";

import "./style.css";

const PODS_VIEW_TAB_INDEX = 0;
const METRICS_TAB_INDEX = 1;
const SPEC_TAB_INDEX = 2;
const PROCESSING_RATES_TAB_INDEX = 3;
const K8S_EVENTS_TAB_INDEX = 4;
const ERRORS_TAB_INDEX = 5;
const BUFFERS_TAB_INDEX = 6;

export enum VertexType {
  SOURCE,
  SINK,
  MAP,
  REDUCE,
  MONOVERTEX,
}

export interface VertexDetailsProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  vertexSpecs: any;
  vertexMetrics: any;
  buffers?: BufferInfo[] | null;
  type: string;
  setModalOnClose?: (props: SpecEditorModalProps | undefined) => void;
  refresh: () => void;
}

export interface VertexDetailsContextProps {
  openMetrics: (options: OpenMetricsOptions) => void;
  expanded: Set<string>;
  setExpanded: Dispatch<SetStateAction<Set<string>>>;
  presets: any;
  setPresets: Dispatch<SetStateAction<any>>;
}

export interface OpenMetricsOptions {
  panelId: string;
  presets?: any;
  pod?: Pod;
}

export const VertexDetailsContext = createContext<VertexDetailsContextProps>({
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  openMetrics: () => {},
  expanded: new Set(),
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  setExpanded: () => {},
  presets: undefined,
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  setPresets: () => {},
});

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
  const { addError, disableMetricsCharts } =
    useContext<AppContextProps>(AppContext);
  const [errorsCount, setErrorsCount] = useState<number>(0);
  const [vertexSpec, setVertexSpec] = useState<any>();
  const [vertexType, setVertexType] = useState<VertexType | undefined>();
  const [tabValue, setTabValue] = useState<number>(PODS_VIEW_TAB_INDEX);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [metricsPod, setMetricsPod] = useState<Pod | undefined>();
  const [updateModalOnClose, setUpdateModalOnClose] = useState<
    SpecEditorModalProps | undefined
  >();
  const [presets, setPresets] = useState<any>(undefined);
  const [updateModalOpen, setUpdateModalOpen] = useState(false);
  const [targetTab, setTargetTab] = useState<number | undefined>();
  const [constructedDetails, setConstructedDetails] = useState<
    (ContainerError & { pod: string })[]
  >([]);

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
    } else if (type === "monoVertex") {
      setVertexType(VertexType.MONOVERTEX);
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
              src={reduceIcon}
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
      case VertexType.MONOVERTEX:
        return (
          <Box sx={headerContainerStyle}>
            <img
              src={monoVertexIcon}
              alt="mono vertex"
              className={"vertex-details-header-icon"}
            />
            <span className={textClass}>Mono Vertex</span>
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
        if (newValue === METRICS_TAB_INDEX) {
          setMetricsPod(undefined);
          setPresets(undefined);
        }
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
    if (targetTab === METRICS_TAB_INDEX) {
      setMetricsPod(undefined);
      setPresets(undefined);
    }
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

  const { data: errorsDetailsData } = useErrorsFetch({
    namespaceId,
    pipelineId,
    vertexId,
    type,
    addError,
  });

  // flattens the error data into a single array of container errors
  const constructDetails = useCallback((details?: ReplicaErrors[]) => {
    const newDetails: (ContainerError & { pod: string })[] = [];
    if (details) {
      details?.forEach((d) => {
        d.containerErrors.forEach((c) => {
          newDetails.push({
            pod: d.replica,
            container: c.container,
            timestamp: c.timestamp,
            code: c.code,
            message: c.message,
            details: c.details,
          });
        });
      });
    }
    setConstructedDetails(newDetails);
  }, []);

  useEffect(() => {
    if (errorsDetailsData) constructDetails(errorsDetailsData);
  }, [errorsDetailsData, constructDetails]);

  const filterErrorsWithinLast24Hours = useCallback(
    (containerErrorsData: (ContainerError & { pod: string })[]) => {
      const currentTime = new Date().getTime();
      const twentyFourHoursAgo = currentTime - 24 * 60 * 60 * 1000; // 24 hours in milliseconds
      return containerErrorsData.filter((errorData) => {
        return (
          errorData.timestamp &&
          new Date(errorData.timestamp).getTime() >= twentyFourHoursAgo
        );
      });
    },
    []
  );

  useEffect(() => {
    const filteredDetailsData =
      filterErrorsWithinLast24Hours(constructedDetails);
    setErrorsCount(filteredDetailsData.length);
  }, [constructedDetails, filterErrorsWithinLast24Hours]);

  const openMetrics = useCallback(
    ({ panelId, presets: nextPresets, pod }: OpenMetricsOptions) => {
      if (disableMetricsCharts) return;

      setMetricsPod(pod);
      setPresets(nextPresets);
      setExpanded((previousExpanded) =>
        new Set(previousExpanded).add(panelId)
      );
      setTabValue(METRICS_TAB_INDEX);
    },
    [disableMetricsCharts]
  );

  useEffect(() => {
    if (disableMetricsCharts && tabValue === METRICS_TAB_INDEX) {
      setMetricsPod(undefined);
      setPresets(undefined);
      setTabValue(PODS_VIEW_TAB_INDEX);
    }
  }, [disableMetricsCharts, tabValue]);

  const showBuffersTab = !!buffers || type === "source";

  return (
    <VertexDetailsContext.Provider
      value={{
        openMetrics,
        expanded,
        setExpanded,
        presets,
        setPresets,
      }}
    >
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
              value={PODS_VIEW_TAB_INDEX}
              label="Pods View"
              data-testid="pods-tab"
            />
            {!disableMetricsCharts && (
              <Tab
                className={
                  tabValue === METRICS_TAB_INDEX
                    ? "vertex-details-tab-selected"
                    : "vertex-details-tab"
                }
                value={METRICS_TAB_INDEX}
                label="Metrics"
                data-testid="metrics-tab"
              />
            )}
            <Tab
              className={
                tabValue === SPEC_TAB_INDEX
                  ? "vertex-details-tab-selected"
                  : "vertex-details-tab"
              }
              value={SPEC_TAB_INDEX}
              label="Spec"
              data-testid="spec-tab"
            />
            <Tab
              className={
                tabValue === PROCESSING_RATES_TAB_INDEX
                  ? "vertex-details-tab-selected"
                  : "vertex-details-tab"
              }
              value={PROCESSING_RATES_TAB_INDEX}
              label="Processing Rates"
              data-testid="pr-tab"
            />
            <Tab
              className={
                tabValue === K8S_EVENTS_TAB_INDEX
                  ? "vertex-details-tab-selected"
                  : "vertex-details-tab"
              }
              value={K8S_EVENTS_TAB_INDEX}
              label="K8s Events"
              data-testid="events-tab"
            />
            <Tab
              className={
                tabValue === ERRORS_TAB_INDEX
                  ? "vertex-details-tab-selected"
                  : "vertex-details-tab"
              }
              label={
                <Box className={"vertex-details-errors-tab-title"}>
                  <Box>Errors</Box>
                  <Box sx={{ color: errorsCount ? "red" : "#6B6C72" }}>
                    {errorsCount}
                  </Box>
                </Box>
              }
              value={ERRORS_TAB_INDEX}
              data-testid="errors-tab"
            />
            {showBuffersTab && (
              <Tab
                className={
                  tabValue === BUFFERS_TAB_INDEX
                    ? "vertex-details-tab-selected"
                    : "vertex-details-tab"
                }
                value={BUFFERS_TAB_INDEX}
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
              type={type}
            />
          )}
        </div>
        {!disableMetricsCharts && (
          <div
            className="vertex-details-tab-panel vertex-details-tab-panel-metrics"
            role="tabpanel"
            hidden={tabValue !== METRICS_TAB_INDEX}
          >
            {tabValue === METRICS_TAB_INDEX && (
              <Metrics
                namespaceId={namespaceId}
                pipelineId={pipelineId}
                type={type}
                vertexId={vertexId}
                pod={metricsPod}
              />
            )}
          </div>
        )}
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
                type={type}
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
              namespaceId={namespaceId}
              pipelineId={pipelineId}
              type={type}
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
              pipelineId={
                type === "monoVertex"
                  ? `${pipelineId} (MonoVertex)`
                  : pipelineId
              }
              vertexId={type === "monoVertex" ? undefined : vertexId}
              excludeHeader
              square
            />
          )}
        </div>
        <div
          className="vertex-details-tab-panel"
          role="tabpanel"
          hidden={tabValue !== ERRORS_TAB_INDEX}
        >
          {tabValue === ERRORS_TAB_INDEX && (
            <Errors details={constructedDetails} square />
          )}
        </div>
        {showBuffersTab && (
          <div
            className="vertex-details-tab-panel"
            role="tabpanel"
            hidden={tabValue !== BUFFERS_TAB_INDEX}
          >
            {tabValue === BUFFERS_TAB_INDEX && (
              <Buffers
                buffers={buffers || []}
                namespaceId={namespaceId}
                pipelineId={pipelineId}
                vertexId={vertexId}
                type={type}
              />
            )}
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
    </VertexDetailsContext.Provider>
  );
}
