import React, {
  createContext,
  MouseEvent,
  useCallback,
  useEffect,
  useMemo,
  useState,
  useContext,
} from "react";
import ReactFlow, {
  applyEdgeChanges,
  applyNodeChanges,
  Edge,
  EdgeChange,
  Node,
  NodeChange,
  NodeTypes,
  EdgeTypes,
  Position,
  Panel,
  useReactFlow,
  useViewport,
  ReactFlowProvider,
} from "reactflow";
import {
  Accordion,
  AccordionSummary,
  Typography,
  AccordionDetails,
  Tooltip,
} from "@mui/material";
import IconButton from "@mui/material/IconButton";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { Alert, Box, Button, CircularProgress } from "@mui/material";
import { graphlib, layout } from "dagre";
import CustomEdge from "./partials/CustomEdge";
import CustomNode from "./partials/CustomNode";
import { AppContext } from "../../../../../App";
import { AppContextProps } from "../../../../../types/declarations/app";
import { SidebarType } from "../../../../common/SlidingSidebar";
import {
  FlowProps,
  GraphProps,
  HighlightContextProps,
} from "../../../../../types/declarations/graph";
import {
  PAUSED,
  PAUSING,
  RUNNING,
  getAPIResponseError,
  timeAgo,
  getBaseHref,
} from "../../../../../utils";
import { ErrorIndicator } from "../../../../common/ErrorIndicator";
import { CollapseContext } from "../../../../common/SummaryPageLayout";
import lock from "../../../../../images/lock.svg";
import unlock from "../../../../../images/unlock.svg";
import scrollToggle from "../../../../../images/move-arrows.svg";
import closedHand from "../../../../../images/closed.svg";
import fullscreen from "../../../../../images/fullscreen.svg";
import zoomInIcon from "../../../../../images/zoom-in.svg";
import zoomOutIcon from "../../../../../images/zoom-out.svg";
import source from "../../../../../images/source.png";
import map from "../../../../../images/map.png";
import reduce from "../../../../../images/reduce.png";
import sink from "../../../../../images/sink.png";
import input from "../../../../../images/input0.svg";
import generator from "../../../../../images/generator0.svg";

import "reactflow/dist/style.css";
import "./style.css";

const nodeWidth = 252;
const nodeHeight = 72;
const graphDirection = "LR";

const defaultNodeTypes: NodeTypes = {
  custom: CustomNode,
};

const defaultEdgeTypes: EdgeTypes = {
  custom: CustomEdge,
};

//sets nodes and edges to highlight in the graph
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
export const HighlightContext = createContext<HighlightContextProps>(null);

const getLayoutedElements = (
  nodes: Node[],
  edges: Edge[],
  direction: string
) => {
  const dagreGraph = new graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  const isHorizontal = direction === "LR";
  dagreGraph.setGraph({ rankdir: direction, ranksep: 240, edgesep: 180 });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
  });

  edges.forEach((edge) => {
    // skipping the side input edges to ensure graph alignment without the sideInputs
    if (!edge?.data?.sideInputEdge)
      dagreGraph.setEdge(edge.source, edge.target);
  });

  layout(dagreGraph);
  // setting nodes excepts generators and assigning generator height after remaining nodes are laid out
  let max_pos = 0;
  nodes.forEach((node) => {
    if (node?.data?.type !== "sideInput" && node?.data?.type !== "generator") {
      const nodeWithPosition = dagreGraph.node(node.id);
      max_pos = Math.max(max_pos, nodeWithPosition.y);
    }
  });
  let cnt = 2;
  nodes.forEach((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    if (node?.data?.type === "sideInput" || node?.data?.type === "generator") {
      nodeWithPosition.x = nodeWidth;
      nodeWithPosition.y = max_pos + nodeHeight * 0.75 * cnt;
      cnt++;
    }
    node.targetPosition = isHorizontal ? Position.Left : Position.Top;
    node.sourcePosition = isHorizontal ? Position.Right : Position.Bottom;

    // We are shifting the dagre node position (anchor=center) to the top left
    // so that it matches the React Flow node anchor point (top left).
    node.position = {
      x: nodeWithPosition.x - nodeWidth / 2,
      y: nodeWithPosition.y - nodeHeight / 2,
    };
  });

  return { nodes, edges };
};

const Flow = (props: FlowProps) => {
  const zoomLevel = useViewport().zoom;
  const { zoomIn, zoomOut, fitView } = useReactFlow();
  const [isLocked, setIsLocked] = useState(false);
  const [isPanOnScrollLocked, setIsPanOnScrollLocked] = useState(false);
  const [isMap, setIsMap] = useState(false);
  const [isReduce, setIsReduce] = useState(false);
  const [isSideInput, setIsSideInput] = useState(false);
  const isCollapsed = useContext(CollapseContext);
  const { host, isReadOnly } = useContext<AppContextProps>(AppContext);
  const {
    nodes,
    edges,
    onNodesChange,
    onEdgesChange,
    handleNodeClick,
    handleEdgeClick,
    handlePaneClick,
    handleEdgeEnter,
    handleEdgeLeave,
    refresh,
    namespaceId,
    data,
  } = props;

  const onIsLockedChange = useCallback(
    () => setIsLocked((prevState) => !prevState),
    []
  );
  const onIsPanOnScrollLockedChange = useCallback(
    () => setIsPanOnScrollLocked((prevState) => !prevState),
    []
  );
  const onFullScreen = useCallback(() => fitView(), [zoomLevel]);
  const onZoomIn = useCallback(() => zoomIn({ duration: 500 }), [zoomLevel]);
  const onZoomOut = useCallback(() => zoomOut({ duration: 500 }), [zoomLevel]);

  const [error, setError] = useState<string | undefined>(undefined);
  const [successMessage, setSuccessMessage] = useState<string | undefined>(
    undefined
  );
  const [statusPayload, setStatusPayload] = useState<any>(undefined);
  const [timerDateStamp, setTimerDateStamp] = useState<any>(undefined);
  const [timer, setTimer] = useState<any | undefined>(undefined);

  const handleTimer = useCallback(() => {
    if (timer) {
      clearInterval(timer);
    }
    const dateString = new Date().toISOString();
    const time = timeAgo(dateString);
    setTimerDateStamp(time);
    const pauseTimer = setInterval(() => {
      const time = timeAgo(dateString);
      setTimerDateStamp(time);
    }, 1000);
    setTimer(pauseTimer);
  }, [timer]);

  const handlePlayClick = useCallback(() => {
    handleTimer();
    setStatusPayload({
      spec: {
        lifecycle: {
          desiredPhase: RUNNING,
        },
      },
    });
  }, [handleTimer]);

  const handlePauseClick = useCallback(() => {
    handleTimer();
    setStatusPayload({
      spec: {
        lifecycle: {
          desiredPhase: PAUSED,
        },
      },
    });
  }, [handleTimer]);

  useEffect(() => {
    const patchStatus = async () => {
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines/${
            data?.pipeline?.metadata?.name
          }`,
          {
            method: "PATCH",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify(statusPayload),
          }
        );
        const error = await getAPIResponseError(response);
        if (error) {
          setError(error);
        } else {
          refresh();
          setSuccessMessage("Status updated successfully");
        }
      } catch (e: any) {
        setError(e);
      }
    };
    if (statusPayload) {
      patchStatus();
    }
  }, [statusPayload, host]);

  useEffect(() => {
    if (
      statusPayload?.spec?.lifecycle?.desiredPhase === PAUSED &&
      data?.pipeline?.status?.phase === PAUSED
    ) {
      setStatusPayload(undefined);
    }
    if (
      statusPayload?.spec?.lifecycle?.desiredPhase === RUNNING &&
      data?.pipeline?.status?.phase === RUNNING
    ) {
      setStatusPayload(undefined);
    }
  }, [data]);

  useEffect(() => {
    nodes.forEach((node) => {
      if (node?.data?.type === "sideInput") {
        setIsSideInput(true);
      }
      if (node?.data?.type === "udf") {
        node.data?.nodeInfo?.udf?.groupBy ? setIsReduce(true) : setIsMap(true);
      }
    });
  }, [nodes]);

  return (
    <ReactFlow
      nodeTypes={defaultNodeTypes}
      edgeTypes={defaultEdgeTypes}
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onEdgeClick={handleEdgeClick}
      onNodeClick={handleNodeClick}
      onPaneClick={handlePaneClick}
      onEdgeMouseEnter={handleEdgeEnter}
      onEdgeMouseLeave={handleEdgeLeave}
      fitView
      preventScrolling={!isLocked}
      panOnDrag={!isLocked}
      panOnScroll={isPanOnScrollLocked}
      minZoom={0.1}
      maxZoom={3.1}
    >
      {!isReadOnly && (
        <Panel
          position="top-left"
          style={{ marginTop: isCollapsed ? "4.8rem" : "10.8rem" }}
        >
          <Box
            sx={{
              display: "flex",
              flexDirection: "row",
            }}
          >
            <Button
              variant="contained"
              data-testid="resume"
              sx={{
                height: "3.2rem",
                width: "8rem",
                fontWeight: "bold",
                fontSize: "1.4rem",
              }}
              onClick={handlePlayClick}
              disabled={data?.pipeline?.status?.phase === RUNNING}
            >
              Resume
            </Button>
            <Button
              variant="contained"
              data-testid="pause"
              sx={{
                height: "3.2rem",
                width: "8rem",
                marginLeft: "1.6rem",
                fontWeight: "bold",
                fontSize: "1.4rem",
              }}
              onClick={handlePauseClick}
              disabled={
                data?.pipeline?.status?.phase === PAUSED ||
                data?.pipeline?.status?.phase === PAUSING
              }
            >
              Pause
            </Button>
            <Box sx={{ marginLeft: "1.6rem" }}>
              {error && statusPayload ? (
                <Alert
                  severity="error"
                  sx={{
                    backgroundColor: "#FDEDED",
                    color: "#5F2120",
                    fontSize: "1.6rem",
                  }}
                >
                  {error}
                </Alert>
              ) : successMessage &&
                statusPayload &&
                ((statusPayload.spec.lifecycle.desiredPhase === PAUSED &&
                  data?.pipeline?.status?.phase !== PAUSED) ||
                  (statusPayload.spec.lifecycle.desiredPhase === RUNNING &&
                    data?.pipeline?.status?.phase !== RUNNING)) ? (
                <div
                  style={{
                    borderRadius: "1.3rem",
                    width: "22.8rem",
                    background: "#F0F0F0",
                    display: "flex",
                    flexDirection: "row",
                    padding: "0.8rem",
                    color: "#516F91",
                    alignItems: "center",
                  }}
                  data-testid="pipeline-status"
                >
                  <CircularProgress
                    sx={{
                      width: "2rem !important",
                      height: "2rem !important",
                    }}
                  />{" "}
                  <Box
                    sx={{
                      display: "flex",
                      flexDirection: "column",
                    }}
                  >
                    <span style={{ marginLeft: "1.6rem", fontSize: "1.6rem" }}>
                      {statusPayload?.spec?.lifecycle?.desiredPhase === PAUSED
                        ? "Pipeline Pausing..."
                        : "Pipeline Resuming..."}
                    </span>
                    <span style={{ marginLeft: "1.6rem", fontSize: "1.6rem" }}>
                      {timerDateStamp}
                    </span>
                  </Box>
                </div>
              ) : (
                ""
              )}
            </Box>
          </Box>
        </Panel>
      )}
      <Panel
        position="top-right"
        style={{ marginTop: isCollapsed ? "0.8rem" : "10.8rem" }}
      >
        <ErrorIndicator />
      </Panel>
      <Panel position="bottom-left" className={"interaction"}>
        <Tooltip
          title={
            <div className={"interaction-button-tooltip"}>
              {isLocked ? "Unlock Graph" : "Lock Graph"}
            </div>
          }
          placement={"top"}
          arrow
        >
          <IconButton onClick={onIsLockedChange} data-testid={"lock"}>
            <img src={isLocked ? lock : unlock} alt={"lock-unlock"} />
          </IconButton>
        </Tooltip>
        <Tooltip
          title={
            <div className={"interaction-button-tooltip"}>
              {isPanOnScrollLocked ? "Zoom On Scroll" : "Pan on Scroll"}
            </div>
          }
          placement={"top"}
          arrow
        >
          <IconButton
            onClick={onIsPanOnScrollLockedChange}
            data-testid={"panOnScroll"}
          >
            <img
              src={isPanOnScrollLocked ? closedHand : scrollToggle}
              alt={"panOnScrollLock"}
            />
          </IconButton>
        </Tooltip>
        <div className={"divider"} />
        <Tooltip
          title={<div className={"interaction-button-tooltip"}>Fit Graph</div>}
          placement={"top"}
          arrow
        >
          <IconButton onClick={onFullScreen} data-testid={"fitView"}>
            <img src={fullscreen} alt={"fullscreen"} />
          </IconButton>
        </Tooltip>
        <div className={"divider"} />
        <Tooltip
          title={<div className={"interaction-button-tooltip"}>Zoom In</div>}
          placement={"top"}
          arrow
        >
          <IconButton onClick={onZoomIn} data-testid={"zoomIn"}>
            <img src={zoomInIcon} alt="zoom-in" />
          </IconButton>
        </Tooltip>
        <Tooltip
          title={<div className={"interaction-button-tooltip"}>Zoom Out</div>}
          placement="top"
          arrow
        >
          <IconButton onClick={onZoomOut} data-testid={"zoomOut"}>
            <img src={zoomOutIcon} alt="zoom-out" />
          </IconButton>
        </Tooltip>
        <svg
          xmlns="http://www.w3.org/2000/svg"
          width="54"
          height="30"
          viewBox="0 0 54 30"
          fill="none"
        >
          <g>
            <rect
              x="0.5"
              y="0.5"
              width="53"
              height="29"
              rx="3.5"
              fill="white"
              stroke="#D4D7DC"
            />
            <text x="50%" y="50%" className={"zoom-percent-text"}>
              {(((zoomLevel - 0.1) / 2) * 100).toFixed(0)}%
            </text>
          </g>
        </svg>
      </Panel>
      <Panel
        position="top-left"
        className={"legend"}
        style={{
          marginTop: isCollapsed
            ? isReadOnly
              ? "5.2rem"
              : "9.2rem"
            : isReadOnly
            ? "12rem"
            : "15.2rem",
          cursor: "default",
        }}
      >
        <Accordion>
          <AccordionSummary
            expandIcon={<ExpandMoreIcon />}
            aria-controls="panel1a-content"
            id="panel1a-header"
          >
            <Typography sx={{ fontSize: "1.6rem" }}>Legend</Typography>
          </AccordionSummary>
          <AccordionDetails>
            <div className={"legend-title"}>
              <img src={source} alt={"source"} />
              <div className={"legend-text"}>Source</div>
            </div>
            {isMap && (
              <div className={"legend-title"}>
                <img src={map} alt={"map"} />
                <div className={"legend-text"}>Map</div>
              </div>
            )}
            {isReduce && (
              <div className={"legend-title"}>
                <img src={reduce} alt={"reduce"} />
                <div className={"legend-text"}>Reduce</div>
              </div>
            )}
            <div className={"legend-title"}>
              <img src={sink} alt={"sink"} />
              <div className={"legend-text"}>Sink</div>
            </div>
            {isSideInput && (
              <div className={"legend-title"}>
                <img src={input} width={22} alt={"input"} />
                <div className={"legend-text"}>Input</div>
              </div>
            )}
            {isSideInput && (
              <div className={"legend-title"}>
                <img src={generator} width={22} alt={"generator"} />
                <div className={"legend-text"}>Generator</div>
              </div>
            )}
          </AccordionDetails>
        </Accordion>
      </Panel>
    </ReactFlow>
  );
};

// hides the side input edges
const hide = (hidden: { [x: string]: any }) => (edge: Edge) => {
  edge.hidden = hidden[edge?.data?.source];
  return edge;
};

const getHiddenValue = (edges: Edge[]) => {
  const hiddenEdges: { [key: string]: boolean } = {};
  edges?.forEach((edge) => {
    if (edge?.data?.sideInputEdge) {
      hiddenEdges[edge?.data?.source] = true;
    }
  });
  return hiddenEdges;
};

export default function Graph(props: GraphProps) {
  const { data, namespaceId, pipelineId, refresh } = props;
  const { sidebarProps, setSidebarProps } =
    useContext<AppContextProps>(AppContext);

  const { nodes: layoutedNodes, edges: layoutedEdges } = useMemo(() => {
    return getLayoutedElements(data.vertices, data.edges, graphDirection);
  }, [data]);

  const [nodes, setNodes] = useState<Node[]>(layoutedNodes);
  const [edges, setEdges] = useState<Edge[]>(layoutedEdges);
  const [sideNodes, setSideNodes] = useState<Map<string, Node>>(new Map());
  const [sideEdges, setSideEdges] = useState<Map<string, string>>(new Map());
  const initialHiddenValue = getHiddenValue(layoutedEdges);
  const [hidden, setHidden] = useState(initialHiddenValue);

  useEffect(() => {
    const nodeSet = new Map();
    layoutedNodes.forEach((node) => {
      if (node?.data?.sideHandle) {
        nodeSet.set(node?.data?.name, node);
      }
    });
    setSideNodes(nodeSet);
  }, [layoutedNodes]);

  useEffect(() => {
    const edgeSet: Map<string, string> = new Map();
    layoutedEdges.forEach((edge) => {
      if (edge?.data?.sideInputEdge) {
        edgeSet.set(edge?.id, edge?.targetHandle ?? "");
      }
    });
    setSideEdges(edgeSet);
  }, [layoutedEdges]);

  useEffect(() => {
    setNodes(layoutedNodes);
    setEdges(layoutedEdges);
  }, [data]);

  const onNodesChange = useCallback(
    (changes: NodeChange[]) =>
      setNodes((nds) => applyNodeChanges(changes, nds)),
    [setNodes]
  );

  const onEdgesChange = useCallback(
    (changes: EdgeChange[]) =>
      setEdges((eds) => applyEdgeChanges(changes, eds)),
    [setEdges]
  );

  const [edgeId, setEdgeId] = useState<string>();

  const openEdgeSidebar = useCallback(
    (edge: Edge) => {
      if (!setSidebarProps) {
        return;
      }
      const existingProps = sidebarProps ? JSON.stringify(sidebarProps) : "";
      const updated = {
        type: SidebarType.EDGE_DETAILS,
        edgeDetailsProps: {
          edgeId: edge.id,
          watermarks: edge?.data?.edgeWatermark?.watermarks,
        },
      };
      if (existingProps === JSON.stringify(updated)) {
        // Do not update if no data change. Avoids infinite loop.
        return;
      }
      setSidebarProps(updated);
    },
    [setSidebarProps, sidebarProps]
  );

  const handleEdgeClick = useCallback(
    (_: MouseEvent, edge: Edge) => {
      setEdgeId(edge.id);
      const updatedEdgeHighlightValues: any = {};
      updatedEdgeHighlightValues[edge.id] = true;
      setHighlightValues(updatedEdgeHighlightValues);
      openEdgeSidebar(edge);
    },
    [setSidebarProps, namespaceId, pipelineId, openEdgeSidebar]
  );

  // This has been added to make sure that edge container refreshes on edges being refreshed
  useEffect(() => {
    let flag = false;
    edges.forEach((dataEdge) => {
      const edge = {} as Edge;
      if (dataEdge.id === edgeId && !flag) {
        edge.data = dataEdge.data;
        edge.label = dataEdge.label;
        edge.id = dataEdge.id;
        flag = true;
        if (sidebarProps && sidebarProps?.type === SidebarType?.EDGE_DETAILS) {
          // Update sidebar data if already open
          openEdgeSidebar(edge);
        }
      }
    });
  }, [edges, edgeId, sidebarProps, openEdgeSidebar]);

  const [nodeId, setNodeId] = useState<string>();

  useEffect(() => {
    setEdges((eds) => eds.map(hide(hidden)));
  }, [hidden, data]);

  const openNodeSidebar = useCallback(
    (node: Node) => {
      if (!setSidebarProps) {
        return;
      }
      const existingProps = sidebarProps ? JSON.stringify(sidebarProps) : "";
      if (node?.data?.type === "sideInput") {
        const updated = {
          type: SidebarType.GENERATOR_DETAILS,
          generatorDetailsProps: {
            namespaceId,
            pipelineId,
            vertexId: node.id,
            generatorDetails: sideNodes.get(node.id) || {},
          },
        };
        if (existingProps === JSON.stringify(updated)) {
          // Do not update if no data change. Avoids infinite loop.
          return;
        }
        setSidebarProps(updated);
      } else {
        const updated = {
          type: SidebarType.VERTEX_DETAILS,
          vertexDetailsProps: {
            namespaceId,
            pipelineId,
            vertexId: node.id,
            vertexSpecs: node?.data?.nodeInfo,
            vertexMetrics: node?.data?.vertexMetrics?.podMetrics?.data,
            buffers: node?.data?.buffers,
            type: node?.data?.type,
            refresh,
          },
        };
        if (existingProps === JSON.stringify(updated)) {
          // Do not update if no data change. Avoids infinite loop.
          return;
        }
        setSidebarProps(updated);
      }
    },
    [namespaceId, pipelineId, sideNodes, setSidebarProps, sidebarProps, refresh]
  );

  const handleNodeClick = useCallback(
    (event: MouseEvent | undefined, node: Node) => {
      setNodeId(node.id);
      const target = event?.target as HTMLElement;
      setHidden((prevState) => {
        const updatedState: any = {};
        Object.keys(prevState).forEach((key) => {
          updatedState[key] =
            node?.data?.type === "sideInput" && target?.innerText === "---"
              ? !(key === node.id)
              : true;
        });
        return updatedState;
      });
      if (node?.data?.type !== "sideInput" || target?.innerText === "")
        openNodeSidebar(node);
    },
    [setHidden, openNodeSidebar]
  );

  // This has been added to make sure that node container refreshes on nodes being refreshed
  useEffect(() => {
    nodes.forEach((dataNode) => {
      const node = {} as Node;
      if (dataNode.id === nodeId) {
        node.data = dataNode.data;
        node.id = dataNode.id;
        if (
          sidebarProps &&
          (sidebarProps.type === SidebarType.VERTEX_DETAILS ||
            sidebarProps.type === SidebarType.GENERATOR_DETAILS)
        ) {
          // Update sidebar data if already open
          openNodeSidebar(node);
        }
      }
    });
  }, [nodes, nodeId, sidebarProps, openNodeSidebar]);

  const [highlightValues, setHighlightValues] = useState<{
    [key: string]: boolean;
  }>({});

  const handlePaneClick = useCallback(() => {
    setHighlightValues({});
    setHidden((prevState) => {
      const updatedState: any = {};
      Object.keys(prevState).forEach((key) => {
        updatedState[key] = true;
      });
      return updatedState;
    });
  }, [setHidden]);

  const [hoveredEdge, setHoveredEdge] = useState<string>("");

  const handleEdgeEnter = useCallback(
    (_: any, edge: Edge) => {
      if (edge?.data?.sideInputEdge) return;
      setHidden(initialHiddenValue);
      setHoveredEdge(edge.id);
      const updatedEdgeHighlightValues: any = {};
      updatedEdgeHighlightValues[edge.id] = true;
      setHighlightValues(updatedEdgeHighlightValues);
    },
    [initialHiddenValue]
  );

  const handleEdgeLeave = useCallback(
    (_: any, edge: Edge) => {
      if (edge?.data?.sideInputEdge) return;
      setHidden(initialHiddenValue);
      setHoveredEdge("");
      if (sidebarProps === undefined) {
        setHighlightValues({});
      }
    },
    [sidebarProps, initialHiddenValue]
  );

  return (
    <div style={{ height: "100%" }}>
      <div className="Graph" data-testid="graph">
        <HighlightContext.Provider
          value={{
            highlightValues,
            setHighlightValues,
            setHidden,
            sideInputNodes: sideNodes,
            sideInputEdges: sideEdges,
            hoveredEdge,
          }}
        >
          <ReactFlowProvider>
            <Flow
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              handleNodeClick={handleNodeClick}
              handleEdgeClick={handleEdgeClick}
              handlePaneClick={handlePaneClick}
              handleEdgeEnter={handleEdgeEnter}
              handleEdgeLeave={handleEdgeLeave}
              setSidebarProps={setSidebarProps}
              refresh={refresh}
              namespaceId={namespaceId}
              data={data}
            />
          </ReactFlowProvider>
        </HighlightContext.Provider>
      </div>
    </div>
  );
}
