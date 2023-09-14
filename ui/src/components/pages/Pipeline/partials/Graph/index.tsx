import { MouseEvent, useCallback, useEffect, useMemo, useState } from "react";

import ReactFlow, {
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  Connection,
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
import Card from "@mui/material/Card";
import IconButton from "@mui/material/IconButton";
import { graphlib, layout } from "dagre";
import EdgeInfo from "./partials/EdgeInfo";
import NodeInfo from "./partials/NodeInfo";
import Spec from "./partials/Spec";
import CustomEdge from "./partials/CustomEdge";
import CustomNode from "./partials/CustomNode";
import { FlowProps, GraphProps } from "../../../../../types/declarations/graph";
import lock from "../../../../../images/lock.svg";
import unlock from "../../../../../images/unlock.svg";
import scrollToggle from "../../../../../images/move-arrows.svg";
import fullscreen from "../../../../../images/fullscreen.svg";
import sidePanel from "../../../../../images/side-panel.svg";
import zoomInIcon from "../../../../../images/zoom-in.svg";
import zoomOutIcon from "../../../../../images/zoom-out.svg";

import "reactflow/dist/style.css";
import "./style.css";

const nodeWidth = 230;
const nodeHeight = 48;
const graphDirection = "LR";

const defaultNodeTypes: NodeTypes = {
  custom: CustomNode,
};

const defaultEdgeTypes: EdgeTypes = {
  custom: CustomEdge,
};

const getLayoutedElements = (
  nodes: Node[],
  edges: Edge[],
  direction: string
) => {
  const dagreGraph = new graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  const isHorizontal = direction === "LR";
  dagreGraph.setGraph({ rankdir: direction, ranksep: 80 });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  layout(dagreGraph);

  nodes.forEach((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
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
  const [isScrollLocked, setIsScrollLocked] = useState(false);
  const {
    nodes,
    edges,
    onNodesChange,
    onEdgesChange,
    onConnect,
    handleNodeClick,
    handleEdgeClick,
    handlePaneClick,
  } = props;

  return (
    <ReactFlow
      nodeTypes={defaultNodeTypes}
      edgeTypes={defaultEdgeTypes}
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onConnect={onConnect}
      onEdgeClick={handleEdgeClick}
      onNodeClick={handleNodeClick}
      onPaneClick={handlePaneClick}
      fitView
      preventScrolling={isScrollLocked}
      panOnDrag={!isLocked}
    >
      <Panel position="bottom-left">
        <IconButton onClick={() => setIsLocked((prevState) => !prevState)}>
          <img src={isLocked ? lock : unlock} alt={"lock"} />
        </IconButton>
        <IconButton
          onClick={() => setIsScrollLocked((prevState) => !prevState)}
        >
          <img src={scrollToggle} alt={"scrollLock"} />
        </IconButton>
        <div className={"divider"} />
        <IconButton onClick={() => fitView()}>
          <img src={fullscreen} alt={"fullscreen"} />
        </IconButton>
        <IconButton
          onClick={() => {
            //TODO add single panel logic
            alert("sidePanel");
          }}
        >
          <img src={sidePanel} alt={"sidePanel"} />
        </IconButton>
        <div className={"divider"} />
        <IconButton
          onClick={() => {
            zoomIn({ duration: 500 });
          }}
        >
          <img src={zoomInIcon} alt="zoom-in" />
        </IconButton>
        <IconButton
          onClick={() => {
            zoomOut({ duration: 500 });
          }}
        >
          <img src={zoomOutIcon} alt="zoom-out" />
        </IconButton>
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
              {(((zoomLevel - 0.5) / 1.5) * 100).toFixed(0)}%
            </text>
          </g>
        </svg>
      </Panel>
    </ReactFlow>
  );
};

export default function Graph(props: GraphProps) {
  const { data, namespaceId, pipelineId } = props;

  const { nodes: layoutedNodes, edges: layoutedEdges } = useMemo(() => {
    return getLayoutedElements(data.vertices, data.edges, graphDirection);
  }, [data]);

  const [nodes, setNodes] = useState<Node[]>(layoutedNodes);
  const [edges, setEdges] = useState<Edge[]>(layoutedEdges);

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
  const onConnect = useCallback(
    (connection: Connection) => setEdges((eds) => addEdge(connection, eds)),
    [setEdges]
  );

  const [edgeOpen, setEdgeOpen] = useState(false);

  const [edgeId, setEdgeId] = useState<string>();
  const [edge, setEdge] = useState<Edge>();

  const handleEdgeClick = (event: MouseEvent, edge: Edge) => {
    setEdge(edge);
    setEdgeId(edge.id);
    setEdgeOpen(true);
    setShowSpec(false);
    setNodeOpen(false);
  };

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
        setEdge(edge);
      }
    });
  }, [edges, edgeId]);

  const [nodeOpen, setNodeOpen] = useState(false);

  const [nodeId, setNodeId] = useState<string>();
  const [node, setNode] = useState<Node>();

  const handleNodeClick = (event: MouseEvent, node: Node) => {
    setNode(node);
    setNodeId(node.id);
    setNodeOpen(true);
    setShowSpec(false);
    setEdgeOpen(false);
  };

  // This has been added to make sure that node container refreshes on nodes being refreshed
  useEffect(() => {
    nodes.forEach((dataNode) => {
      const node = {} as Node;
      if (dataNode.id === nodeId) {
        node.data = dataNode.data;
        node.id = dataNode.id;
        setNode(node);
      }
    });
  }, [nodes, nodeId]);

  const handlePaneClick = () => {
    setShowSpec(true);
    setEdgeOpen(false);
    setNodeOpen(false);
  };

  const [showSpec, setShowSpec] = useState(true);

  return (
    <div style={{ height: "100%" }}>
      <div className="Graph" data-testid="graph">
        <ReactFlowProvider>
          <Flow
            {...{
              nodes,
              edges,
              onNodesChange,
              onEdgesChange,
              onConnect,
              handleNodeClick,
              handleEdgeClick,
              handlePaneClick,
            }}
          />
        </ReactFlowProvider>
      </div>
      //TODO move this to side panel
      <Card
        sx={{ borderBottom: 1, borderColor: "divider", boxShadow: 1 }}
        data-testid={"card"}
        variant={"outlined"}
      >
        {showSpec && <Spec pipeline={data.pipeline} />}
        {edgeOpen && <EdgeInfo data-testid="edge-info" edge={edge} />}
        {nodeOpen && (
          <NodeInfo
            data-testid="node-info"
            node={node}
            namespaceId={namespaceId}
            pipelineId={pipelineId}
          />
        )}
      </Card>
    </div>
  );
}
