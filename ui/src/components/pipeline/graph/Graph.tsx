import {ComponentType, MouseEvent, useCallback, useEffect, useMemo, useState} from "react";
import ReactFlow, {
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  Connection,
  Edge,
  EdgeChange,
  Node,
  NodeChange, NodeProps, NodeTypes, NodeTypesWrapped,Position,
} from "react-flow-renderer";
import dagre from "dagre";
import EdgeInfo from "../edgeinfo/EdgeInfo";
import NodeInfo from "../nodeinfo/NodeInfo";
import { GraphData } from "../../../utils/models/pipeline";
import wrapNode from "react-flow-renderer"

import "./Graph.css";
import Spec from "../spec/Spec";
import { Card } from "@mui/material";
import SourceNode from "./SourceNode";
import UDFNode from "./UDFNode";
import SinkNode from "./SinkNode";

const nodeWidth = 172;
const nodeHeight = 36;

const defaultNodeTypes: NodeTypes = {
 udf: UDFNode, sink: SinkNode, source: SourceNode
};

const getLayoutedElements = (
  nodes: Node[],
  edges: Edge[],
  direction = "LR"
) => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  const isHorizontal = direction === "LR";
  dagreGraph.setGraph({ rankdir: direction });

  nodes.forEach((node) => {

    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  nodes.forEach((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    node.targetPosition = isHorizontal ? Position.Left : Position.Top;
    node.sourcePosition = isHorizontal ? Position.Right : Position.Bottom;

    // We are shifting the dagre node position (anchor=center center) to the top left
    // so it matches the React Flow node anchor point (top left).
    node.position = {
      x: nodeWithPosition.x + Math.random() / 1000,
      y: nodeWithPosition.y,
    };

    return node;
  });

  return { nodes, edges };
};

interface GraphProps {
  data: GraphData;
  namespaceId: string | undefined;
  pipelineId: string | undefined;
}

export default function Graph(props: GraphProps) {
  const { data, namespaceId, pipelineId } = props;

  const { nodes: layoutedNodes, edges: layoutedEdges } = useMemo(() => {
    return getLayoutedElements(data.vertices, data.edges);
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
    setEdgeOpen(true);
    setEdgeId(edge.id);
    setShowSpec(false);
    setNodeOpen(false);
  };

  // This has been added to make sure that edge container refreshes on edges being refreshed
  useEffect(() => {
    edges.forEach((dataEdge) => {
      const edge = {} as Edge;
      if (dataEdge.id === edgeId) {
        edge.data = dataEdge.data;
        edge.id = dataEdge.id;
        setEdge(edge);
      }
    });
  }, [edges, edgeId]);

  const [nodeOpen, setNodeOpen] = useState(false);

  const [node, setNode] = useState<Node>();

  const handleNodeClick = (event: MouseEvent, node: Node) => {
    setNodeOpen(true);
    setNode(node);
    setShowSpec(false);
    setEdgeOpen(false);
  };

  const handlePaneClick = () => {
    setShowSpec(true);
    setEdgeOpen(false);
    setNodeOpen(false);
  };

  const [showSpec, setShowSpec] = useState(true);

  return (
    <div
      style={{
        overflow: "scroll",
      }}
    >
      <div
        className="Graph"
        data-testid="graph"
        style={{
          width: "100%",
          height: 500,
          overflow: "scroll !important",
        }}
      >
        <ReactFlow
          preventScrolling={false}
          nodeTypes={defaultNodeTypes}
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onEdgeClick={handleEdgeClick}
          onNodeClick={handleNodeClick}
          onPaneClick={handlePaneClick}
          fitView
          zoomOnScroll={true}
          panOnDrag={true}
        />
      </div>

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