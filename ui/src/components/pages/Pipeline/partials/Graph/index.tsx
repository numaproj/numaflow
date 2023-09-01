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
  Controls,
} from "reactflow";
import { Card } from "@mui/material";
import { graphlib, layout } from "dagre";
import EdgeInfo from "./partials/EdgeInfo";
import NodeInfo from "./partials/NodeInfo";
import Spec from "./partials/Spec";
import CustomEdge from "./partials/CustomEdge";
import CustomNode from "./partials/CustomNode";
import { GraphProps } from "../../../../../types/declarations/graph";

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
    <div>
      <div className="Graph" data-testid="graph">
        <ReactFlow
          preventScrolling={false}
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
          zoomOnScroll={true}
          panOnDrag={true}
        >
          <Controls />
        </ReactFlow>
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
