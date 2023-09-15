import { Edge, Node } from "reactflow";
import { GraphData, Pipeline } from "./pipeline";

export interface GraphProps {
  data: GraphData;
  namespaceId: string | undefined;
  pipelineId: string | undefined;
}

export interface SpecProps {
  pipeline: Pipeline;
}

export interface EdgeInfoProps {
  edge: Edge;
}

export interface NodeInfoProps {
  node: Node;
  namespaceId: string | undefined;
  pipelineId: string | undefined;
}

export interface FlowProps {
  nodes: Node[];
  edges: Edge[];
  onNodesChange: any;
  onEdgesChange: any;
  onConnect: (params: any) => void;
  handleNodeClick: (e: Element | EventType, node: Node) => void;
  handleEdgeClick: (e: Element | EventType, edge: Edge) => void;
  handlePaneClick: () => void;
}
