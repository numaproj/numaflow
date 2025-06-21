import { Dispatch, SetStateAction } from "react";
import { Edge, Node } from "@xyflow/react";
import { GraphData, Pipeline } from "./pipeline";

export interface GraphProps {
  data: GraphData;
  namespaceId: string | undefined;
  pipelineId: string | undefined;
  type: "monoVertex" | "pipeline";
  refresh: () => void;
}

export interface SpecProps {
  pipeline: Pipeline;
}

export interface EdgeInfoProps {
  edge: Edge<Record<string, any>>;
}

export interface NodeInfoProps {
  node: Node<Record<string, any>>;
  namespaceId: string | undefined;
  pipelineId: string | undefined;
}

export interface FlowProps {
  nodes: Node<Record<string, any>>[];
  edges: Edge<Record<string, any>>[];
  onNodesChange: any;
  onEdgesChange: any;
  handleNodeClick: (e: Element | EventType, node: Node) => void;
  handleEdgeClick: (e: Element | EventType, edge: Edge) => void;
  handlePaneClick: () => void;
  handleEdgeEnter: (e: Element | EventType, edge: Edge) => void;
  handleEdgeLeave: (e: Element | EventType, edge: Edge) => void;
  setSidebarProps: (props: any) => void;
  refresh: () => void;
  namespaceId: string | undefined;
  data: any;
  type: string;
}

export interface HighlightContextProps {
  sideInputNodes: Map<string, Node>;
  sideInputEdges: Map<string, string>;
  highlightValues: { [key: string]: boolean };
  setHighlightValues: Dispatch<SetStateAction<{ [key: string]: boolean }>>;
  setHidden: Dispatch<SetStateAction<{ [key: string]: boolean }>>;
  hoveredEdge: string;
}
