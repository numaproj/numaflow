import { Dispatch, SetStateAction } from "react";
import { Edge, Node } from "reactflow";
import { GraphData, Pipeline } from "./pipeline";

export interface GraphProps {
  data: GraphData;
  namespaceId: string | undefined;
  pipelineId: string | undefined;
  refresh: () => void;
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
  handleNodeClick: (e: Element | EventType, node: Node) => void;
  handleEdgeClick: (e: Element | EventType, edge: Edge) => void;
  handlePaneClick: () => void;
  handleEdgeEnter: (e: Element | EventType, edge: Edge) => void;
  handleEdgeLeave: (e: Element | EventType, edge: Edge) => void;
  setSidebarProps: (props: any) => void;
  refresh: () => void;
  namespaceId: string | undefined;
  data: any;
}

export interface HighlightContextProps {
  sideInputNodes: Map<string, Node>;
  sideInputEdges: Map<string, string>;
  highlightValues: { [key: string]: boolean };
  setHighlightValues: Dispatch<SetStateAction<{ [key: string]: boolean }>>;
  setHidden: Dispatch<SetStateAction<{ [key: string]: boolean }>>;
  hoveredEdge: string;
}
