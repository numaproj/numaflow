import { Dispatch, SetStateAction } from "react";
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
  handleNodeClick: (e: Element | EventType, node: Node) => void;
  handleEdgeClick: (e: Element | EventType, edge: Edge) => void;
  handlePaneClick: () => void;
}

export interface HighlightContextProps {
  sideInputNodes: Map<string, Node>;
  sideInputEdges: Map<string, string>;
  highlightValues: { [key: string]: boolean };
  setHighlightValues: Dispatch<SetStateAction<{ [key: string]: boolean }>>;
  setHidden: Dispatch<SetStateAction<{ [key: string]: boolean }>>;
  handleNodeClick: (e: any, node: Node<any, string>) => void;
}
