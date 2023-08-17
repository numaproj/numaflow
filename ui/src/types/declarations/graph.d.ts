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
