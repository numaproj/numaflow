import { Edge, Node } from "reactflow";
import { GraphData, Pipeline } from "./pipeline";

interface GraphProps {
  data: GraphData;
  namespaceId: string | undefined;
  pipelineId: string | undefined;
}

interface SpecProps {
  pipeline: Pipeline;
}

interface EdgeInfoProps {
  edge: Edge;
}

interface NodeInfoProps {
  node: Node;
  namespaceId: string | undefined;
  pipelineId: string | undefined;
}
