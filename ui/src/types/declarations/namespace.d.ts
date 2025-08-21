import { Dispatch, SetStateAction } from "react";
import { PipelineData } from "../../components/pages/Namespace/partials/NamespaceListingWrapper/PipelinesTypes";

export interface NamespacePipelineSummary {
  name: string;
  status: string;
  // TODO spec
}

export interface NamespaceSummaryData {
  pipelinesCount: number;
  pipelinesActiveCount: number;
  pipelinesInactiveCount: number;
  pipelinesHealthyCount: number;
  pipelinesWarningCount: number;
  pipelinesCriticalCount: number;
  isbsCount: number;
  isbsActiveCount: number;
  isbsInactiveCount: number;
  isbsHealthyCount: number;
  isbsWarningCount: number;
  isbsCriticalCount: number;
  pipelineSummaries: NamespacePipelineSummary[];
  pipelineRawData?: any;
  isbRawData?: any;
}

export interface NamespaceSummaryFetchResult {
  data?: NamespaceSummaryData;
  pipelineRawData?: any;
  isbRawData?: any;
  monoVertexRawData?: any;
  loading: boolean;
  error: any;
  refresh: () => void;
}

export interface NamespaceSummaryFetchProps {
  namespace: string;
  loadOnRefresh?: boolean;
  addError: (error: string) => void;
}
export interface PipelineCardProps {
  namespace: string;
  data: NamespacePipelineSummary;
  statusData?: any;
  isbData?: any;
  refresh: () => void;
  setPipelineHealthMap: Dispatch<SetStateAction<Record<string, string>>>;
}

export interface MonoVertexCardProps {
  namespace: string;
  data: NamespacePipelineSummary;
  statusData?: any;
  isbData?: any;
  refresh: () => void;
  setMonoVertexHealthMap: Dispatch<SetStateAction<Record<string, string>>>;
}

export interface NamespacePipelineListingProps {
  namespace: string;
  data: NamespaceSummaryData;
  pipelineData?: Map<string, PipelineData>;
  isbData?: any;
  monoVertexData?: any;
  refresh: () => void;
}

export interface K8sEvent {
  eventKey: number;
  namespace: string;
  timestamp: string;
  type: string;
  object: string;
  reason: string;
  message: string;
}

export interface K8sEventSummary {
  normalCount: number;
  warningCount: number;
  events: K8sEvent[];
}
export interface NamespaceK8sEventsFetchProps {
  namespace: string;
  pipeline?: string;
  vertex?: string;
}
export interface NamespaceK8sEventsFetchResult {
  data?: K8sEventSummary;
  loading: boolean;
  error: any;
}
