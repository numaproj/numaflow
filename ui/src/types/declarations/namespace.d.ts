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
}

export interface NamespaceSummaryFetchResult {
  data?: NamespaceSummaryData;
  loading: boolean;
  error: any;
}

export interface NamespaceSummaryFetchProps {
  namespace: string;
  loadOnRefresh?: boolean;
}
export interface PipelineCardProps {
  namespace: string;
  data: NamespacePipelineSummary;
}
export interface NamespacePipelineListingProps {
  namespace: string;
  data: NamespaceSummaryData;
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
}
export interface NamespaceK8sEventsFetchResult {
  data?: K8sEventSummary;
  loading: boolean;
  error: any;
}
