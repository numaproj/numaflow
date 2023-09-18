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