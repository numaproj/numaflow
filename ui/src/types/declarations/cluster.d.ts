export interface ClusterNamespaceSummary {
  name: string;
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
}

export interface ClusterSummaryData {
  namespacesCount: number;
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
  nameSpaceSummaries: ClusterNamespaceSummary[];
}

export interface ClusterSummaryFetchResult {
  data?: ClusterSummaryData;
  loading: boolean;
  error: any;
}

export interface ClusterSummaryFetchProps {
  loadOnRefresh?: boolean;
}

export interface ClusterNamespaceListingProps {
  data: ClusterSummaryData;
}

export interface NamespaceCardProps {
  data: ClusterNamespaceSummary;
}
