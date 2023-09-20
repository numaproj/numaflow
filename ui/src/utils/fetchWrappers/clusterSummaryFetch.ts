import { useEffect, useState } from "react";
import { useFetch } from "./fetch";

const MOCK_DATA = [
  {
    namespace: "my-ns",
    pipelineSummary: {
      active: {
        Healthy: 1,
        Warning: 1,
        Critical: 1,
      },
      inactive: 2,
    },
    isbServiceSummary: {
      active: {
        Healthy: 2,
        Warning: 2,
        Critical: 2,
      },
      inactive: 2,
    },
  },
  {
    namespace: "my-other-ns",
    pipelineSummary: {
      active: {
        Healthy: 5,
        Warning: 1,
        Critical: 1,
      },
      inactive: 1,
    },
    isbServiceSummary: {
      active: {
        Healthy: 5,
        Warning: 2,
        Critical: 2,
      },
      inactive: 1,
    },
  },
  {
    namespace: "my-other-ns1",
    pipelineSummary: {
      active: {
        Healthy: 5,
        Warning: 1,
        Critical: 1,
      },
      inactive: 1,
    },
    isbServiceSummary: {
      active: {
        Healthy: 5,
        Warning: 2,
        Critical: 2,
      },
      inactive: 1,
    },
  },
  {
    namespace: "my-other-ns2",
    pipelineSummary: {
      active: {
        Healthy: 5,
        Warning: 1,
        Critical: 1,
      },
      inactive: 1,
    },
    isbServiceSummary: {
      active: {
        Healthy: 5,
        Warning: 2,
        Critical: 2,
      },
      inactive: 1,
    },
  },
  {
    namespace: "my-other-ns3",
    pipelineSummary: {
      active: {
        Healthy: 5,
        Warning: 1,
        Critical: 1,
      },
      inactive: 1,
    },
    isbServiceSummary: {
      active: {
        Healthy: 5,
        Warning: 2,
        Critical: 2,
      },
      inactive: 1,
    },
  },
  {
    namespace: "my-other-ns4",
    pipelineSummary: {
      active: {
        Healthy: 5,
        Warning: 1,
        Critical: 1,
      },
      inactive: 1,
    },
    isbServiceSummary: {
      active: {
        Healthy: 5,
        Warning: 2,
        Critical: 2,
      },
      inactive: 1,
    },
  },
  {
    namespace: "my-other-ns5",
    pipelineSummary: {
      active: {
        Healthy: 5,
        Warning: 1,
        Critical: 1,
      },
      inactive: 1,
    },
    isbServiceSummary: {
      active: {
        Healthy: 5,
        Warning: 2,
        Critical: 2,
      },
      inactive: 1,
    },
  },
  {
    namespace: "my-other-ns6",
    pipelineSummary: {
      active: {
        Healthy: 5,
        Warning: 1,
        Critical: 1,
      },
      inactive: 1,
    },
    isbServiceSummary: {
      active: {
        Healthy: 5,
        Warning: 2,
        Critical: 2,
      },
      inactive: 1,
    },
  },
];

const DEFAULT_NS_NAME = "default";

const rawDataToClusterSummary = (rawData: any): ClusterSummaryData | undefined => {
  if (!rawData || !Array.isArray(rawData)) {
    return undefined;
  }
  const namespacesCount = rawData.length;
  let pipelinesCount = 0;
  let pipelinesActiveCount = 0;
  let pipelinesInactiveCount = 0;
  let pipelinesHealthyCount = 0;
  let pipelinesWarningCount = 0;
  let pipelinesCriticalCount = 0;
  let isbsCount = 0;
  let isbsActiveCount = 0;
  let isbsInactiveCount = 0;
  let isbsHealthyCount = 0;
  let isbsWarningCount = 0;
  let isbsCriticalCount = 0;
  const nameSpaceSummaries: ClusterNamespaceSummary[] = [];

  rawData.forEach((ns: any) => {
    // Pipeline counts
    const nsPipelinesHealthyCount = ns.pipelineSummary?.active?.Healthy || 0;
    const nsPipelinesWarningCount = ns.pipelineSummary?.active?.Warning || 0;
    const nsPipelinesCriticalCount = ns.pipelineSummary?.active?.Critical || 0;
    const nsPipelinesActiveCount =
      nsPipelinesHealthyCount +
      nsPipelinesWarningCount +
      nsPipelinesCriticalCount;
    const nsPipelinesInactiveCount = ns.pipelineSummary?.inactive || 0;
    const nsPipelinesCount = nsPipelinesActiveCount + nsPipelinesInactiveCount;
    // ISB counts
    const nsIsbsHealthyCount = ns.isbServiceSummary?.active?.Healthy || 0;
    const nsIsbsWarningCount = ns.isbServiceSummary?.active?.Warning || 0;
    const nsIsbsCriticalCount = ns.isbServiceSummary?.active?.Critical || 0;
    const nsIsbsActiveCount =
      nsIsbsHealthyCount + nsIsbsWarningCount + nsIsbsCriticalCount;
    const nsIsbsInactiveCount = ns.isbServiceSummary?.inactive || 0;
    const nsIsbsCount = nsIsbsActiveCount + nsIsbsInactiveCount;
    // Add ns summary to array
    nameSpaceSummaries.push({
      name: ns.namespace || DEFAULT_NS_NAME,
      pipelinesCount: nsPipelinesCount,
      pipelinesActiveCount: nsPipelinesActiveCount,
      pipelinesInactiveCount: nsPipelinesInactiveCount,
      pipelinesHealthyCount: nsPipelinesHealthyCount,
      pipelinesWarningCount: nsPipelinesWarningCount,
      pipelinesCriticalCount: nsPipelinesCriticalCount,
      isbsCount: nsIsbsCount,
      isbsActiveCount: nsIsbsActiveCount,
      isbsInactiveCount: nsIsbsInactiveCount,
      isbsHealthyCount: nsIsbsHealthyCount,
      isbsWarningCount: nsIsbsWarningCount,
      isbsCriticalCount: nsIsbsCriticalCount,
    });
    // Add counts to cluster summary counts
    pipelinesCount += nsPipelinesCount;
    pipelinesActiveCount += nsPipelinesActiveCount;
    pipelinesInactiveCount += nsPipelinesInactiveCount;
    pipelinesHealthyCount += nsPipelinesHealthyCount;
    pipelinesWarningCount += nsPipelinesWarningCount;
    pipelinesCriticalCount += nsPipelinesCriticalCount;
    isbsCount += nsIsbsCount;
    isbsActiveCount += nsIsbsActiveCount;
    isbsInactiveCount += nsIsbsInactiveCount;
    isbsHealthyCount += nsIsbsHealthyCount;
    isbsWarningCount += nsIsbsWarningCount;
    isbsCriticalCount += nsIsbsCriticalCount;
  });
  return {
    namespacesCount,
    pipelinesCount,
    pipelinesActiveCount,
    pipelinesInactiveCount,
    pipelinesHealthyCount,
    pipelinesWarningCount,
    pipelinesCriticalCount,
    isbsCount,
    isbsActiveCount,
    isbsInactiveCount,
    isbsHealthyCount,
    isbsWarningCount,
    isbsCriticalCount,
    nameSpaceSummaries,
  };
};

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
  refreshKey?: number;
  loadOnRefresh?: boolean;
}

export const useClusterSummaryFetch = ({
  refreshKey = 0,
  loadOnRefresh = false,
}: ClusterSummaryFetchProps) => {
  const [results, setResults] = useState<ClusterSummaryFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
  });

  // TODO get data from actual API
  // const {
  //   data: fetchData,
  //   loading: fetchLoading,
  //   error: fetchError,
  // } = useFetch(`/api/v1/namespaces/${namespaceId}/pipelines`);

  useEffect(() => {
    if (loadOnRefresh) {
      setResults((results) => ({ ...results, loading: true, error: undefined }));
    }
    // TODO replace with actual API results
    setTimeout(() => {
      setResults({ data: rawDataToClusterSummary(MOCK_DATA), loading: false, error: undefined });
    }, 2000);
  }, [refreshKey]);

  return results;
};
