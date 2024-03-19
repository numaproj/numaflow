import { useContext, useEffect, useState } from "react";
import { useFetch, Options } from "./fetch";
import { getBaseHref } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";
import {
  ClusterNamespaceSummary,
  ClusterSummaryData,
  ClusterSummaryFetchProps,
  ClusterSummaryFetchResult,
} from "../../types/declarations/cluster";

const rawDataToClusterSummary = (
  rawData: any[]
): ClusterSummaryData | undefined => {
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
      isEmpty: !!ns.isEmpty,
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

const DEFAULT_NS_NAME = "default";
const DATA_REFRESH_INTERVAL = 30000; // ms

export const useClusterSummaryFetch = ({
  loadOnRefresh = false,
  addError,
}: ClusterSummaryFetchProps) => {
  const [results, setResults] = useState<ClusterSummaryFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
  });
  const [options, setOptions] = useState<Options>({
    skip: false,
    requestKey: "",
  });
  const { host } = useContext<AppContextProps>(AppContext);

  const {
    data: fetchData,
    loading: fetchLoading,
    error: fetchError,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/cluster-summary`,
    undefined,
    options
  );

  useEffect(() => {
    setInterval(() => {
      setOptions({
        skip: false,
        requestKey: "id" + Math.random().toString(16).slice(2),
      });
    }, DATA_REFRESH_INTERVAL);
  }, []);

  useEffect(() => {
    if (fetchLoading) {
      if (options?.requestKey === "" || loadOnRefresh) {
        // Only set loading true when first load or when loadOnRefresh is true
        setResults({
          data: undefined,
          loading: true,
          error: undefined,
        });
      }
      return;
    }
    if (fetchError) {
      if (options?.requestKey === "") {
        // Failed on first load, return error
        setResults({
          data: undefined,
          loading: false,
          error: fetchError,
        });
      } else {
        // Failed on refresh, add error to app context
        addError(fetchError);
      }
      return;
    }
    if (fetchData?.errMsg) {
      if (options?.requestKey === "") {
        // Failed on first load, return error
        setResults({
          data: undefined,
          loading: false,
          error: fetchData.errMsg,
        });
      } else {
        // Failed on refresh, add error to app context
        addError(fetchData.errMsg);
      }
      return;
    }
    if (fetchData) {
      const clusterSummary = rawDataToClusterSummary(fetchData.data);
      setResults({
        data: clusterSummary,
        loading: false,
        error: undefined,
      });
      return;
    }
  }, [fetchData, fetchLoading, fetchError, loadOnRefresh, options]);

  return results;
};
