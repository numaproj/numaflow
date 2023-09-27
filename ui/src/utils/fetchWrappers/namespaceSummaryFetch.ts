import { useEffect, useState } from "react";
import { useFetch, Options } from "./fetch";
import {
  NamespacePipelineSummary,
  NamespaceSummaryData,
  NamespaceSummaryFetchProps,
  NamespaceSummaryFetchResult,
} from "../../types/declarations/namespace";

const MOCK_PIPELINE_DATA: any[] = [
  {
    name: "pipeline1",
    status: "healthy",
  },
  {
    name: "pipeline2",
    status: "warning",
  },
  {
    name: "pipeline3",
    status: "critical",
  },
  {
    name: "pipeline4",
    status: "inactive",
  },
];
const MOCK_ISB_DATA: any[] = [
  {
    name: "isb1",
    status: "healthy",
  },
  {
    name: "isb2",
    status: "warning",
  },
  {
    name: "isb3",
    status: "critical",
  },
  {
    name: "isb4",
    status: "inactive",
  },
];

const rawDataToNamespaceSummary = (
  rawPipelineData: any[],
  rawIsbData: any[]
): NamespaceSummaryData | undefined => {
  if (
    !rawPipelineData ||
    !Array.isArray(rawPipelineData) ||
    !rawIsbData ||
    !Array.isArray(rawIsbData)
  ) {
    return undefined;
  }
  const pipelinesCount = rawPipelineData.length;
  let pipelinesActiveCount = 0;
  let pipelinesInactiveCount = 0;
  let pipelinesHealthyCount = 0;
  let pipelinesWarningCount = 0;
  let pipelinesCriticalCount = 0;
  const isbsCount = rawIsbData.length;
  let isbsActiveCount = 0;
  let isbsInactiveCount = 0;
  let isbsHealthyCount = 0;
  let isbsWarningCount = 0;
  let isbsCriticalCount = 0;
  const pipelineSummaries: NamespacePipelineSummary[] = [];
  rawPipelineData.forEach((pipeline: any) => {
    switch (pipeline.status) {
      case "healthy":
        pipelinesActiveCount++;
        pipelinesHealthyCount++;
        break;
      case "warning":
        pipelinesActiveCount++;
        pipelinesWarningCount++;
        break;
      case "critical":
        pipelinesActiveCount++;
        pipelinesCriticalCount++;
        break;
      case "inactive":
        pipelinesInactiveCount++;
        break;
      default:
        break;
    }
    // Add pipeline summary to array
    pipelineSummaries.push({
      name: pipeline.name,
      status: pipeline.status,
    });
  });
  rawIsbData.forEach((isb: any) => {
    switch (isb.status) {
      case "healthy":
        isbsActiveCount++;
        isbsHealthyCount++;
        break;
      case "warning":
        isbsActiveCount++;
        isbsWarningCount++;
        break;
      case "critical":
        isbsActiveCount++;
        isbsCriticalCount++;
        break;
      case "inactive":
        isbsInactiveCount++;
        break;
      default:
        break;
    }
  });
  // TODO how to map ISB to pipeline?
  return {
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
    pipelineSummaries,
  };
};

const DATA_REFRESH_INTERVAL = 15000; // ms

export const useNamespaceSummaryFetch = ({
  namespace,
  loadOnRefresh = false,
}: NamespaceSummaryFetchProps) => {
  const [results, setResults] = useState<NamespaceSummaryFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
  });
  const [options, setOptions] = useState<Options>({
    skip: false,
    requestKey: "",
  });
  const {
    data: pipelineData,
    loading: pipelineLoading,
    error: pipelineError,
  } = useFetch(
    `/api/v1_1/namespaces/${namespace}/pipelines`,
    undefined,
    options
  );
  const {
    data: isbData,
    loading: isbLoading,
    error: isbError,
  } = useFetch(
    `/api/v1_1/namespaces/${namespace}/isb-services`,
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
    if (pipelineLoading || isbLoading) {
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
    // if (pipelineError || isbError) {
    //   setResults({
    //     data: undefined,
    //     loading: false,
    //     error: pipelineError || isbError,
    //   });
    //   return;
    // }
    // if (pipelineData?.errMsg || isbData?.errMsg) {
    //   setResults({
    //     data: undefined,
    //     loading: false,
    //     error: pipelineData?.errMsg || isbData?.errMsg,
    //   });
    //   return;
    // }
    // if (pipelineData && isbData) {
    const nsSummary = rawDataToNamespaceSummary(
      // TODO REMOVE MOCK
      MOCK_PIPELINE_DATA,
      MOCK_ISB_DATA
    );
    // const nsSummary = rawDataToNamespaceSummary(
    //   pipelineData.data,
    //   isbData.data
    // );
    setResults({
      data: nsSummary,
      loading: false,
      error: undefined,
    });
    //   return;
    // }
  }, [
    pipelineData,
    isbData,
    pipelineLoading,
    isbLoading,
    pipelineError,
    isbError,
    loadOnRefresh,
    options,
  ]);

  return results;
};
