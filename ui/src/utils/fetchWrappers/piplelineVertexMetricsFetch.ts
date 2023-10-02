import { useEffect, useState } from "react";
import { useFetch, Options } from "./fetch";
import {
  PipelineVertexMetrics,
  PipelineVertexMetricsFetchResult,
  PiplelineVertexMetricsFetchProps,
} from "../../types/declarations/pipeline";

const rawDataToVertexMetrics = (
  rawData: any
): PipelineVertexMetrics[] | undefined => {
  if (!rawData) {
    return undefined;
  }
  return Object.keys(rawData).map((vertexId: string) => {
    return {
      vertexId,
      metrics: rawData[vertexId].map((item: any, index: number) => {
        return {
          partition: index,
          oneM:
            item.processingRates && item.processingRates["1m"]
              ?item.processingRates["1m"].toFixed(2)
              : 0,
          fiveM:
            item.processingRates && item.processingRates["5m"]
              ? item.processingRates["5m"].toFixed(2)
              : 0,
          fifteenM:
            item.processingRates && item.processingRates["15m"]
              ? item.processingRates["15m"].toFixed(2)
              : 0,
        };
      }),
    };
  });
};

export const usePiplelineVertexMetricsFetch = ({
  namespace,
  pipeline,
  loadOnRefresh = false,
}: PiplelineVertexMetricsFetchProps) => {
  const [results, setResults] = useState<PipelineVertexMetricsFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
  });
  const [options] = useState<Options>({
    skip: false,
    requestKey: "",
  });
  const { data, loading, error } = useFetch(
    `/api/v1/namespaces/${namespace}/pipelines/${pipeline}/vertices/metrics`,
    undefined,
    options
  );

  useEffect(() => {
    if (loading) {
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
    if (error) {
      setResults({
        data: undefined,
        loading: false,
        error: error,
      });
      return;
    }
    if (data?.errMsg) {
      setResults({
        data: undefined,
        loading: false,
        error: data?.errMsg,
      });
      return;
    }
    if (data) {
      const result = rawDataToVertexMetrics(data.data);
      setResults({
        data: result,
        loading: false,
        error: undefined,
      });
      return;
    }
  }, [data, loading, error, loadOnRefresh, options]);

  return results;
};
