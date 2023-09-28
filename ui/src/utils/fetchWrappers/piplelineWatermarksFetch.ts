import { useEffect, useState } from "react";
import { useFetch, Options } from "./fetch";
import {
  PipelineWatermarks,
  PipelineWatermarksFetchResult,
  PipelineWatermarksFetchProps,
} from "../../types/declarations/pipeline";

const rawDataToWatermarks = (
  rawData: any
): PipelineWatermarks[] | undefined => {
  if (!rawData) {
    return undefined;
  }
  return rawData.map((item: any) => {
    return {
      edgeId: item.edge,
      watermarks: item.watermarks.map((watermark: number, index: number) => {
        return {
          partition: index,
          watermark,
          formattedWatermark: new Date(watermark).toISOString(),
        }
      }),
    };
  });
};

export const usePiplelineWatermarksFetch = ({
  namespace,
  pipeline,
  loadOnRefresh = false,
}: PipelineWatermarksFetchProps) => {
  const [results, setResults] = useState<PipelineWatermarksFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
  });
  const [options] = useState<Options>({
    skip: false,
    requestKey: "",
  });
  const { data, loading, error } = useFetch(
    `/api/v1_1/namespaces/${namespace}/pipelines/${pipeline}/watermarks`,
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
      const result = rawDataToWatermarks(data.data);
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
