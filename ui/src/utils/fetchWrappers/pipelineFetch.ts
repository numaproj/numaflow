import { useEffect, useState } from "react";
import { Options, useFetch } from "./fetch";
import {PipelineSummaryFetchResult} from "../../types/declarations/pipeline";


const DATA_REFRESH_INTERVAL = 15000; // ms

// fetch pipeline summary and ISB summary
export const usePipelineSummaryFetch = ({ namespaceId, pipelineId }: any) => {
  const [results, setResults] = useState<PipelineSummaryFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
  });
  const [isb, setIsb] = useState<string | null>(null);
  const [options, setOptions] = useState<Options>({
    skip: false,
    requestKey: "",
  });
  const {
    data: pipelineData,
    loading: pipelineLoading,
    error: pipelineError,
  } = useFetch(
    `/api/v1_1/namespaces/${namespaceId}/pipelines/${pipelineId}`,
    undefined,
    options
  );

  const {
    data: isbData,
    loading: isbLoading,
    error: isbError,
  } = useFetch(
    `/api/v1_1/namespaces/${namespaceId}/isb-services/${isb}`,
    undefined,
    isb ? options : { ...options, skip: true }
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
      if (options?.requestKey === "") {
        setResults({
          data: undefined,
          loading: true,
          error: undefined,
        });
      }
      return;
    }
    if (pipelineError || isbError) {
      setResults({
        data: undefined,
        loading: false,
        error: pipelineError || isbError,
      });
      return;
    }
    if (pipelineData?.errMsg || isbData?.errMsg) {
      setResults({
        data: undefined,
        loading: false,
        error: pipelineData?.errMsg || isbData?.errMsg,
      });
      return;
    }
    if (pipelineData) {
      if (pipelineData.data?.data?.pipeline?.spec?.interStepBufferServiceName) {
        setIsb(
          pipelineData.data?.data?.pipeline?.spec?.interStepBufferServiceName
        );
      } else {
        setIsb("default");
      }
    }
    if (pipelineData && isbData) {
      const pipelineSummary = {
        pipelineData: pipelineData?.data,
        isbData: isbData?.data,
      };
      setResults({
        data: pipelineSummary,
        loading: false,
        error: undefined,
      });
      return;
    }
  }, [
    pipelineData,
    isbData,
    pipelineLoading,
    isbLoading,
    pipelineError,
    isbError,
    options,
  ]);
  return results;
};
