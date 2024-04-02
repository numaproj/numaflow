import { useEffect, useState, useCallback, useContext } from "react";
import { Options, useFetch } from "./fetch";
import { PipelineSummaryFetchResult } from "../../types/declarations/pipeline";
import { DEFAULT_ISB, getBaseHref } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";

const DATA_REFRESH_INTERVAL = 15000; // ms

// fetch pipeline summary and ISB summary
export const usePipelineSummaryFetch = ({
  namespaceId,
  pipelineId,
  addError,
}: any) => {
  const [isb, setIsb] = useState<string | null>(null);
  const [options, setOptions] = useState<Options>({
    skip: false,
    requestKey: "",
  });

  const refresh = useCallback(() => {
    setOptions({
      skip: false,
      requestKey: "id" + Math.random().toString(16).slice(2),
    });
  }, []);

  const [results, setResults] = useState<PipelineSummaryFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
    refresh,
  });

  const { host } = useContext<AppContextProps>(AppContext);

  const {
    data: pipelineData,
    loading: pipelineLoading,
    error: pipelineError,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}`,
    undefined,
    options
  );

  const {
    data: isbData,
    loading: isbLoading,
    error: isbError,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/isb-services/${isb}`,
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
          refresh,
        });
      }
      return;
    }
    if (pipelineError || isbError) {
      if (options?.requestKey === "") {
        // Failed on first load, return error
        setResults({
          data: undefined,
          loading: false,
          error: pipelineError || isbError,
          refresh,
        });
      } else {
        // Failed on refresh, add error to app context
        addError(pipelineError || isbError);
      }
      return;
    }
    if (pipelineData?.errMsg || isbData?.errMsg) {
      if (options?.requestKey === "") {
        // Failed on first load, return error
        setResults({
          data: undefined,
          loading: false,
          error: pipelineData?.errMsg || isbData?.errMsg,
          refresh,
        });
      } else {
        // Failed on refresh, add error to app context
        addError(pipelineData?.errMsg || isbData?.errMsg);
      }
      return;
    }
    if (pipelineData) {
      if (pipelineData.data?.pipeline?.spec?.interStepBufferServiceName) {
        setIsb(pipelineData.data?.pipeline?.spec?.interStepBufferServiceName);
      } else {
        setIsb(DEFAULT_ISB);
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
        refresh,
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
    refresh,
    addError,
    isb,
  ]);
  return results;
};
