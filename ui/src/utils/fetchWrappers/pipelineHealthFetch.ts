import { useEffect, useState, useCallback, useContext } from "react";
import { Options, useFetch } from "./fetch";
import { getBaseHref } from "../index";
import { PipelineHealthFetchResult } from "../../types/declarations/pipeline";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";

const DATA_REFRESH_INTERVAL = 15000; // ms

// fetch pipeline health status hook
// this will help in refreshing health status at Pipeline page
export const usePipelineHealthFetch = ({
  namespaceId,
  pipelineId,
  addError,
  pipelineAbleToLoad = true,
}: any) => {
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

  const [results, setResults] = useState<PipelineHealthFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
    refresh,
  });

  const { host } = useContext<AppContextProps>(AppContext);

  const {
    data: pipelineHealthData,
    loading: pipelineHealthLoading,
    error: pipelineHealthError,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/health`,
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
    if (pipelineAbleToLoad) {
      if (pipelineHealthLoading) {
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
      if (pipelineHealthError) {
        if (options?.requestKey === "") {
          // Failed on first load, return error
          setResults({
            data: undefined,
            loading: false,
            error: pipelineHealthError,
            refresh,
          });
        } else {
          // Failed on refresh, add error to app context
          addError(pipelineHealthError);
        }
        return;
      }
      if (pipelineHealthData?.errMsg) {
        if (options?.requestKey === "") {
          // Failed on first load, return error
          setResults({
            data: undefined,
            loading: false,
            error: pipelineHealthData?.errMsg,
            refresh,
          });
        } else {
          // Failed on refresh, add error to app context
          addError(pipelineHealthData?.errMsg);
        }
        return;
      }
      if (pipelineHealthData?.data) {
        setResults({
          data: pipelineHealthData.data,
          loading: false,
          error: undefined,
          refresh,
        });
        return;
      }
    }
  }, [
    pipelineHealthData,
    pipelineHealthLoading,
    pipelineHealthError,
    pipelineAbleToLoad,
    options,
    refresh,
    addError,
  ]);

  return results;
};
