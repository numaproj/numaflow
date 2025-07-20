import { useEffect, useState, useCallback, useContext } from "react";
import { Options, useFetch } from "./fetch";
import { getBaseHref } from "../index";
import { MonoVertexHealthFetchResult } from "../../types/declarations/pipeline";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";

const DATA_REFRESH_INTERVAL = 15000; // ms

// fetch mono-vertex health status hook
export const useMonoVertexHealthFetch = ({
  namespaceId,
  monoVertexId,
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

  const [results, setResults] = useState<MonoVertexHealthFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
    refresh,
  });

  const { host } = useContext<AppContextProps>(AppContext);

  const {
    data: monoVertexHealthData,
    loading: monoVertexHealthLoading,
    error: monoVertexHealthError,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/mono-vertices/${monoVertexId}/health`,
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
      if (monoVertexHealthLoading) {
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
      if (monoVertexHealthError) {
        if (options?.requestKey === "") {
          // Failed on first load, return error
          setResults({
            data: undefined,
            loading: false,
            error: monoVertexHealthError,
            refresh,
          });
        } else {
          // Failed on refresh, add error to app context
          addError(monoVertexHealthError);
        }
        return;
      }
      if (monoVertexHealthData?.errMsg) {
        if (options?.requestKey === "") {
          // Failed on first load, return error
          setResults({
            data: undefined,
            loading: false,
            error: monoVertexHealthData?.errMsg,
            refresh,
          });
        } else {
          // Failed on refresh, add error to app context
          addError(monoVertexHealthData?.errMsg);
        }
        return;
      }
      if (monoVertexHealthData?.data) {
        setResults({
          data: monoVertexHealthData.data,
          loading: false,
          error: undefined,
          refresh,
        });
        return;
      }
    }
  }, [
    monoVertexHealthData,
    monoVertexHealthLoading,
    monoVertexHealthError,
    pipelineAbleToLoad,
    options,
    refresh,
    addError,
  ]);

  return results;
};
