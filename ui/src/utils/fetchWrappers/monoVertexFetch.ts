import { useEffect, useState, useCallback, useContext } from "react";
import { Options, useFetch } from "./fetch";
import { MonoVertexSummaryFetchResult } from "../../types/declarations/pipeline";
import { getBaseHref } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";

const DATA_REFRESH_INTERVAL = 15000; // ms

// fetch monoVertex summary
export const useMonoVertexSummaryFetch = ({
  namespaceId,
  pipelineId,
  addError,
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

  const [results, setResults] = useState<MonoVertexSummaryFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
    refresh,
  });

  const { host } = useContext<AppContextProps>(AppContext);

  const {
    data: monoVertexData,
    loading: monoVertexLoading,
    error: monoVertexError,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/mono-vertices/${pipelineId}`,
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
    if (monoVertexLoading) {
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
    if (monoVertexError) {
      if (options?.requestKey === "") {
        // Failed on first load, return error
        setResults({
          data: undefined,
          loading: false,
          error: monoVertexError,
          refresh,
        });
      } else {
        // Failed on refresh, add error to app context
        addError(monoVertexError);
      }
      return;
    }
    if (monoVertexData?.errMsg) {
      if (options?.requestKey === "") {
        // Failed on first load, return error
        setResults({
          data: undefined,
          loading: false,
          error: monoVertexData?.errMsg,
          refresh,
        });
      } else {
        // Failed on refresh, add error to app context
        addError(monoVertexData?.errMsg);
      }
      return;
    }
    if (monoVertexData) {
      const monoVertexSummary = {
        monoVertexData: monoVertexData?.data,
      };
      setResults({
        data: monoVertexSummary,
        loading: false,
        error: undefined,
        refresh,
      });
      return;
    }
  }, [
    monoVertexData,
    monoVertexLoading,
    monoVertexError,
    options,
    refresh,
    addError,
  ]);
  return results;
};
