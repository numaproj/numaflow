import { useEffect, useState, useCallback, useContext } from "react";
import { Options, useFetch } from "./fetch";
import { getBaseHref } from "../index";
import { ErrorsDetailsFetchResult } from "../../types/declarations/pods";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";

const DATA_REFRESH_INTERVAL = 30000; // ms

// fetch vertex replica errors status
export const useErrorsFetch = ({
  namespaceId,
  pipelineId,
  vertexId,
  type,
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

  const [results, setResults] = useState<ErrorsDetailsFetchResult>({
    data: undefined,
    loading: true,
    error: undefined,
    refresh,
  });

  const { host } = useContext<AppContextProps>(AppContext);

  const {
    data: errorsDetailsData,
    loading: errorsDetailsLoading,
    error: errorsDetailsError,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/${
      type === "monoVertex"
        ? "mono-vertices"
        : `pipelines/${pipelineId}/vertices`
    }/${vertexId}/errors`,
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
    if (errorsDetailsLoading) {
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
    if (errorsDetailsError) {
      if (options?.requestKey === "") {
        // Failed on first load, return error
        setResults({
          data: undefined,
          loading: false,
          error: errorsDetailsError,
          refresh,
        });
      } else {
        // Failed on refresh, add error to app context
        addError(errorsDetailsError);
      }
      return;
    }
    if (errorsDetailsData?.errMsg) {
      if (options?.requestKey === "") {
        // Failed on first load, return error
        setResults({
          data: undefined,
          loading: false,
          error: errorsDetailsData?.errMsg,
          refresh,
        });
      } else {
        // Failed on refresh, add error to app context
        addError(errorsDetailsData?.errMsg);
      }
      return;
    }
    if (errorsDetailsData?.data) {
      setResults({
        data: errorsDetailsData.data,
        loading: false,
        error: undefined,
        refresh,
      });
      return;
    }
  }, [
    errorsDetailsData,
    errorsDetailsLoading,
    errorsDetailsError,
    options,
    refresh,
    addError,
  ]);

  return results;
};
