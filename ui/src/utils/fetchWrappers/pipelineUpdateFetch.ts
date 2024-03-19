import { useContext, useEffect, useState } from "react";
import { Options, useFetch } from "./fetch";
import { getBaseHref } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";
import { PipelineUpdateFetchResult } from "../../types/declarations/pipeline";

const DATA_REFRESH_INTERVAL = 1000; // ms

// fetch pipeline to check for existence
export const usePipelineUpdateFetch = ({
  namespaceId,
  pipelineId,
  active,
  refreshInterval = DATA_REFRESH_INTERVAL,
}: any) => {
  const [options, setOptions] = useState<Options>({
    skip: !active,
    requestKey: "",
  });

  const [results, setResults] = useState<PipelineUpdateFetchResult>({
    pipelineAvailable: false,
  });
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [intervalId, setIntervalId] = useState<any>();

  const { host } = useContext<AppContextProps>(AppContext);

  const { data, loading, error } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}`,
    undefined,
    options
  );

  useEffect(() => {
    if (!active) {
      // Clear any existing interval running
      setIntervalId((prev: any) => {
        if (prev) {
          clearInterval(prev);
        }
        return undefined;
      });
      return;
    }
    // Set periodic interval to refresh data
    const id = setInterval(() => {
      setOptions({
        skip: false,
        requestKey: "id" + Math.random().toString(16).slice(2),
      });
    }, refreshInterval);
    // Clear any existing interval running and store new one
    setIntervalId((prev: any) => {
      if (prev) {
        clearInterval(prev);
      }
      return id;
    });
    return () => {
      // Clear interval on unmount
      clearInterval(id);
    };
  }, [active, refreshInterval]);

  useEffect(() => {
    if (loading) {
      if (options?.requestKey === "") {
        // Only set false when its the first load. Keep existing result otherwise.
        setResults({
          pipelineAvailable: false,
        });
      }
      return;
    }
    if (error || data?.errMsg) {
      setResults({
        pipelineAvailable: false,
      });
      return;
    }
    if (data?.data) {
      setResults({
        pipelineAvailable: true,
      });
      return;
    }
  }, [data, loading, error, options]);

  return results;
};
