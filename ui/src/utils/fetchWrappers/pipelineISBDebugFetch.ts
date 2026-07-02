import {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { AppContext } from "../../App";
import { AppContextProps } from "../../types/declarations/app";
import {
  PipelineISBConsumersResponse,
  PipelineISBDebugFetchResult,
  PipelineISBKVStoresResponse,
  PipelineISBStreamsResponse,
} from "../../types/declarations/pipeline";
import { getBaseHref } from "..";
import { Options, useFetch } from "./fetch";

export interface PipelineISBDebugFetchProps {
  namespaceId?: string;
  pipelineId?: string;
  vertexId?: string;
  from?: string;
  to?: string;
  partition?: number;
  enabled: boolean;
}

export const usePipelineISBDebugFetch = ({
  namespaceId,
  pipelineId,
  vertexId,
  from,
  to,
  partition,
  enabled,
}: PipelineISBDebugFetchProps): PipelineISBDebugFetchResult => {
  const { host } = useContext<AppContextProps>(AppContext);
  const shouldFetch = enabled && !!namespaceId && !!pipelineId;
  const [options, setOptions] = useState<Options>({
    skip: !shouldFetch,
    requestKey: "",
  });
  const [data, setData] = useState<
    | {
        streams?: PipelineISBStreamsResponse;
        consumers?: PipelineISBConsumersResponse;
        kvStores?: PipelineISBKVStoresResponse;
      }
    | undefined
  >();
  const [loading, setLoading] = useState(shouldFetch);
  const [error, setError] = useState<any>(undefined);

  const queryString = useMemo(() => {
    const params = new URLSearchParams();
    if (vertexId) {
      params.set("vertex", vertexId);
    }
    if (from) {
      params.set("from", from);
    }
    if (to) {
      params.set("to", to);
    }
    if (partition !== undefined) {
      params.set("partition", `${partition}`);
    }
    const query = params.toString();
    return query ? `?${query}` : "";
  }, [vertexId, from, to, partition]);

  const baseUrl = `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/isb`;
  const requestIdentity = useMemo(
    () => `${baseUrl}${queryString}`,
    [baseUrl, queryString]
  );
  const requestToken = `${requestIdentity}:${options.requestKey || ""}`;
  const requestTokenRef = useRef(requestToken);

  const refresh = useCallback(() => {
    setData(undefined);
    setError(undefined);
    setLoading(shouldFetch);
    setOptions({
      skip: !shouldFetch,
      requestKey: "id" + Math.random().toString(16).slice(2),
    });
  }, [shouldFetch]);

  const {
    data: streamsData,
    loading: streamsLoading,
    error: streamsError,
  } = useFetch(`${baseUrl}/streams${queryString}`, undefined, options);

  const {
    data: consumersData,
    loading: consumersLoading,
    error: consumersError,
  } = useFetch(`${baseUrl}/consumers${queryString}`, undefined, options);

  const {
    data: kvStoresData,
    loading: kvStoresLoading,
    error: kvStoresError,
  } = useFetch(`${baseUrl}/kv-stores${queryString}`, undefined, options);

  useEffect(() => {
    setOptions((previousOptions) => {
      if (previousOptions.skip === !shouldFetch) {
        return previousOptions;
      }
      return {
        ...previousOptions,
        skip: !shouldFetch,
      };
    });
  }, [shouldFetch]);

  useEffect(() => {
    if (requestTokenRef.current !== requestToken) {
      requestTokenRef.current = requestToken;
      setData(undefined);
      setError(undefined);
      setLoading(shouldFetch);
      return;
    }
    if (!shouldFetch) {
      setLoading(false);
      return;
    }
    if (streamsLoading || consumersLoading || kvStoresLoading) {
      setLoading(true);
      return;
    }
    const fetchError = streamsError || consumersError || kvStoresError;
    const currentStreamsData = !streamsError ? streamsData?.data : undefined;
    const currentConsumersData = !consumersError
      ? consumersData?.data
      : undefined;
    const currentKVStoresData = !kvStoresError ? kvStoresData?.data : undefined;
    const apiError =
      (!streamsError && streamsData?.errMsg) ||
      (!consumersError && consumersData?.errMsg) ||
      (!kvStoresError && kvStoresData?.errMsg);
    const availableData = {
      ...(currentStreamsData ? { streams: currentStreamsData } : {}),
      ...(currentConsumersData ? { consumers: currentConsumersData } : {}),
      ...(currentKVStoresData ? { kvStores: currentKVStoresData } : {}),
    };
    if (Object.keys(availableData).length) {
      setData(availableData);
      setError(fetchError || apiError);
      setLoading(false);
      return;
    }
    if (fetchError || apiError) {
      setData(undefined);
      setError(fetchError || apiError);
      setLoading(false);
      return;
    }
    setLoading(false);
  }, [
    streamsData,
    streamsLoading,
    streamsError,
    consumersData,
    consumersLoading,
    consumersError,
    kvStoresData,
    kvStoresLoading,
    kvStoresError,
    shouldFetch,
    requestToken,
  ]);

  return { data, loading, error, refresh };
};
