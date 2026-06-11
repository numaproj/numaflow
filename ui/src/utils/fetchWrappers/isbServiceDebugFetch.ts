import { useCallback, useContext, useEffect, useState } from "react";
import { AppContext } from "../../App";
import { AppContextProps } from "../../types/declarations/app";
import {
  ISBJetStreamResponse,
  ISBServiceDebugFetchResult,
} from "../../types/declarations/pipeline";
import { getBaseHref } from "..";
import { Options, useFetch } from "./fetch";

export const useISBServiceDebugFetch = ({
  namespaceId,
  isbId,
  enabled,
}: {
  namespaceId?: string;
  isbId?: string;
  enabled: boolean;
}): ISBServiceDebugFetchResult => {
  const { host } = useContext<AppContextProps>(AppContext);
  const shouldFetch = enabled && !!namespaceId && !!isbId;
  const [options, setOptions] = useState<Options>({
    skip: !shouldFetch,
    requestKey: "",
  });
  const [data, setData] = useState<
    | {
        jetStream: ISBJetStreamResponse;
      }
    | undefined
  >();
  const [loading, setLoading] = useState(shouldFetch);
  const [error, setError] = useState<any>(undefined);

  const refresh = useCallback(() => {
    setOptions({
      skip: !shouldFetch,
      requestKey: "id" + Math.random().toString(16).slice(2),
    });
  }, [shouldFetch]);

  const {
    data: jetStreamData,
    loading: jetStreamLoading,
    error: jetStreamError,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/isb-services/${isbId}/jetstream`,
    undefined,
    options
  );

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
    if (!shouldFetch) {
      setData(undefined);
      setError(undefined);
      setLoading(false);
    }
  }, [shouldFetch]);

  useEffect(() => {
    if (!shouldFetch) {
      return;
    }
    if (jetStreamLoading) {
      setLoading(true);
      return;
    }
    if (jetStreamError) {
      setData(undefined);
      setError(jetStreamError);
      setLoading(false);
      return;
    }
    if (jetStreamData?.errMsg) {
      setData(undefined);
      setError(jetStreamData.errMsg);
      setLoading(false);
      return;
    }
    if (jetStreamData?.data) {
      setData({
        jetStream: jetStreamData.data,
      });
      setError(undefined);
      setLoading(false);
      return;
    }
    setLoading(false);
  }, [jetStreamData, jetStreamLoading, jetStreamError, shouldFetch]);

  return { data, loading, error, refresh };
};
