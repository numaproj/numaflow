import { useCallback, useContext, useEffect, useState } from "react";
import { AppContext } from "../../App";
import { AppContextProps } from "../../types/declarations/app";
import {
  ISBJetStreamResponse,
  ISBServiceDebugFetchResult,
} from "../../types/declarations/pipeline";
import { getBaseHref } from "..";

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
  const [requestKey, setRequestKey] = useState("");
  const [data, setData] = useState<
    | {
        jetStream: ISBJetStreamResponse;
      }
    | undefined
  >();
  const [loading, setLoading] = useState(enabled);
  const [error, setError] = useState<any>(undefined);

  const refresh = useCallback(() => {
    setRequestKey("id" + Math.random().toString(16).slice(2));
  }, []);

  useEffect(() => {
    if (!enabled || !namespaceId || !isbId) {
      setLoading(false);
      setData(undefined);
      setError(undefined);
      return;
    }

    let cancelled = false;
    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/isb-services/${isbId}/jetstream`
        );
        const responseJSON = await response.json();
        const responseError = responseJSON?.errMsg;
        if (!response.ok || responseError) {
          throw new Error(responseError || `Response code: ${response.status}`);
        }
        if (!cancelled) {
          setData({
            jetStream: responseJSON?.data,
          });
          setError(undefined);
        }
      } catch (e: any) {
        if (!cancelled) {
          setError(e.message);
          setData(undefined);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    fetchData();
    return () => {
      cancelled = true;
    };
  }, [enabled, namespaceId, isbId, host, requestKey]);

  return { data, loading, error, refresh };
};
