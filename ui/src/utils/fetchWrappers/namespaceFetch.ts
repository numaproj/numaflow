import { useContext, useEffect, useState } from "react";
import { useFetch } from "./fetch";
import { getBaseHref } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";

export const useNamespaceFetch = (namespaceId: string | undefined) => {
  const [pipelines, setPipelines] = useState<string[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const { host } = useContext<AppContextProps>(AppContext);

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}/pipelines`
  );

  useEffect(() => {
    if (fetchLoading) {
      setLoading(true);
      return;
    }

    if (error) {
      setLoading(false);
      return;
    }

    if (data) {
      const pipelines = data.map((p: any) => {
        if (p?.metadata?.name) {
          return p.metadata.name;
        }
      });
      setPipelines(pipelines);
      setLoading(false);
      return;
    }
  }, [data, fetchLoading]);

  return { pipelines, error, loading };
};
