import { useEffect, useState } from "react";
import { useFetch } from "./fetch";

export const useNamespaceFetch = (namespaceId: string | undefined) => {
  const [pipelines, setPipelines] = useState<string[]>([]);
  const [loading, setLoading] = useState<boolean>(true);

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(`api/v1/namespaces/${namespaceId}/pipelines`);

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
