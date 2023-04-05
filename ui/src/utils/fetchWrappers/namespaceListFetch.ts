import { useEffect, useState } from "react";
import { useFetch } from "./fetch";

export const useNamespaceListFetch = () => {
  const [namespaceList, setNamespaceList] = useState<string[] | undefined>(undefined);
  const [loading, setLoading] = useState<boolean>(true);

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(
    `/api/v1/namespaces`
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
      setNamespaceList(data);
      setLoading(false);
      return;
    }
  }, [data, fetchLoading]);

  return { namespaceList, error, loading };
};
