import { useEffect, useState } from "react";
import { NamespaceStatus } from "../models/namespace";
import { useFetch } from "./fetch";

export const useNamespaceStatusFetch = () => {
  const [namespaceStatus, setNamespaceStatus] = useState<NamespaceStatus | undefined>(undefined);
  const [loading, setLoading] = useState<boolean>(true);

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(
    `/api/v1/namespaces/status`
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
      setNamespaceStatus(data);
      setLoading(false);
      return;
    }
  }, [data, fetchLoading]);

  return { namespaceStatus, error, loading };
};
