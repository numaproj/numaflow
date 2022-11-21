import { useEffect, useState } from "react";
import { EdgeInfo } from "../models/pipeline";
import { useFetch } from "./fetch";

export const useEdgesInfoFetch = (
  namespaceId: string | undefined,
  pipelineId: string | undefined,
  requestKey: string
) => {
  const [edgesInfo, setEdgesInfo] = useState<EdgeInfo[]>([]);
  const [loading, setLoading] = useState<boolean>(true);

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(
    `api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/edges?refreshKey=${requestKey}`
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
      setEdgesInfo(data);
      setLoading(false);
      return;
    }
  }, [data, fetchLoading]);

  return { edgesInfo, error, loading };
};
