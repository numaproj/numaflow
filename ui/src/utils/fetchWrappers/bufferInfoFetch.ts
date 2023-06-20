import { useEffect, useState } from "react";
import { BufferInfo } from "../models/pipeline";
import { useFetch } from "./fetch";

export const useBuffersInfoFetch = (
  namespaceId: string | undefined,
  pipelineId: string | undefined,
  requestKey: string
) => {
  const [buffersInfo, setBuffersInfo] = useState<BufferInfo[]>([]);
  const [loading, setLoading] = useState<boolean>(true);

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(
    `/api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}/buffers?refreshKey=${requestKey}`
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
      setBuffersInfo(data);
      setLoading(false);
      return;
    }
  }, [data, fetchLoading]);

  return { buffersInfo, error, loading };
};
