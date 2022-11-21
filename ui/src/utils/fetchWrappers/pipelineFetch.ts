import { useEffect, useState } from "react";
import { Pipeline } from "../models/pipeline";
import { useFetch } from "./fetch";

export const usePipelineFetch = (
  namespaceId: string | undefined,
  pipelineId: string | undefined,
  requestKey: string
) => {
  const [pipeline, setPipeline] = useState<Pipeline | undefined>(undefined);
  const [loading, setLoading] = useState<boolean>(true);

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(
    `api/v1/namespaces/${namespaceId}/pipelines/${pipelineId}?refreshKey=${requestKey}`
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
      setPipeline(data);
      setLoading(false);
      return;
    }
  }, [data, fetchLoading]);

  return { pipeline, error, loading };
};
