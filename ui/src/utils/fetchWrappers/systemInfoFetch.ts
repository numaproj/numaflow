import { useEffect, useState } from "react";
import { SystemInfo } from "../models/systemInfo";
import { useFetch } from "./fetch";

export const useSystemInfoFetch = () => {
  const [systemInfo, setSystemInfo] = useState<SystemInfo | undefined>(undefined);
  const [loading, setLoading] = useState<boolean>(true);

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(
    `/api/v1/sysinfo`
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
      setSystemInfo(data);
      setLoading(false);
      return;
    }
  }, [data, fetchLoading]);

  return { systemInfo, error, loading };
};
