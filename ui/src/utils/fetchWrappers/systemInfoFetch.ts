import { useEffect, useState } from "react";
import { SystemInfo } from "../models/systemInfo";
import { useFetch } from "./fetch";

export const useSystemInfoFetch = () => {
  const [systemInfo, setSystemInfo] = useState<SystemInfo | undefined>(
    undefined
  );
  const [errMsg, setErrMsg] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(true);

  const { data, loading: fetchLoading, error } = useFetch(`/api/v1/sysinfo`);

  useEffect(() => {
    setLoading(fetchLoading);
    if (error) {
      setErrMsg("Failed to fetch the system info");
      return;
    }
    if (data?.errMsg) {
      setErrMsg(data.errMsg);
      return;
    }
    if (data) {
      setSystemInfo(data?.data);
      return;
    }
  }, [data, fetchLoading]);

  return { systemInfo, error: errMsg, loading };
};
