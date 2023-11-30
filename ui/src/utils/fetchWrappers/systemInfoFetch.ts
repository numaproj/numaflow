import { useEffect, useState, useMemo } from "react";
import { SystemInfo } from "../models/systemInfo";
import { useFetch } from "./fetch";
import { getBaseHref } from "../index";
import { useLocation } from "react-router-dom";

export const useSystemInfoFetch = () => {
  const location = useLocation();
  const [systemInfo, setSystemInfo] = useState<SystemInfo | undefined>(
    undefined
  );
  const [errMsg, setErrMsg] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(true);

  const options = useMemo(
    () => ({ skip: location.pathname === "/login" }),
    [location.pathname]
  );

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(`${getBaseHref()}/api/v1/sysinfo`, undefined, options);

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
