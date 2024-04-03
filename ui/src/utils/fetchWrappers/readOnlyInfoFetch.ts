import { useEffect, useState, useMemo } from "react";
import { useLocation } from "react-router-dom";
import { ReadOnlyInfo } from "../models/readOnlyInfo";
import { useFetch } from "./fetch";
import { getBaseHref } from "../index";

export interface IsReadOnlyProps {
  host: string;
}

export const useReadOnlyInfoFetch = (props: IsReadOnlyProps) => {
  const location = useLocation();
  const [readOnlyInfo, setReadOnlyInfo] = useState<ReadOnlyInfo | undefined>(
    undefined
  );
  const [errMsg, setErrMsg] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(true);

  const options = useMemo(
    () => ({ skip: location.pathname === "/login" }),
    [location.pathname]
  );
  const { host } = props;

  const {
    data,
    loading: fetchLoading,
    error,
  } = useFetch(
    `${host}${getBaseHref()}/api/v1/readonlyinfo`,
    undefined,
    options
  );

  useEffect(() => {
    setLoading(fetchLoading);
    if (error) {
      setErrMsg("Failed to fetch the read only info");
      return;
    }
    if (data?.errMsg) {
      setErrMsg(data.errMsg);
      return;
    }
    if (data) {
      setReadOnlyInfo(data?.data);
      return;
    }
  }, [data, fetchLoading]);

  return { readOnlyInfo, error: errMsg, loading };
};
