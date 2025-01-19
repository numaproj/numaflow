import { useContext, useEffect, useState } from "react";
import { getBaseHref } from "../index";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";

export interface MetricsDiscoveryDataProps {
  objectType: string;
}

export interface MetricsDiscoveryInfo {
  data: any;
}

export const useMetricsDiscoveryDataFetch = (
  props: MetricsDiscoveryDataProps
) => {
  const [metricsDiscoveryData, setMetricsDiscoveryData] = useState<
    MetricsDiscoveryInfo | undefined
  >(undefined);
  const [errMsg, setErrMsg] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(true);

  const { host } = useContext<AppContextProps>(AppContext);
  const { objectType } = props;

  useEffect(() => {
    const getMetricsDiscoveryData = async () => {
      try {
        setLoading(true);
        const response = await fetch(
          `${host}${getBaseHref()}/api/v1/metrics-discovery/object/${objectType}`
        );
        if (!response.ok) {
          const errMsg = `Failed to discover metrics for the ${objectType}. Response code: ${response.status}`;
          setErrMsg(errMsg);
          setLoading(false);
        }
        const data = await response.json();
        setMetricsDiscoveryData(data);
        setLoading(false);
      } catch (e: any) {
        setErrMsg(e.message);
        setLoading(false);
      }
    };

    getMetricsDiscoveryData();
  }, []);

  return { metricsDiscoveryData, error: errMsg, loading };
};
