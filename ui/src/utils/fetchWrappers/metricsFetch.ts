import { useEffect, useState, useContext } from "react";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";
import { getBaseHref } from "../index";

export interface useMetricsFetchProps {
  metricReq: any;
  filters: any;
}

export const useMetricsFetch = ({
  metricReq,
  filters,
}: useMetricsFetchProps) => {
  const { host } = useContext<AppContextProps>(AppContext);
  const urlPath = `${host}${getBaseHref()}/api/v1/metrics-proxy`;
  const [chartData, setChartData] = useState<any[] | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(false);

  useEffect(() => {
    const controller = new AbortController();

    const fetchData = async () => {
      setIsLoading(true);
      if (
        Object.keys(filters).length > 0 &&
        metricReq?.dimension !== undefined
      ) {
        try {
          const response = await fetch(urlPath, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              ...metricReq,
              filters,
              display_name: undefined,
            }),
            signal: controller.signal,
          });
          const data = await response.json();
          if (controller.signal.aborted) return;
          if (data?.data === null) {
            setChartData(null);
            setError(data?.errMsg);
          } else {
            setChartData(data?.data);
            setError(null);
          }
        } catch (e) {
          if (controller.signal.aborted) return;
          console.error("Error fetching data:", e);
          if (e instanceof Error) {
            setError(e);
          } else {
            setError(null);
          }
        } finally {
          if (!controller.signal.aborted) {
            setIsLoading(false);
          }
        }
      } else {
        setIsLoading(false);
      }
    };
    void fetchData();

    return () => {
      controller.abort();
    };
  }, [metricReq, filters, urlPath]);

  return { chartData, error, isLoading };
};
