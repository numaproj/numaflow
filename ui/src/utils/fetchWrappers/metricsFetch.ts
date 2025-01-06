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
    const fetchData = async () => {
      setIsLoading(true);
      if (filters !== null && Object.keys(filters).length > 0 && metricReq?.dimension !== undefined) {
        try {
          const response = await fetch(urlPath, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ ...metricReq, filters }),
          });
          const data = await response.json();
          if (data?.data === null) {
            setChartData(null);
            setError(data?.errMsg);
          } else {
            setChartData(data?.data);
            setError(null);
          }
        } catch (e) {
          console.error("Error fetching data:", e);
          if (e instanceof Error) {
            setError(e);
          } else {
            setError(null);
          }
        } finally {
          setIsLoading(false);
        }
      } else{
        setIsLoading(false);
      }
    };
    fetchData();
  }, [metricReq, filters]);

  return { chartData, error, isLoading };
};
