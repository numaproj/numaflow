import { useEffect, useState, useContext } from "react";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";
import { getBaseHref } from "../index";


export interface Filters {
  namespace: string;
  [key: string]: string;
}

export interface useMetricsFetchProps {
  metricName: string;
  dimension: string;
  duration?: string;
  quantile?: string;
  filters: Filters;
}

export const useMetricsFetch = ({
  metricName,
  dimension,
  duration,
  quantile,
  filters
}: useMetricsFetchProps) => {
    const { host } = useContext<AppContextProps>(AppContext);
    const urlPath = `${host}${getBaseHref()}/api/v1/metrics-proxy`;
    const [chartData, setChartData] = useState([]);
    const [error, setError] = useState<Error | null>(null);
    const [shouldFetch, setShouldFetch] = useState(true);
    const [isLoading, setIsLoading] = useState(false);

    useEffect(() => {
      const fetchData = async() => {
        if (!shouldFetch) return;
        setIsLoading(true);
        try{
          const response = await fetch(urlPath, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              metric_name: metricName,
              filters: filters,
              dimension: dimension,
              duration: duration,
              quantile: quantile
            }),
          });
          const data = await response.json();
          setChartData(data?.data[0]?.values);
          setError(null);
        } catch(e){
          console.error("Error fetching data:", e);
          if (e instanceof Error){
            setError(e);
          } else {
            setError(null);
          }
        } finally{
          setIsLoading(false);
          setShouldFetch(false);
        }
      }
      fetchData();
    }, [metricName, dimension, duration, quantile, filters] )

    return {chartData, error, setShouldFetch, isLoading}
}