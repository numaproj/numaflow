import { useEffect, useState, useContext } from "react";
import { AppContextProps } from "../../types/declarations/app";
import { AppContext } from "../../App";
import { getBaseHref } from "../index";
import { Dayjs } from "dayjs";


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
  startTime?: Dayjs | null;
  endTime?: Dayjs | null;
}

export const useMetricsFetch = ({
  metricName,
  dimension,
  duration,
  quantile,
  filters,
  startTime,
  endTime, 
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
              quantile: quantile,
              start_time: startTime ? startTime.toISOString() : undefined,
              end_time: endTime ? endTime.toISOString() : undefined, 
            }),
          });
          const data = await response.json();
          if (data?.data === null){
            setChartData([]);
            setError(data?.errMsg);
          } else {
            setChartData(data?.data[0]?.values);
            setError(null);
          }
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
    }, [metricName, dimension, duration, quantile, filters, startTime, endTime] )

    return {chartData, error, setShouldFetch, isLoading}
}