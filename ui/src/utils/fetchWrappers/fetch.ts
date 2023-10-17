import { useState, useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";

export interface Options {
  skip: boolean;
  requestKey?: string;
}

export const useFetch = (
  url: string,
  fetchOptions?: RequestInit,
  options?: Options
) => {
  const location = useLocation();
  const navigate = useNavigate();
  const [data, setData] = useState<any>(undefined);
  const [error, setError] = useState<any>(undefined);
  const [loading, setLoading] = useState<boolean>(
    (options && !options.skip) || true
  );

  useEffect(() => {
    if (options?.skip) {
      setLoading(false);
      return;
    }

    const fetchData = async () => {
      setLoading(true);
      try {
        const response = await fetch(url, fetchOptions);
        if (!response.ok) {
          if (response.status === 401) {
            // Unauthorized user, redirect to login page
            navigate(`/login?returnUrl=${location.pathname}`);
          } else {
            setError(response.status);
          }
          setLoading(false);
        } else {
          const data = await response.json();
          setError(undefined);
          setData(data);
          setLoading(false);
        }
      } catch (e: any) {
        setError(e.message);
        setLoading(false);
      }
    };

    fetchData();
  }, [url, fetchOptions, options]);

  return { data, error, loading };
};
