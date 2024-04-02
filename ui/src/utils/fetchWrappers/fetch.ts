import { useState, useEffect } from "react";
import { useLocation, useHistory } from "react-router-dom";

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
  const history = useHistory();
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
            // Unauthenticated user, redirect to login page
            history.push(`/login?returnUrl=${location.pathname}`);
          } else if (response.status === 403) {
            // Unauthorized user, display given or default error message
            const data = await response.json();
            if (data.errMsg) {
              setError(`Error: ${data.errMsg}`);
            } else {
              setError(
                `Error: user is not authorized to execute the requested action.`
              );
            }
          } else {
            setError(`Response code: ${response.status}`);
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
