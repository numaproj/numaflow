import { useEffect, useMemo, useState } from "react";
import { useLocation } from "react-router-dom";
import { ControllerInfo } from "../models/controllerInfo";
import { getBaseHref } from "../index";

export interface ControllerInfoFetchProps {
  host: string;
  // Preferred namespace from the URL (current UI context).
  namespace?: string;
  // From /sysinfo — used when the controller lives in the install ns (cluster mode).
  managedNamespace?: string;
  namespaced?: boolean;
}

async function fetchControllerInfo(
  host: string,
  namespace: string
): Promise<ControllerInfo | undefined> {
  const response = await fetch(
    `${host}${getBaseHref()}/api/v1/namespaces/${namespace}/controller-info`
  );
  if (!response.ok) {
    throw new Error(`Response code: ${response.status}`);
  }
  const body = await response.json();
  if (body?.errMsg) {
    throw new Error(body.errMsg);
  }
  return body?.data as ControllerInfo | undefined;
}

export const useControllerInfoFetch = (props: ControllerInfoFetchProps) => {
  const location = useLocation();
  const { host, namespace, managedNamespace, namespaced } = props;
  const [controllerInfo, setControllerInfo] = useState<
    ControllerInfo | undefined
  >(undefined);
  const [error, setError] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(true);

  // Boolean skip so navigating between non-login routes does not re-fetch.
  const skip = useMemo(
    () => location.pathname === "/login",
    [location.pathname]
  );

  useEffect(() => {
    if (skip) {
      setLoading(false);
      return;
    }

    const targetNs = namespace || managedNamespace;
    if (!targetNs) {
      setControllerInfo(undefined);
      setLoading(false);
      return;
    }

    let cancelled = false;

    const load = async () => {
      setLoading(true);
      setError("");
      try {
        let info = await fetchControllerInfo(host, targetNs);

        // Cluster-scoped installs keep the controller in managedNamespace (often
        // numaflow-system). When browsing another namespace, retry that install ns
        // so Version Details still shows controller version/scope.
        if (
          info &&
          !info.found &&
          namespaced === false &&
          managedNamespace &&
          managedNamespace !== targetNs
        ) {
          info = await fetchControllerInfo(host, managedNamespace);
        }

        if (!cancelled) {
          setControllerInfo(info);
          setLoading(false);
        }
      } catch (e: any) {
        if (!cancelled) {
          setError(e?.message || "Failed to fetch the controller info");
          setControllerInfo(undefined);
          setLoading(false);
        }
      }
    };

    load();
    return () => {
      cancelled = true;
    };
  }, [host, namespace, managedNamespace, namespaced, skip]);

  return { controllerInfo, error, loading };
};
