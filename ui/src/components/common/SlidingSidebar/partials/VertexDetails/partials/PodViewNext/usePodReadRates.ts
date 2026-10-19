import { useContext, useEffect, useRef, useState } from "react";
import { AppContext } from "../../../../../../../App";
import { AppContextProps } from "../../../../../../../types/declarations/app";
import { Pod, PodReadRates } from "../../../../../../../types/declarations/pods";
import { getBaseHref } from "../../../../../../../utils";

export const POD_RATES_REFRESH_MS = 60_000;
const MONO_VERTEX = "monoVertex";

interface PodReadRateIds {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  type: string;
  pods: Pod[] | undefined;
  /** When false, keep the last rates but do not poll. */
  active?: boolean;
}

interface DaemonPodRate {
  hostname?: string;
  rate5m?: number;
  rate15m?: number;
}

/** podReadRatesPath is the API v2 daemon rate endpoint for a MonoVertex or pipeline vertex. */
export function podReadRatesPath({
  namespaceId,
  pipelineId,
  vertexId,
  type,
}: Omit<PodReadRateIds, "pods">): string {
  const namespace = encodeURIComponent(namespaceId);
  if (type === MONO_VERTEX) {
    return `/api/v2/namespaces/${namespace}/mono-vertices/${encodeURIComponent(vertexId)}/pods/read-rates`;
  }
  return `/api/v2/namespaces/${namespace}/pipelines/${encodeURIComponent(pipelineId)}/vertices/${encodeURIComponent(vertexId)}/pods/read-rates`;
}

/**
 * ratesByPodName joins daemon hostnames to Kubernetes pod names.
 * The hostname is a prefix of the pod name, and the trailing hyphen keeps "mv-1" from matching "mv-10".
 * Every pod is present: an unmatched pod gets an empty rate so it is not treated as idle.
 */
export function ratesByPodName(
  pods: Pod[],
  daemonPods: DaemonPodRate[] | null | undefined
): Map<string, PodReadRates> {
  const rates = new Map<string, PodReadRates>();
  pods.forEach((pod) => {
    const match = (daemonPods || []).find(
      (item) => item.hostname && pod.name.startsWith(`${item.hostname}-`)
    );
    rates.set(
      pod.name,
      match ? { rate5m: match.rate5m, rate15m: match.rate15m } : {}
    );
  });
  return rates;
}

/**
 * usePodReadRates returns per-pod read rates over 5m and 15m windows from the vertex daemon,
 * refreshed every minute while the Pods tab is visible. It returns undefined until the request
 * succeeds, and again if the daemon cannot be reached or the vertex changes, so a broken or
 * stale path never makes every pod look idle.
 */
export function usePodReadRates({
  namespaceId,
  pipelineId,
  vertexId,
  type,
  pods,
  active = true,
}: PodReadRateIds): Map<string, PodReadRates> | undefined {
  const { host } = useContext<AppContextProps>(AppContext);
  const [rates, setRates] = useState<Map<string, PodReadRates> | undefined>();
  const podsRef = useRef(pods);
  podsRef.current = pods;
  const podNames = (pods || []).map((pod) => pod.name).join("\n");

  useEffect(() => {
    if (!active) return;
    // Drop the previous vertex's rates immediately so they cannot classify the new fleet.
    setRates(undefined);
    let generation = 0;
    let controller: AbortController | undefined;
    const url = `${host}${getBaseHref()}${podReadRatesPath({
      namespaceId,
      pipelineId,
      vertexId,
      type,
    })}`;

    const load = async () => {
      controller?.abort();
      const current = new AbortController();
      controller = current;
      const request = ++generation;
      try {
        const response = await fetch(url, { signal: current.signal });
        if (current.signal.aborted || request !== generation) return;
        if (!response.ok) {
          setRates(undefined);
          return;
        }
        const body = await response.json();
        if (current.signal.aborted || request !== generation) return;
        setRates(ratesByPodName(podsRef.current || [], body?.pods));
      } catch {
        if (current.signal.aborted || request !== generation) return;
        setRates(undefined);
      }
    };

    void load();
    const timer = window.setInterval(() => void load(), POD_RATES_REFRESH_MS);
    return () => {
      generation += 1;
      controller?.abort();
      window.clearInterval(timer);
    };
  }, [active, host, namespaceId, pipelineId, vertexId, type, podNames]);

  return rates;
}
