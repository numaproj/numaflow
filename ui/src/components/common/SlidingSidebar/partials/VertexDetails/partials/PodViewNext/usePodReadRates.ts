import { useContext, useEffect, useMemo, useState } from "react";
import { AppContext } from "../../../../../../../App";
import { AppContextProps } from "../../../../../../../types/declarations/app";
import { PodReadRates } from "../../../../../../../types/declarations/pods";
import { useMetricsFetch } from "../../../../../../../utils/fetchWrappers/metricsFetch";

export const POD_RATES_REFRESH_MS = 60_000;
/** Only the latest point matters, so a short range keeps the proxy query cheap. */
const RANGE_MS = 3 * 60_000;
const MONO_VERTEX = "monoVertex";

interface PodReadRateIds {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  type: string;
}

/** podRateRequest builds a per-pod read-rate query; metric and pattern names must exist in the proxy ConfigMap. */
export function podRateRequest(
  { namespaceId, pipelineId, vertexId, type }: PodReadRateIds,
  duration: "5m" | "15m",
  now: number
) {
  const isMono = type === MONO_VERTEX;
  return {
    metricReq: {
      metric_name: isMono ? "monovtx_read_total" : "forwarder_data_read_total",
      pattern_name: isMono ? "mono_vertex_throughput" : "vertex_throughput",
      dimension: "pod",
      duration,
      start_time: new Date(now - RANGE_MS).toISOString(),
      end_time: new Date(now).toISOString(),
    },
    filters: isMono
      ? { namespace: namespaceId, mvtx_name: vertexId }
      : { namespace: namespaceId, pipeline: pipelineId, vertex: vertexId },
  };
}

/** latestByPod maps each series' `pod` label to its most recent value. */
export function latestByPod(chartData: any[] | null | undefined): Map<string, number> {
  const byPod = new Map<string, number>();
  (chartData || []).forEach((series) => {
    const pod = series?.metric?.pod;
    const values: [number, string][] = series?.values || [];
    if (!pod || !values.length) return;
    const value = Number(values[values.length - 1][1]);
    if (Number.isFinite(value)) byPod.set(pod, value);
  });
  return byPod;
}

/**
 * usePodReadRates returns per-pod read rates over 5m and 15m windows, refreshed every minute.
 * It returns undefined while metrics charts are disabled, and until both queries succeed with at
 * least one series, so a broken metrics path never makes every pod look idle.
 */
export function usePodReadRates({
  namespaceId,
  pipelineId,
  vertexId,
  type,
}: PodReadRateIds): Map<string, PodReadRates> | undefined {
  const { disableMetricsCharts } = useContext<AppContextProps>(AppContext);
  const enabled = !disableMetricsCharts;
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), POD_RATES_REFRESH_MS);
    return () => clearInterval(timer);
  }, []);

  const ids = { namespaceId, pipelineId, vertexId, type };
  const fiveMin = useMemo(
    () => (enabled ? podRateRequest(ids, "5m", now) : undefined),
    [enabled, namespaceId, pipelineId, vertexId, type, now]
  );
  const fifteenMin = useMemo(
    () => (enabled ? podRateRequest(ids, "15m", now) : undefined),
    [enabled, namespaceId, pipelineId, vertexId, type, now]
  );
  const short = useMetricsFetch({
    metricReq: fiveMin?.metricReq,
    filters: fiveMin?.filters ?? NO_FILTERS,
  });
  const long = useMetricsFetch({
    metricReq: fifteenMin?.metricReq,
    filters: fifteenMin?.filters ?? NO_FILTERS,
  });

  return useMemo(() => {
    if (!enabled || short.error || long.error || !short.chartData || !long.chartData) {
      return undefined;
    }
    const rate5m = latestByPod(short.chartData);
    const rate15m = latestByPod(long.chartData);
    if (!rate15m.size) return undefined;
    const rates = new Map<string, PodReadRates>();
    const record = (_value: number, pod: string) =>
      rates.set(pod, { rate5m: rate5m.get(pod) ?? 0, rate15m: rate15m.get(pod) ?? 0 });
    rate5m.forEach(record);
    rate15m.forEach(record);
    return rates;
  }, [enabled, short.chartData, short.error, long.chartData, long.error]);
}

const NO_FILTERS = {};
