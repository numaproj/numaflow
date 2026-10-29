import { useContext, useEffect, useMemo, useState } from "react";
import { AppContext } from "../../../../../../../App";
import { AppContextProps } from "../../../../../../../types/declarations/app";
import { getBaseHref } from "../../../../../../../utils";
import { useMetricsFetch } from "../../../../../../../utils/fetchWrappers/metricsFetch";
import {
  DataHealth,
  MONO_VERTEX,
  VertexScaleStatus,
  SignalQueries,
  SignalQuery,
  SignalSeries,
  WatermarkEdge,
  seriesValues,
  signalQueries,
  splitDropSeries,
} from "./goldenSignalsModel";

export const HEALTH_REFRESH_MS = 60_000;
const WINDOW_MS = 30 * 60_000;

interface VertexHealthDataProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  type: string;
  /** When false, the hidden Pod View does not issue metrics or health requests. */
  active: boolean;
}

export interface VertexHealthData {
  queries: SignalQueries;
  series: SignalSeries;
  health?: DataHealth;
  scaleStatus?: VertexScaleStatus;
  watermarkEdges?: WatermarkEdge[];
  metricsEnabled: boolean;
  now: number;
}

const NO_FILTERS = {};

/** useSeries returns `undefined` before the first response, `null` on a proxy error, else summed values. */
function useSeries(
  query: SignalQuery | undefined,
  range: { start: string; end: string },
  enabled: boolean
): number[] | null | undefined {
  const metricReq = useMemo(
    () =>
      query && enabled
        ? {
            metric_name: query.metric_name,
            pattern_name: query.pattern_name,
            dimension: query.dimension,
            quantile: query.quantile,
            duration: query.duration,
            start_time: range.start,
            end_time: range.end,
          }
        : undefined,
    [query, enabled, range]
  );
  const filters = query && enabled ? query.filters : NO_FILTERS;
  const { chartData, error } = useMetricsFetch({ metricReq, filters });
  const values = useMemo(
    () => (chartData === null ? undefined : seriesValues(chartData)),
    [chartData]
  );
  if (!metricReq) return undefined;
  return error ? null : values;
}

/** useDropRates keeps duplicate drops separate from other runtime drop reasons. */
function useDropRates(
  query: SignalQuery | undefined,
  range: { start: string; end: string },
  enabled: boolean,
  isMono: boolean
): { loss: number[] | null | undefined; duplicate: number[] | null | undefined } {
  const metricReq = useMemo(
    () =>
      query && enabled
        ? {
            metric_name: query.metric_name,
            pattern_name: query.pattern_name,
            dimension: query.dimension,
            quantile: query.quantile,
            duration: query.duration,
            start_time: range.start,
            end_time: range.end,
          }
        : undefined,
    [query, enabled, range]
  );
  const filters = query && enabled ? query.filters : NO_FILTERS;
  const { chartData, error } = useMetricsFetch({ metricReq, filters });
  const split = useMemo(
    () => (chartData === null ? undefined : splitDropSeries(chartData, isMono)),
    [chartData, isMono]
  );
  if (!metricReq) return { loss: undefined, duplicate: undefined };
  if (error) return { loss: null, duplicate: null };
  return { loss: split?.loss, duplicate: split?.duplicate };
}

async function fetchData<T>(url: string, signal: AbortSignal): Promise<T | undefined> {
  const response = await fetch(url, { signal });
  if (!response.ok) return undefined;
  const body = await response.json();
  return body?.errMsg ? undefined : body?.data;
}

async function fetchJson<T>(url: string, signal: AbortSignal): Promise<T | undefined> {
  const response = await fetch(url, { signal });
  if (!response.ok) return undefined;
  return response.json();
}

interface PipelineVertexStatus {
  replicas?: { ready?: number; desired?: number };
}

interface PipelineVertexSummary {
  lastScaledAt?: string;
}

/** useVertexHealthData loads the strip's live inputs and refreshes them every minute. */
export function useVertexHealthData({
  namespaceId,
  pipelineId,
  vertexId,
  type,
  active,
}: VertexHealthDataProps): VertexHealthData {
  const { host, disableMetricsCharts } = useContext<AppContextProps>(AppContext);
  const metricsEnabled = !disableMetricsCharts;
  const isMono = type === MONO_VERTEX;
  const [now, setNow] = useState(() => Date.now());
  const [health, setHealth] = useState<DataHealth>();
  const [scaleStatus, setScaleStatus] = useState<VertexScaleStatus>();
  const [watermarkEdges, setWatermarkEdges] = useState<WatermarkEdge[]>();
  const [fetchedAt, setFetchedAt] = useState<number>();

  useEffect(() => {
    if (!active) return undefined;
    const timer = setInterval(() => setNow(Date.now()), HEALTH_REFRESH_MS);
    return () => clearInterval(timer);
  }, [active]);

  const queries = useMemo(
    () => signalQueries({ namespaceId, pipelineId, vertexId, type }),
    [namespaceId, pipelineId, vertexId, type]
  );
  const range = useMemo(
    () => ({
      start: new Date(now - WINDOW_MS).toISOString(),
      end: new Date(now).toISOString(),
    }),
    [now]
  );

  const fetchMetrics = metricsEnabled && active;
  const p99 = useSeries(queries.p99, range, fetchMetrics);
  const stageRead = useSeries(queries.stageRead, range, fetchMetrics);
  const stageTransformer = useSeries(queries.stageTransformer, range, fetchMetrics);
  const stageUdf = useSeries(queries.stageUdf, range, fetchMetrics);
  const stageSink = useSeries(queries.stageSink, range, fetchMetrics);
  const stageWrite = useSeries(queries.stageWrite, range, fetchMetrics);
  const stageAck = useSeries(queries.stageAck, range, fetchMetrics);
  const dropRates = useDropRates(queries.drops, range, fetchMetrics, isMono);
  const nacks = useSeries(queries.nacks, range, fetchMetrics);
  const fallback = useSeries(queries.fallback, range, fetchMetrics);
  const critical = useSeries(queries.critical, range, fetchMetrics);
  const series = useMemo<SignalSeries>(
    () => ({
      p99,
      stageRead,
      stageTransformer,
      stageUdf,
      stageSink,
      stageWrite,
      stageAck,
      drops: dropRates.loss,
      duplicateDrops: dropRates.duplicate,
      nacks,
      fallback,
      critical,
    }),
    [
      p99,
      stageRead,
      stageTransformer,
      stageUdf,
      stageSink,
      stageWrite,
      stageAck,
      dropRates,
      nacks,
      fallback,
      critical,
    ]
  );

  useEffect(() => {
    if (!active) return undefined;
    const controller = new AbortController();
    const base = `${host}${getBaseHref()}/api/v1/namespaces/${namespaceId}`;
    const load = async () => {
      const vertexBase = `${host}${getBaseHref()}/api/v2/namespaces/${namespaceId}/pipelines/${pipelineId}/vertices/${vertexId}`;
      const [healthData, vertexData, watermarks, pipelineStatus, pipelineSummary] =
        await Promise.allSettled([
          fetchData<DataHealth>(
            isMono ? `${base}/mono-vertices/${vertexId}/health` : `${base}/pipelines/${pipelineId}/health`,
            controller.signal
          ),
          isMono
            ? fetchData<any>(`${base}/mono-vertices/${vertexId}`, controller.signal)
            : Promise.resolve(undefined),
          isMono
            ? Promise.resolve(undefined)
            : fetchData<WatermarkEdge[]>(`${base}/pipelines/${pipelineId}/watermarks`, controller.signal),
          isMono
            ? Promise.resolve(undefined)
            : fetchJson<PipelineVertexStatus>(`${vertexBase}/status`, controller.signal),
          isMono
            ? Promise.resolve(undefined)
            : fetchJson<PipelineVertexSummary>(`${vertexBase}/summary`, controller.signal),
        ]);
      if (controller.signal.aborted) return;
      setHealth(healthData.status === "fulfilled" ? healthData.value : undefined);
      if (isMono) {
        setScaleStatus(
          vertexData.status === "fulfilled" ? vertexData.value?.monoVertex?.status : undefined
        );
      } else {
        const status = pipelineStatus.status === "fulfilled" ? pipelineStatus.value : undefined;
        const summary = pipelineSummary.status === "fulfilled" ? pipelineSummary.value : undefined;
        const desired = status?.replicas?.desired;
        setScaleStatus(
          desired === undefined
            ? undefined
            : {
                desiredReplicas: desired,
                readyReplicas: status?.replicas?.ready,
                lastScaledAt: summary?.lastScaledAt,
              }
        );
      }
      setWatermarkEdges(watermarks.status === "fulfilled" ? watermarks.value : undefined);
      setFetchedAt(Date.now());
    };
    void load();
    return () => controller.abort();
  }, [active, host, isMono, namespaceId, pipelineId, vertexId, now]);

  // Watermark lag must be measured against the response time, not the earlier refresh tick.
  return {
    queries,
    series,
    health,
    scaleStatus,
    watermarkEdges,
    metricsEnabled,
    now: fetchedAt ?? now,
  };
}
