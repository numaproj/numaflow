import {
  HealthCard,
  InspectTarget,
  PodFleetItem,
  SignalTone,
  VertexHealth,
} from "../../../../../../../types/declarations/pods";
import { BufferInfo } from "../../../../../../../types/declarations/pipeline";

export const MONO_VERTEX = "monoVertex";

/** SignalQueryKey names every Prometheus series the strip can request; keys stay fixed so hooks run in order. */
export const SIGNAL_QUERY_KEYS = [
  "p99",
  "stageRead",
  "stageTransformer",
  "stageUdf",
  "stageSink",
  "stageWrite",
  "stageAck",
  "readRate",
  "outRate",
  "pending",
  "drops",
  "nacks",
  "fallback",
  "critical",
] as const;

export type SignalQueryKey = (typeof SIGNAL_QUERY_KEYS)[number];

export interface SignalQuery {
  metric_name: string;
  pattern_name: string;
  dimension: string;
  quantile?: string;
  duration?: string;
  filters: Record<string, string>;
}

export type SignalQueries = Partial<Record<SignalQueryKey, SignalQuery>>;

/** SignalSeries holds one value per step; `null` means the request failed, `[]` means no samples. */
export type SignalSeries = Partial<Record<SignalQueryKey, number[] | null>>;

const STAGE_LABELS: Partial<Record<SignalQueryKey, string>> = {
  stageRead: "read",
  stageTransformer: "transformer",
  stageUdf: "UDF",
  stageSink: "sink",
  stageWrite: "write",
  stageAck: "ack",
};

interface VertexIds {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  type: string;
}

/** signalQueries returns the metrics-proxy requests for a vertex; names must exist in the proxy ConfigMap. */
export function signalQueries({
  namespaceId,
  pipelineId,
  vertexId,
  type,
}: VertexIds): SignalQueries {
  const isMono = type === MONO_VERTEX;
  const filters: Record<string, string> = isMono
    ? { namespace: namespaceId, mvtx_name: vertexId }
    : { namespace: namespaceId, pipeline: pipelineId, vertex: vertexId };
  const dimension = isMono ? "mono-vertex" : "vertex";
  const histogram = (metric_name: string): SignalQuery => ({
    metric_name,
    pattern_name: isMono ? "mono_vertex_histogram" : "vertex_histogram",
    dimension,
    quantile: "0.99",
    duration: "5m",
    filters,
  });
  const rate = (metric_name: string): SignalQuery => ({
    metric_name,
    pattern_name: isMono ? "mono_vertex_throughput" : "vertex_throughput",
    dimension,
    duration: "1m",
    filters,
  });
  const pending: SignalQuery = {
    metric_name: isMono ? "monovtx_pending" : "vertex_pending_messages",
    pattern_name: isMono ? "mono_vertex_gauge" : "vertex_gauge",
    dimension,
    filters: { ...filters, period: "default" },
  };

  if (isMono) {
    return {
      p99: histogram("monovtx_processing_time_bucket"),
      stageRead: histogram("monovtx_read_time_bucket"),
      stageTransformer: histogram("monovtx_transformer_time_bucket"),
      stageUdf: histogram("monovtx_udf_time_bucket"),
      stageSink: histogram("monovtx_sink_time_bucket"),
      stageAck: histogram("monovtx_ack_time_bucket"),
      readRate: rate("monovtx_read_total"),
      outRate: rate("monovtx_sink_write_total"),
      pending,
      drops: rate("monovtx_dropped_total"),
      nacks: rate("monovtx_nack_total"),
      fallback: rate("monovtx_fallback_sink_write_total"),
      critical: rate("monovtx_critical_error_total"),
    };
  }
  return {
    p99: histogram("forwarder_processing_time_bucket"),
    stageRead: histogram("forwarder_read_processing_time_bucket"),
    stageUdf: histogram("forwarder_udf_processing_time_bucket"),
    stageWrite: histogram("forwarder_write_processing_time_bucket"),
    stageAck: histogram("forwarder_ack_processing_time_bucket"),
    readRate: rate("forwarder_data_read_total"),
    outRate: rate("forwarder_write_total"),
    pending,
    drops: rate("forwarder_drop_total"),
    nacks: rate("forwarder_nack_total"),
    critical: rate("forwarder_critical_error_total"),
  };
}

/** seriesValues sums all returned series per timestamp, so per-reason or per-partition series collapse to one line. */
export function seriesValues(chartData: any[] | null | undefined): number[] | null {
  if (!chartData) return null;
  const byTimestamp = new Map<number, number>();
  chartData.forEach((series) =>
    (series?.values || []).forEach(([timestamp, value]: [number, string]) => {
      const parsed = Number(value);
      if (!Number.isFinite(parsed) || parsed < 0) return;
      byTimestamp.set(timestamp, (byTimestamp.get(timestamp) || 0) + parsed);
    })
  );
  return Array.from(byTimestamp.entries())
    .sort(([a], [b]) => a - b)
    .map(([, value]) => value);
}

export function latest(values?: number[] | null): number | undefined {
  return values && values.length ? values[values.length - 1] : undefined;
}

/** trendVsAverage compares the latest point with the mean of the earlier window points. */
export function trendVsAverage(values?: number[] | null): number | undefined {
  if (!values || values.length < 2) return undefined;
  const earlier = values.slice(0, -1);
  const average = earlier.reduce((sum, value) => sum + value, 0) / earlier.length;
  if (average <= 0) return undefined;
  return (values[values.length - 1] - average) / average;
}

/** toSparkline resamples a series into equal buckets scaled to the tallest bucket. */
export function toSparkline(values?: number[] | null, bars = 8): number[] | undefined {
  if (!values || values.length === 0) return undefined;
  const buckets = Array.from({ length: bars }, (_, index) => {
    const start = Math.floor((index * values.length) / bars);
    const end = Math.max(start + 1, Math.floor(((index + 1) * values.length) / bars));
    const slice = values.slice(start, Math.min(end, values.length));
    return slice.length ? slice.reduce((sum, value) => sum + value, 0) / slice.length : 0;
  });
  const max = Math.max(...buckets);
  if (max <= 0) return buckets.map(() => 8);
  return buckets.map((value) => Math.max(8, Math.round((value / max) * 100)));
}

export function formatMicros(us: number): string {
  if (us < 1000) return `${Math.round(us)} µs`;
  if (us < 1_000_000) {
    const ms = us / 1000;
    return `${ms < 10 ? ms.toFixed(1) : Math.round(ms)} ms`;
  }
  return `${(us / 1_000_000).toFixed(2)} s`;
}

export function formatCompact(value: number): string {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1000) return `${(value / 1000).toFixed(1)}k`;
  if (value >= 10 || Number.isInteger(value)) return `${Math.round(value)}`;
  return value.toFixed(1);
}

export function formatElapsed(ms: number): string {
  const seconds = Math.max(0, ms / 1000);
  if (seconds < 60) return `${seconds < 10 ? seconds.toFixed(1) : Math.round(seconds)} s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)} min`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)} h`;
  return `${Math.round(seconds / 86400)}d`;
}

export function formatAgo(timestamp: string | undefined, now: number): string | undefined {
  const parsed = timestamp ? Date.parse(timestamp) : NaN;
  if (!Number.isFinite(parsed)) return undefined;
  return `${formatElapsed(now - parsed)} ago`;
}

export function formatDrain(seconds: number): string {
  if (seconds < 60) return "<1 min";
  if (seconds < 3600) return `~${Math.round(seconds / 60)} min`;
  return `~${(seconds / 3600).toFixed(1)} h`;
}

function formatTrend(trend: number | undefined, suffix: string): string | undefined {
  if (trend === undefined || Math.abs(trend) < 0.1) return undefined;
  return `${trend > 0 ? "↑" : "↓"}${Math.round(Math.abs(trend) * 100)}% ${suffix}`;
}

/** drainTime is seconds to clear the backlog at the current read rate; 0 when empty, Infinity when stalled. */
export function drainTime(pending?: number, rate?: number): number | undefined {
  if (pending === undefined) return undefined;
  if (pending === 0) return 0;
  if (!rate || rate <= 0) return Infinity;
  return pending / rate;
}

export interface WatermarkEdge {
  from: string;
  to: string;
  isWatermarkEnabled?: boolean;
  watermarks?: (number | string)[];
}

/** watermarkLag uses outgoing edges, or incoming edges for sinks, and ignores unset (-1) partitions. */
export function watermarkLag(
  edges: WatermarkEdge[] | undefined,
  vertexId: string,
  now: number
): number | undefined {
  if (!edges?.length) return undefined;
  const outgoing = edges.filter((edge) => edge.from === vertexId);
  const relevant = outgoing.length ? outgoing : edges.filter((edge) => edge.to === vertexId);
  const watermarks = relevant
    .filter((edge) => edge.isWatermarkEnabled !== false)
    .flatMap((edge) => edge.watermarks || [])
    .map(Number)
    .filter((watermark) => Number.isFinite(watermark) && watermark > 0);
  if (!watermarks.length) return undefined;
  return Math.max(0, now - Math.min(...watermarks));
}

export function slowestStage(
  stages: { name: string; micros: number }[]
): { name: string; micros: number } | undefined {
  return stages.reduce<{ name: string; micros: number } | undefined>(
    (slowest, stage) => (!slowest || stage.micros > slowest.micros ? stage : slowest),
    undefined
  );
}

const TONE_RANK: Record<SignalTone, number> = { neutral: 0, ok: 1, warn: 2, critical: 3 };

export function worstTone(tones: (SignalTone | undefined)[]): SignalTone {
  return tones.reduce<SignalTone>(
    (worst, tone) => (tone && TONE_RANK[tone] > TONE_RANK[worst] ? tone : worst),
    "neutral"
  );
}

export interface DaemonRates {
  rate1m?: number;
  rate5m?: number;
  rate15m?: number;
  pending?: number;
}

function sumDefined(values: (number | undefined)[]): number | undefined {
  const known = values.filter((value): value is number => value !== undefined);
  return known.length ? known.reduce((sum, value) => sum + value, 0) : undefined;
}

function parseDaemonNumber(value: unknown): number | undefined {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : undefined;
}

/** readDaemonRates sums daemon rates and pendings across partitions; `-1` (not reported) is dropped. */
export function readDaemonRates(vertexMetrics: any, type: string, vertexId: string): DaemonRates {
  const partitions: any[] =
    type === MONO_VERTEX
      ? vertexMetrics
        ? [vertexMetrics]
        : []
      : (vertexMetrics && vertexMetrics[vertexId]) || [];
  const pick = (field: "processingRates" | "pendings", period: string) =>
    sumDefined(partitions.map((partition) => parseDaemonNumber(partition?.[field]?.[period])));
  return {
    rate1m: pick("processingRates", "1m"),
    rate5m: pick("processingRates", "5m"),
    rate15m: pick("processingRates", "15m"),
    pending: pick("pendings", "default"),
  };
}

export interface DataHealth {
  dataHealthStatus?: string;
  dataHealthCode?: string;
  dataHealthMessage?: string;
}

export interface MonoVertexScaleStatus {
  desiredReplicas?: number;
  readyReplicas?: number;
  lastScaledAt?: string;
}

export interface VertexHealthInput {
  type: string;
  vertexId: string;
  metricsEnabled: boolean;
  now: number;
  daemon: DaemonRates;
  series: SignalSeries;
  queries: SignalQueries;
  buffers?: BufferInfo[];
  watermarkEdges?: WatermarkEdge[];
  health?: DataHealth;
  monoStatus?: MonoVertexScaleStatus;
  scale?: { min?: number; max?: number };
  fleetItems: PodFleetItem[];
  recentErrorCount?: number;
}

const UNAVAILABLE = "Metrics unavailable";
const UNAVAILABLE_DETAIL = "Prometheus is not configured";
const LOADING = "Loading…";

function metricsTarget(
  queries: SignalQueries,
  key: SignalQueryKey,
  presets?: Record<string, string>
): InspectTarget | undefined {
  const metric = queries[key]?.metric_name;
  return metric ? { kind: "metrics", metric, presets } : undefined;
}

function latencyCard(input: VertexHealthInput): HealthCard {
  const base = { id: "latency" as const, title: "Latency" };
  if (!input.metricsEnabled) {
    return { ...base, value: UNAVAILABLE, detail: UNAVAILABLE_DETAIL, tone: "neutral" };
  }
  const p99 = input.series.p99;
  const current = latest(p99);
  const inspect = metricsTarget(input.queries, "p99", { quantile: "0.99", duration: "5m" });
  if (current === undefined) {
    return {
      ...base,
      value: p99 === undefined ? LOADING : "No data",
      detail: p99 === undefined ? "Fetching processing time" : "No processing-time samples in 30 min",
      tone: "neutral",
      inspect,
    };
  }
  const stages = (Object.keys(STAGE_LABELS) as SignalQueryKey[])
    .filter((key) => input.queries[key])
    .map((key) => ({ name: STAGE_LABELS[key] as string, micros: latest(input.series[key]) }))
    .filter((stage): stage is { name: string; micros: number } => stage.micros !== undefined);
  const slowest = slowestStage(stages);
  const trend = trendVsAverage(p99);
  const tone: SignalTone =
    trend !== undefined && trend > 1 ? "critical" : trend !== undefined && trend > 0.25 ? "warn" : "ok";
  const detail = [
    slowest
      ? `slowest stage: ${slowest.name} P99 ${formatMicros(slowest.micros)}`
      : "Processing time per batch",
    formatTrend(trend, "vs 30m avg"),
  ]
    .filter(Boolean)
    .join(" · ");
  const stageTotal = stages.reduce((sum, stage) => sum + stage.micros, 0);
  return {
    ...base,
    value: `P99 ${formatMicros(current)}`,
    detail,
    tone,
    sparkline: toSparkline(p99),
    stages: stages.map((stage) => ({ name: stage.name, value: formatMicros(stage.micros) })),
    // Batch processing time includes waiting for the source to fill a batch, which no stage measures.
    stagesNote:
      stages.length && stageTotal < current / 2
        ? `Stages add up to ${formatMicros(stageTotal)}; the rest of the batch time is spent waiting for messages.`
        : undefined,
    inspect,
  };
}

function trafficCard(input: VertexHealthInput): HealthCard {
  const base = { id: "traffic" as const, title: "Traffic" };
  const { rate1m, rate5m, rate15m, pending } = input.daemon;
  const sparkline = input.metricsEnabled ? toSparkline(input.series.readRate) : undefined;
  const inspect: InspectTarget = { kind: "tab", tab: "processingRates" };
  if (rate1m === undefined) {
    return {
      ...base,
      value: "No data",
      detail: "Daemon has not reported rates yet",
      tone: "neutral",
      sparkline,
      inspect,
    };
  }
  const out = input.metricsEnabled ? latest(input.series.outRate) : undefined;
  const averages = [
    rate5m !== undefined ? `5m avg ${formatCompact(rate5m)}` : undefined,
    rate15m !== undefined ? `15m avg ${formatCompact(rate15m)}` : undefined,
    out !== undefined ? `out ${formatCompact(out)}/s` : undefined,
  ];
  if (rate1m === 0 && pending !== undefined && pending > 0) {
    return {
      ...base,
      value: "Stalled",
      detail: `0 msg/s with ${formatCompact(pending)} pending`,
      tone: "critical",
      sparkline,
      inspect,
    };
  }
  if (rate1m === 0) {
    return {
      ...base,
      value: "Idle",
      detail: averages.filter(Boolean).join(" · ") || "No messages in the last minute",
      tone: "neutral",
      sparkline,
      inspect,
    };
  }
  const dropped = rate15m !== undefined && rate15m > 0 && rate1m < rate15m * 0.5;
  return {
    ...base,
    value: `${formatCompact(rate1m)} msg/s`,
    detail: averages.filter(Boolean).join(" · "),
    tone: dropped ? "warn" : "ok",
    sparkline,
    inspect,
  };
}

/** bufferPercentFull is usage relative to the buffer's usage limit; 100 means writers are blocked. */
export function bufferPercentFull(buffer: BufferInfo): number {
  if (buffer.isFull) return 100;
  const ratio =
    buffer.bufferUsageLimit > 0 ? buffer.bufferUsage / buffer.bufferUsageLimit : buffer.bufferUsage;
  return Math.min(100, Math.round(ratio * 100));
}

function backlogCard(input: VertexHealthInput): HealthCard {
  const base = { id: "backlog" as const, title: "Backlog" };
  const { pending, rate1m } = input.daemon;
  const buffers = input.buffers || [];
  const fullest = buffers.reduce<BufferInfo | undefined>(
    (max, buffer) => (!max || bufferPercentFull(buffer) > bufferPercentFull(max) ? buffer : max),
    undefined
  );
  const percentFull = fullest ? bufferPercentFull(fullest) : undefined;
  const lag = watermarkLag(input.watermarkEdges, input.vertexId, input.now);
  const pendingSeries = input.metricsEnabled ? input.series.pending : undefined;
  const trend = trendVsAverage(pendingSeries);
  const drain = drainTime(pending, rate1m);
  const inspect: InspectTarget | undefined = buffers.length
    ? { kind: "tab", tab: "buffers" }
    : input.metricsEnabled
    ? metricsTarget(input.queries, "pending")
    : undefined;

  let value: string;
  if (pending === undefined) {
    value = percentFull !== undefined ? `buffer ${percentFull}% full` : "Pending not reported";
  } else if (drain === 0) {
    value = "No backlog";
  } else if (drain === Infinity) {
    value = "Not draining";
  } else {
    value = `${formatDrain(drain as number)} to drain`;
  }

  const detail = [
    pending !== undefined ? `${formatCompact(pending)} pending` : undefined,
    formatTrend(trend, "/ 30m"),
    percentFull !== undefined && pending !== undefined ? `buffer ${percentFull}% full` : undefined,
    lag !== undefined ? `WM lag ${formatElapsed(lag)}` : undefined,
  ]
    .filter(Boolean)
    .join(" · ");

  const atLimit = !!fullest && (fullest.isFull || fullest.bufferUsage >= fullest.bufferUsageLimit);
  const nearLimit = !!fullest && fullest.bufferUsage >= fullest.bufferUsageLimit * 0.75;
  let tone: SignalTone;
  if (drain === Infinity || atLimit || (lag !== undefined && lag > 5 * 60_000)) {
    tone = "critical";
  } else if (
    (drain !== undefined && drain > 300) ||
    (trend !== undefined && trend > 0.5) ||
    nearLimit ||
    (lag !== undefined && lag > 60_000)
  ) {
    tone = "warn";
  } else if (pending === undefined && !fullest && lag === undefined) {
    tone = "neutral";
  } else {
    tone = "ok";
  }

  return {
    ...base,
    value,
    detail: detail || "Source does not report pending messages",
    tone,
    sparkline: toSparkline(pendingSeries),
    inspect,
  };
}

function lossCard(input: VertexHealthInput): HealthCard {
  const base = { id: "loss" as const, title: "Message outcomes" };
  if (!input.metricsEnabled) {
    return { ...base, value: UNAVAILABLE, detail: UNAVAILABLE_DETAIL, tone: "neutral" };
  }
  const isMono = input.type === MONO_VERTEX;
  const { drops, nacks, fallback } = input.series;
  const inspect = metricsTarget(input.queries, "drops");
  const requestedKeys = [
    "drops",
    "nacks",
    ...(isMono ? ["fallback" as const] : []),
  ] as SignalQueryKey[];
  const requested = requestedKeys.map((key) => input.series[key]);
  if (requested.some((series) => series === undefined)) {
    return { ...base, value: LOADING, detail: "Fetching message outcome rates", tone: "neutral", inspect };
  }
  if (requested.every((series) => series === null)) {
    return {
      ...base,
      value: "No data",
      detail: "Message outcome metrics are not served by the metrics proxy",
      tone: "neutral",
      inspect,
    };
  }
  const rate = (series: number[] | null | undefined) =>
    Array.isArray(series) ? latest(series) ?? 0 : 0;
  const dropRate = rate(drops);
  const nackRate = rate(nacks);
  const fallbackRate = rate(fallback);
  const partiallyUnavailable = requested.some((series) => series === null);
  const outcomes = [
    {
      key: "drops" as const,
      name: isMono ? "Duplicate-message drops" : "Other runtime drops",
      description: isMono
        ? "Duplicate source messages discarded by MonoVertex."
        : "Messages discarded by vertex runtime paths, such as a full buffer, unmatched forwarding rule, duplicate, write failure, or exhausted sink handling.",
    },
    {
      key: "nacks" as const,
      name: "Nacks / redeliveries",
      description: "Messages negatively acknowledged for redelivery. A nack is a retry signal, not a confirmed message drop.",
    },
    ...(isMono
      ? [
          {
            key: "fallback" as const,
            name: "Fallback deliveries",
            description:
              "Messages delivered to the configured fallback sink or DLQ instead of the primary sink; this is alternate delivery, not loss.",
          },
        ]
      : []),
  ].map(({ key, name, description }) => {
    const series = input.series[key];
    return {
      name,
      value: series === null ? "Unavailable" : `${formatCompact(rate(series))}/s`,
      description,
      available: series !== null,
    };
  });
  const activeOutcomes = outcomes.filter((outcome) => outcome.available && outcome.value !== "0/s").length;
  const unavailableOutcomes = outcomes.filter((outcome) => !outcome.available).length;
  const detail = unavailableOutcomes
    ? `${unavailableOutcomes} outcome metrics unavailable · hover for details`
    : activeOutcomes
    ? `${activeOutcomes} active outcome ${activeOutcomes === 1 ? "type" : "types"} · hover for details`
    : "No active outcomes · hover for details";
  const tone: SignalTone =
    dropRate > 0
      ? "critical"
      : nackRate > 0 || fallbackRate > 0
      ? "warn"
      : partiallyUnavailable
      ? "neutral"
      : "ok";
  return {
    ...base,
    value: dropRate > 0
      ? `${formatCompact(dropRate)} drops/s`
      : partiallyUnavailable
      ? "Outcome metrics unavailable"
      : "No drops",
    detail,
    tone,
    breakdown: { title: "Message outcomes", items: outcomes },
    // Runtime drop counters only appear after the first drop, so no samples mean a flat zero line.
    sparkline: toSparkline(drops && !drops.length ? [0] : drops),
    inspect,
  };
}

/** isUnhealthyPod counts critical pods; crash loops and other non-running states are critical. */
export function isUnhealthyPod(item: PodFleetItem): boolean {
  return item.severity === "critical";
}

function errorsCard(input: VertexHealthInput): HealthCard {
  const base = { id: "errors" as const, title: "Errors" };
  const items = input.fleetItems;
  const unhealthy = items.filter(isUnhealthyPod).length;
  const restarts = items.reduce((sum, item) => sum + item.restartCount, 0);
  const errors = input.recentErrorCount || 0;
  const criticalSeries = input.series.critical;
  const criticalRate = Array.isArray(criticalSeries) ? latest(criticalSeries) ?? 0 : 0;
  const criticalMetricValue = !input.metricsEnabled
    ? UNAVAILABLE
    : criticalSeries === undefined
    ? LOADING
    : criticalSeries === null
    ? "Unavailable"
    : `${formatCompact(criticalRate)}/s`;
  const value = unhealthy
    ? `${unhealthy} unhealthy ${unhealthy === 1 ? "pod" : "pods"}`
    : criticalRate > 0
    ? `${formatCompact(criticalRate)} critical errors/s`
    : !items.length
    ? "No pods"
    : `All ${items.length} ${items.length === 1 ? "pod" : "pods"} healthy`;
  const detail = `${restarts} ${restarts === 1 ? "restart" : "restarts"} · ${errors} ${
    errors === 1 ? "error" : "errors"
  } (24h)`;
  const inspect = unhealthy
    ? ({ kind: "critical-pods" } as const)
    : criticalRate > 0
    ? metricsTarget(input.queries, "critical")
    : ({ kind: "tab", tab: "errors" } as const);
  return {
    ...base,
    value,
    detail,
    tone:
      unhealthy || criticalRate > 0
        ? "critical"
        : restarts || errors
        ? "warn"
        : items.length
        ? "ok"
        : "neutral",
    breakdown: {
      title: "Error diagnostics",
      items: [
        {
          name: "Unhealthy pods",
          value: String(unhealthy),
          description: "Pods with a container that is not running or has terminated unexpectedly.",
          available: true,
        },
        {
          name: "Container restarts",
          value: String(restarts),
          description: "Total container restart count across the current pod fleet.",
          available: true,
        },
        {
          name: "Container errors",
          value: `${errors} (24h)`,
          description: "Container errors reported by the Errors API during the last 24 hours.",
          available: true,
        },
        {
          name: "Critical runtime errors",
          value: criticalMetricValue,
          description: "Severe runtime errors that can interrupt message processing.",
          available: input.metricsEnabled && criticalSeries !== null,
        },
      ],
    },
    inspect,
  };
}

function verdict(health: DataHealth | undefined, isMono: boolean): VertexHealth["verdict"] {
  const status = health?.dataHealthStatus?.toLowerCase();
  if (!status) return undefined;
  const tone: SignalTone =
    status === "healthy"
      ? "ok"
      : status === "warning"
      ? "warn"
      : status === "critical" || status === "unhealthy"
      ? "critical"
      : "neutral";
  const code = health?.dataHealthCode ? ` (${health.dataHealthCode})` : "";
  return { label: `${isMono ? "Data flow" : "Pipeline data flow"} ${status}${code}`, tone };
}

function replicas(input: VertexHealthInput): VertexHealth["replicas"] {
  if (input.type === MONO_VERTEX) {
    const { readyReplicas, desiredReplicas, lastScaledAt } = input.monoStatus || {};
    if (desiredReplicas === undefined) return undefined;
    const ready = readyReplicas || 0;
    const scaled = formatAgo(lastScaledAt, input.now);
    return {
      label: `${ready}/${desiredReplicas} ready${scaled ? ` · scaled ${scaled}` : ""}`,
      tone: ready < desiredReplicas ? "warn" : "ok",
    };
  }
  if (!input.fleetItems.length) return undefined;
  const running = input.fleetItems.filter((item) => item.runtime?.status === "Running").length;
  const { min, max } = input.scale || {};
  let range = "";
  if (min !== undefined && min === max) range = ` · scale ${min}`;
  else if (min !== undefined || max !== undefined) range = ` · scale ${min ?? 0}–${max ?? "∞"}`;
  return {
    label: `${running} ${running === 1 ? "pod" : "pods"} running${range}`,
    tone: running < input.fleetItems.length ? "warn" : "ok",
  };
}

/** buildVertexHealth turns daemon, pod, error, watermark, and Prometheus inputs into the strip model. */
export function buildVertexHealth(input: VertexHealthInput): VertexHealth {
  const cards = [
    latencyCard(input),
    trafficCard(input),
    backlogCard(input),
    lossCard(input),
    errorsCard(input),
  ];
  const headerVerdict = verdict(input.health, input.type === MONO_VERTEX);
  return {
    tone: worstTone([...cards.map((card) => card.tone), headerVerdict?.tone]),
    verdict: headerVerdict,
    replicas: replicas(input),
    meta: input.metricsEnabled
      ? "trends over the last 30 minutes"
      : "live values · Prometheus not configured",
    cards,
  };
}
