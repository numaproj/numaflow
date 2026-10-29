import { DiscoveredMetric } from "./goldenSignalsModel";

/** Mirrors the `signal` tags in config/base/numaflow-server/numaflow-server-metrics-proxy-config.yaml. */
export const MONO_VERTEX_DISCOVERY: DiscoveredMetric[] = [
  { pattern_name: "mono_vertex_gauge", metric_name: "monovtx_pending" },
  { pattern_name: "mono_vertex_histogram", metric_name: "monovtx_processing_time_bucket", signal: "p99" },
  { pattern_name: "mono_vertex_histogram", metric_name: "monovtx_sink_time_bucket", signal: "stageSink" },
  { pattern_name: "mono_vertex_histogram", metric_name: "monovtx_read_time_bucket", signal: "stageRead" },
  { pattern_name: "mono_vertex_histogram", metric_name: "monovtx_ack_time_bucket", signal: "stageAck" },
  {
    pattern_name: "mono_vertex_histogram",
    metric_name: "monovtx_transformer_time_bucket",
    signal: "stageTransformer",
  },
  { pattern_name: "mono_vertex_histogram", metric_name: "monovtx_udf_time_bucket", signal: "stageUdf" },
  { pattern_name: "mono_vertex_throughput", metric_name: "monovtx_read_total" },
  { pattern_name: "mono_vertex_message_outcomes", metric_name: "monovtx_dropped_total", signal: "drops" },
  { pattern_name: "mono_vertex_message_outcomes", metric_name: "monovtx_nack_total", signal: "nacks" },
  {
    pattern_name: "mono_vertex_message_outcomes",
    metric_name: "monovtx_fallback_sink_write_total",
    signal: "fallback",
  },
  { pattern_name: "mono_vertex_critical_errors", metric_name: "monovtx_critical_error_total", signal: "critical" },
];

export const VERTEX_DISCOVERY: DiscoveredMetric[] = [
  { pattern_name: "vertex_gauge", metric_name: "vertex_pending_messages" },
  { pattern_name: "vertex_histogram", metric_name: "forwarder_write_processing_time_bucket", signal: "stageWrite" },
  { pattern_name: "vertex_histogram", metric_name: "forwarder_read_processing_time_bucket", signal: "stageRead" },
  { pattern_name: "vertex_histogram", metric_name: "forwarder_ack_processing_time_bucket", signal: "stageAck" },
  { pattern_name: "vertex_histogram", metric_name: "forwarder_processing_time_bucket", signal: "p99" },
  { pattern_name: "vertex_histogram", metric_name: "forwarder_udf_processing_time_bucket", signal: "stageUdf" },
  { pattern_name: "vertex_throughput", metric_name: "forwarder_data_read_total" },
  { pattern_name: "vertex_message_outcomes", metric_name: "forwarder_drop_total", signal: "drops" },
  { pattern_name: "vertex_message_outcomes", metric_name: "forwarder_nack_total", signal: "nacks" },
  { pattern_name: "vertex_critical_errors", metric_name: "forwarder_critical_error_total", signal: "critical" },
];
