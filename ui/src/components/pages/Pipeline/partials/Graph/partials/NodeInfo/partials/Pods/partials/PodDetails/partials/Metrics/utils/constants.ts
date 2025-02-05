export const durationOptions = ["1m", "5m", "15m"];

export const durationMap: { [p: string]: string } = {
  "1m": "1 min",
  "5m": "5 mins",
  "15m": "15 mins",
};

export const quantileOptions = ["0.50", "0.90", "0.95", "0.99"];

export const quantileMap: { [p: string]: string } = {
  "0.50": "50th Percentile",
  "0.90": "90th Percentile",
  "0.95": "95th Percentile",
  "0.99": "99th Percentile",
};

export const dimensionMap: { [p: string]: string } = {
  "mono-vertex": "MonoVertex",
  pod: "Pod",
  pipeline: "Pipeline",
  vertex: "Vertex",
  container: "Container",
};

export const dimensionReverseMap: { [p: string]: string } = {
  monoVertex: "mono-vertex",
  source: "vertex",
  udf: "vertex",
  sink: "vertex",
  pipeline: "pipeline",
  pod: "pod",
  container: "container",
};

export const metricNameMap: { [p: string]: string } = {
  monovtx_ack_time_bucket: "Mono Vertex Ack Time Latency",
  monovtx_read_time_bucket: "Mono Vertex Read Time Latency",
  monovtx_processing_time_bucket: "Mono Vertex Processing Time Latency",
  monovtx_sink_time_bucket: "Mono Vertex Sink Write Time Latency",
  forwarder_data_read_total: "Vertex Read Processing Rate",
  monovtx_read_total: "Mono Vertex Read Processing Rate",
  monovtx_pending: "Mono Vertex Pending Messages",
  vertex_pending_messages: "Vertex Pending Messages",
  namespace_pod_cpu_utilization: "Pod CPU Utilization",
  namespace_pod_memory_utilization: "Pod Memory Utilization",
  namespace_app_container_cpu_utilization: "Container CPU Utilization",
  namespace_app_container_memory_utilization: "Container Memory Utilization",
};
