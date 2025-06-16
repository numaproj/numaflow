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

export const VERTEX_PENDING_MESSAGES = "Vertex Pending Messages";
export const UDF_READ_PROCESSING_RATE = "UDF Read Processing Rate";
export const UDF_WRITE_PROCESSING_RATE = "UDF Write Processing Rate";
export const VERTEX_PROCESSING_RATE = "Vertex Read Processing Rate";
export const MONO_VERTEX_PENDING_MESSAGES = "MonoVertex Pending Messages";
export const MONO_VERTEX_PROCESSING_RATE = "MonoVertex Read Processing Rate";
export const MONO_VERTEX_PROCESSING_TIME_LATENCY =
  "MonoVertex Processing Time Latency";
export const MONO_VERTEX_SINK_WRITE_TIME_LATENCY =
  "MonoVertex Sink Write Time Latency";
export const VERTEX_WRITE_PROCESSING_TIME_LATENCY = "Vertex Write Processing Time Latency";
export const VERTEX_READ_PROCESSING_TIME_LATENCY = "Vertex Read Processing Time Latency";
export const POD_CPU_UTILIZATION = "Pod CPU Utilization";
export const POD_MEMORY_UTILIZATION = "Pod Memory Utilization";
export const CONTAINER_CPU_UTILIZATION = "Container CPU Utilization";
export const CONTAINER_MEMORY_UTILIZATION = "Container Memory Utilization";
