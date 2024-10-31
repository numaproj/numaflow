export const durationOptions = ["1m", "5m", "10m"];

export const durationMap: { [p: string]: string } = {
  "1m": "1 min",
  "5m": "5 mins",
  "10m": "10 mins",
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
};

export const dimensionReverseMap: { [p: string]: string } = {
  monoVertex: "mono-vertex",
  pipeline: "pipeline",
  pod: "pod",
};

export const metricNameMap: { [p: string]: string } = {
  monovtx_ack_time_bucket: "Mono Vertex Ack Time Latency (in micro seconds)",
  monovtx_read_time_bucket: "Mono Vertex Read Time Latency (in micro seconds)",
  monovtx_processing_time_bucket:
    "Mono Vertex Processing Time Latency (in micro seconds)",
  monovtx_sink_time_bucket:
    "Mono Vertex Sink Write Time Latency (in micro seconds)",
};
