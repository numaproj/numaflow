import { Edge, Node } from "@xyflow/react";

export interface VertexMetrics {
  ratePerMin: string;
  ratePerFiveMin: string;
  ratePerFifteenMin: string;
  podMetrics: any[];
  error: boolean;
}

export interface EdgeWatermark {
  watermarks: any[];
  isWaterMarkEnabled: boolean;
  WMFetchTime: number;
}

export interface BufferInfo {
  bufferName: string;
  ackPendingCount: number;
  pendingCount: number;
  totalMessages: number;
  isFull: boolean;
  bufferUsage: number;
  bufferUsageLimit: number;
  bufferLength: number;
}

export interface GraphData {
  vertices: Node[];
  edges: Edge[];
  pipeline?: Pipeline;
}

export interface Conditions {
  keyIn: any[];
}

export interface Pipeline {
  spec: Spec;
  metadata: any;
  status?: any;
}

export interface Spec {
  vertices: PipelineVertex[];
  edges: PipelineEdge[];
  sideInputs?: SideInput[];
  watermark: any;
}

export interface PipelineVertex {
  name: string;
  source?: any;
  udf?: any;
  sink?: any;
  sideInputs?: any[];
}

export interface PipelineEdge {
  from: string;
  to: string;
  conditions?: Conditions;
}

export interface SideInput {
  name: string;
  container: any;
  trigger: any;
}

export interface IsbSummary {
  name: string;
  status: string;
  isbService: IsbService;
}

export interface IsbService {
  metadata: any;
  spec: IsbServiceSpec;
  status: IsbServiceStatus;
}

export interface IsbServiceStatus {
  conditions: any;
  phase: string;
}

export interface IsbServiceSpec {
  jetstream?: Jetstream;
  redis?: Redis;
}

export interface ISBJetStreamSummary {
  server: string;
  serverId?: string;
  cluster?: string;
  streams: number;
  consumers: number;
  messages: number;
  bytes: number;
  apiRequests: number;
  apiErrors: number;
  apiErrorRate: number;
  metaLeader: boolean;
}

export interface ISBJetStreamRaftMeta {
  name: string;
  id?: string;
  leader: boolean;
  current?: boolean;
  online: boolean;
  active?: string;
  lag?: number;
}

export interface ISBMonitorError {
  pod: string;
  message: string;
}

export interface ISBJetStreamResponse {
  summary: ISBJetStreamSummary[];
  raftMetaGroup: ISBJetStreamRaftMeta[];
  errors?: ISBMonitorError[];
}

export interface ISBServiceDebugFetchResult {
  data?: {
    jetStream: ISBJetStreamResponse;
  };
  loading: boolean;
  error: any;
  refresh: () => void;
}

export interface PipelineISBStream {
  namespace: string;
  pipeline: string;
  vertex: string;
  from?: string;
  to?: string;
  partition: number;
  stream: string;
  subjects?: string[];
  messages: number;
  bytes: number;
  consumerCount: number;
  firstSeq: number;
  lastSeq: number;
  firstTimestamp?: string;
  lastTimestamp?: string;
  storage?: string;
  replicas?: number;
  retention?: string;
  maxMessages?: number;
  leader?: string;
  sourcePod?: string;
  collectedAt?: string;
  scope: string;
  sharedByInboundEdges: boolean;
}

export interface PipelineISBConsumer {
  namespace: string;
  pipeline: string;
  vertex: string;
  from?: string;
  to?: string;
  partition: number;
  stream: string;
  consumer: string;
  durable?: string;
  filterSubject?: string;
  filterSubjects?: string[];
  ackPolicy?: string;
  deliverPolicy?: string;
  ackWaitSeconds?: number;
  maxAckPending?: number;
  numAckPending: number;
  numRedelivered: number;
  numWaiting: number;
  numPending: number;
  deliveredConsumerSeq: number;
  deliveredStreamSeq: number;
  ackFloorConsumerSeq: number;
  ackFloorStreamSeq: number;
  leader?: string;
  sourcePod?: string;
  collectedAt?: string;
  scope: string;
  sharedByInboundEdges: boolean;
}

export interface PipelineISBKVStore {
  namespace: string;
  pipeline: string;
  scope: string;
  direction?: string;
  vertex?: string;
  from?: string;
  to?: string;
  bucket: string;
  stream: string;
  values: number;
  bytes: number;
  history?: number;
  ttlSeconds?: number;
  replicas?: number;
  storage?: string;
  leader?: string;
  sourcePod?: string;
  collectedAt?: string;
}

export interface PipelineISBStreamsResponse {
  streams: PipelineISBStream[];
  errors?: ISBMonitorError[];
}

export interface PipelineISBConsumersResponse {
  consumers: PipelineISBConsumer[];
  errors?: ISBMonitorError[];
}

export interface PipelineISBKVStoresResponse {
  kvStores: PipelineISBKVStore[];
  errors?: ISBMonitorError[];
}

export interface PipelineISBDebugFetchResult {
  data?: {
    streams?: PipelineISBStreamsResponse;
    consumers?: PipelineISBConsumersResponse;
    kvStores?: PipelineISBKVStoresResponse;
  };
  loading: boolean;
  error: any;
  refresh: () => void;
}

export interface PipelineSummary {
  name: string;
  status: string;
  lag?: number;
  pipeline: Pipeline;
}

export interface PipelineMergeSummaryData {
  pipelineData: PipelineSummary;
  isbData: IsbSummary;
}

export interface PipelineSummaryFetchResult {
  data?: PipelineMergeSummaryData;
  loading: boolean;
  error: any;
  refresh: () => void;
}

export interface MonoVertex {
  spec: any;
  metadata: any;
  status?: any;
}

export interface MonoVertexBypassRule {
  tags?: {
    operator?: "and" | "or" | "not";
    values?: string[];
  };
}

export interface MonoVertexBypass {
  sink?: MonoVertexBypassRule;
  onSuccess?: MonoVertexBypassRule;
  fallback?: MonoVertexBypassRule;
}

export interface MonoVertexSpec {
  source: any;
  sink: any;
  udf?: any;
  bypass?: MonoVertexBypass;
  scale: any;
}

export interface MonoVertexMetrics {
  ratePerMin: string;
  ratePerFiveMin: string;
  ratePerFifteenMin: string;
  podMetrics: any[];
  error: boolean;
}

export interface MonoVertexSummary {
  name: string;
  status: string;
  lag?: number;
  monoVertex: MonoVertex;
}

export interface MonoVertexMergeSummaryData {
  monoVertexData: MonoVertexSummary;
}

export interface MonoVertexSummaryFetchResult {
  data?: MonoVertexMergeSummaryData;
  loading: boolean;
  error: any;
  refresh: () => void;
}

export interface PipelineUpdateFetchResult {
  pipelineAvailable: boolean;
}

export interface PipelineVertexMetric {
  partition: number;
  oneM: number;
  fiveM: number;
  fifteenM: number;
}

export interface PipelineVertexMetrics {
  vertexId: string;
  metrics: PipelineVertexMetric[];
}

export interface PipelineVertexMetricsFetchResult {
  data?: PipelineVertexMetrics[];
  loading: boolean;
  error: any;
}

export interface PiplelineVertexMetricsFetchProps {
  namespace: string;
  pipeline: string;
  loadOnRefresh?: boolean;
}

export interface PipelineWatermark {
  partition: number;
  watermark: number;
  formattedWatermark: string;
}

export interface PipelineWatermarks {
  edgeId: string;
  watermarks: PipelineWatermark[];
}

export interface PipelineWatermarksFetchResult {
  data?: PipelineWatermarks[];
  loading: boolean;
  error: any;
}

export interface PipelineWatermarksFetchProps {
  namespace: string;
  pipeline: string;
  loadOnRefresh?: boolean;
}

export interface PipelineHealthFetchResult {
  data?: PipelineHealthData;
  loading: boolean;
  error: any;
  refresh: () => void;
}

export interface MonoVertexHealthFetchResult {
  data?: MonoVertexHealthData;
  loading: boolean;
  error: any;
  refresh: () => void;
}

export interface MonoVertexHealthData {
  dataHealthCode?: string;
  dataHealthMessage: string;
  dataHealthStatus: string;
  resourceHealthCode0?: string;
  resourceHealthMessage: string;
  resourceHealthStatus: string;
}

export interface PipelineHealthData {
  dataHealthCode?: string;
  dataHealthMessage: string;
  dataHealthStatus: string;
  resourceHealthCode0?: string;
  resourceHealthMessage: string;
  resourceHealthStatus: string;
}
