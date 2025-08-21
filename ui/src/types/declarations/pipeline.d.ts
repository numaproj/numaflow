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

export interface MonoVertexSpec {
  source: any;
  sink: any;
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
