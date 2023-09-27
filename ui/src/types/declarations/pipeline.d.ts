import { Edge, Node } from "reactflow";

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
  watermark: any;
}

export interface PipelineVertex {
  name: string;
  source?: any;
  udf?: any;
  sink?: any;
}

export interface PipelineEdge {
  from: string;
  to: string;
  conditions?: Conditions;
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
}

export interface PipelineSummary {
  name: string;
  status: string;
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
}
