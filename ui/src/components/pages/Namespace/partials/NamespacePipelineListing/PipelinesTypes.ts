export interface PipelinesData {
  data: PipelineData[];
}

export interface PipelineData {
  name: string;
  status: string;
  pipeline: Pipeline;
}

interface Pipeline {
  kind: string;
  apiVersion: string;
  metadata: Metadata;
  spec: Spec;
  status: Status;
}

interface Status {
  conditions: Condition[];
  phase: string;
  lastUpdated: string;
  vertexCount: number;
  sourceCount: number;
  sinkCount: number;
  udfCount: number;
}

interface Condition {
  type: string;
  status: string;
  lastTransitionTime: string;
  reason: string;
  message: string;
}

interface Spec {
  vertices: Vertex[];
  edges: Edge[];
  lifecycle: Lifecycle;
  limits: Limits;
  watermark: Watermark;
}

interface Watermark {
  maxDelay: string;
}

interface Limits {
  readBatchSize: number;
  bufferMaxLength: number;
  bufferUsageLimit: number;
  readTimeout: string;
}

interface Lifecycle {
  deleteGracePeriodSeconds: number;
  desiredPhase: string;
}

interface Edge {
  from: string;
  to: string;
  conditions?: any;
}

interface Vertex {
  name: string;
  source?: Source;
  scale: _;
  udf?: Udf;
  sink?: Sink;
}

interface Sink {
  log: _;
}

interface Udf {
  container?: any;
  builtin: Builtin;
  groupBy?: any;
}

interface Builtin {
  name: string;
}

interface Source {
  generator: Generator;
}

interface Generator {
  rpu: number;
  duration: string;
  msgSize: number;
}

interface Metadata {
  name: string;
  namespace: string;
  uid: string;
  resourceVersion: string;
  generation: number;
  creationTimestamp: string;
  annotations: Annotations;
  finalizers: string[];
  managedFields: ManagedField[];
}

interface ManagedField {
  manager: string;
  operation: string;
  apiVersion: string;
  time: string;
  fieldsType: string;
  fieldsV1: FieldsV1;
  subresource?: string;
}

interface FieldsV1 {
  "f:metadata"?: Fmetadata;
  "f:spec"?: Fspec;
  "f:status"?: Fstatus;
}

interface Fstatus {
  ".": _;
  "f:conditions": _;
  "f:lastUpdated": _;
  "f:phase": _;
  "f:sinkCount": _;
  "f:sourceCount": _;
  "f:udfCount": _;
  "f:vertexCount": _;
}

interface Fspec {
  "."?: _;
  "f:edges"?: _;
  "f:lifecycle"?: Flifecycle;
  "f:limits"?: Flimits;
  "f:watermark"?: Fwatermark;
  "f:vertices"?: _;
}

interface Fwatermark {
  ".": _;
  "f:disabled": _;
  "f:maxDelay": _;
}

interface Flimits {
  ".": _;
  "f:bufferMaxLength": _;
  "f:bufferUsageLimit": _;
  "f:readBatchSize": _;
  "f:readTimeout": _;
}

interface Flifecycle {
  ".": _;
  "f:deleteGracePeriodSeconds": _;
  "f:desiredPhase": _;
}

interface Fmetadata {
  "f:annotations"?: Fannotations;
  "f:finalizers"?: Ffinalizers;
}

interface Ffinalizers {
  ".": _;
  'v:"pipeline-controller"': _;
}

interface Fannotations {
  ".": _;
  "f:kubectl.kubernetes.io/last-applied-configuration": _;
}

interface _ {}

interface Annotations {
  "kubectl.kubernetes.io/last-applied-configuration": string;
}
