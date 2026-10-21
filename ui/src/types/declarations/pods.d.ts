import { EventType } from "@visx/event/lib/types";
import type { ReactNode } from "react";

export interface Pod {
  name: string;
  containers: string[];
  containerSpecMap: Map<string, PodContainerSpec>;
  /** RFC3339 creation time. */
  createdAt?: string;
}

export interface PodsProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  type: string;
  /** When false the view stays mounted but does not poll or open a log stream. */
  active?: boolean;
}

export interface PodContainerSpec {
  name: string;
  cpu?: string;
  cpuParsed?: number;
  memory?: string;
  memoryParsed?: number;
}

export interface ContainerProps {
  pod: Pod;
  containerName: string;
  handleContainerClick: any;
}

export interface PodDetail {
  name: string;
  containerMap: Map<string, PodContainerSpec>;
}

export interface SearchablePodsHeatMapProps {
  pods: Pod[];
  podsDetailsMap: Map<string, PodDetail>;
  onPodClick: (e: Element | EventType, pod: Pod) => void;
  selectedPod: Pod | undefined;
}

export interface PodsHeatMapProps {
  pods: Pod[];
  podsDetailsMap: Map<string, PodDetail>;
  onPodClick: (e: Element | EventType, pod: Pod) => void;
  selectedPod: Pod | undefined;
}

export interface PodHealth {
  name: string;
  pod: Pod;
  details: PodDetail;
  maxCPUPerc: number;
  maxMemPerc: number;
  container: ContainerHealth[];
}

export interface ContainerHealth {
  name: string;
  cpu: string | undefined;
  mem: string | undefined;
  cpuPercent?: number | undefined;
  memoryPercent?: number | undefined;
}

export interface Hexagon {
  name: string;
  data?: any;
  tooltip?: string;
  healthPercent?: number;
  fill?: string;
  stroke?: string;
  strokeWidth?: number;
  type?: string;
  image?: any;
}

export interface HexagonPoints {
  x: number;
  y: number;
  data?: any;
}

export interface HexagonHeatMapProps {
  data: Hexagon[];
  handleClick: any;
  tooltipComponent: any;
  tooltipClass?: string;
  selected: string | undefined;
  containerWidth: number;
}

export interface PodDetailProps {
  namespaceId: string;
  pipelineId: string;
  type: string;
  containerName: string;
  pod: Pod;
  vertexId: string;
  focusControls?: ReactNode;
}
export interface ContainerInfoProps {
  state: string;
  restartCount: number;
  lastStartedAt: string;
  lastTerminationReason?: string;
  lastTerminationMessage?: string;
  lastTerminationExitCode?: number | null;
  waitingReason?: string;
  waitingMessage?: string;
  requestedCPU?: string;
  requestedMemory?: string;
  limitCPU?: string;
  limitMemory?: string;
  totalCPU?: string;
  totalMemory?: string;
}

export interface PodSpecificInfoProps {
  name: string;
  status: string;
  message: string;
  reason: string;
  restartCount: number;
  totalCPU: string;
  totalMemory: string;
}

/** PodRuntimeInfo is the v1 pods-info response normalized for Pod View presenters. */
export interface PodRuntimeInfo {
  name: string;
  status?: string;
  message?: string;
  reason?: string;
  totalCPU?: string;
  totalMemory?: string;
  containerDetailsMap: Record<string, ContainerInfoProps>;
}

export type PodSeverity = "critical" | "warning" | "healthy" | "unknown";

export type PodFleetSort = "severity" | "cpu" | "memory" | "restarts" | "name";

/** PodFleetItem joins pod specification, live usage, and runtime status for Beta presentation. */
export interface PodFleetItem {
  pod: Pod;
  details?: PodDetail;
  runtime?: PodRuntimeInfo;
  severity: PodSeverity;
  /** Human-readable cause of the severity, shown in the chip tooltip. */
  reason?: string;
  /** Usage percentages cover user-defined containers only (`numa` is excluded). */
  maxCPUPercent?: number;
  maxMemoryPercent?: number;
  restartCount: number;
}

export type SignalTone = "ok" | "warn" | "critical" | "neutral";

export type SignalId = "latency" | "traffic" | "backlog" | "loss" | "errors";

/** InspectTarget is where a vertex health card navigates when inspected. */
export type InspectTarget =
  | { kind: "metrics"; metric: string; presets?: Record<string, string> }
  | { kind: "tab"; tab: "processingRates" | "errors" | "buffers" }
  | { kind: "critical-pods" };

export interface HealthCard {
  id: SignalId;
  index: string;
  title: string;
  value: string;
  detail: string;
  /** Status of the signal; cards keep identity colors and only tint the detail. */
  tone: SignalTone;
  /** Bar heights as percentages of the largest bucket; omitted without a series. */
  sparkline?: number[];
  stages?: { name: string; value: string }[];
  stagesNote?: string;
  /** Outcome metrics and their semantics, rendered in the Message outcomes card tooltip. */
  outcomes?: {
    name: string;
    value: string;
    description: string;
    available: boolean;
  }[];
  inspect?: InspectTarget;
}

/** VertexHealth is the presentation model for the Beta Pod View vertex health strip. */
export interface VertexHealth {
  tone: SignalTone;
  verdict?: { label: string; tone: SignalTone };
  replicas?: { label: string; tone: SignalTone };
  meta: string;
  cards: HealthCard[];
}

export interface PodInfoProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  type: string;
  pod: Pod;
  podDetails: PodDetail;
  containerName: string;
  containerInfo: ContainerInfoProps;
  podSpecificInfo: PodSpecificInfoProps;
}

export interface PodLogsProps {
  namespaceId: string;
  podName: string;
  containerName: string;
  type: string;
  /** Rendered only inside the Focus logs dialog, never in the sidebar view. */
  focusControls?: ReactNode;
}

export interface ResourceUsage {
  cpuPercent?: number;
  memoryPercent?: number;
}

export interface ContainerError {
  container: string;
  timestamp: string;
  code: string;
  message: string;
  details: string;
}

export interface ReplicaErrors {
  replica: string;
  containerErrors: ContainerError[];
}

export interface ErrorsFetchResult {
  data?: ReplicaErrors[];
  loading: boolean;
  error: any;
  refresh: () => void;
}
