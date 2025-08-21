import { EventType } from "@visx/event/lib/types";

export interface Pod {
  name: string;
  containers: string[];
  containerSpecMap: Map<string, PodContainerSpec>;
}

export interface PodsProps {
  namespaceId: string;
  pipelineId: string;
  vertexId: string;
  type: string;
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
