import { Pod, PodDetail } from "./pods";
import { EventType } from "@visx/event/lib/types";

export interface PodsHeatMapProps {
  pods: Pod[];
  podsDetailMap: Map<string, PodDetail>;
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
