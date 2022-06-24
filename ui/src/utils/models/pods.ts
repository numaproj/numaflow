export interface Pod {
    name: string;
    containers: string[];
    containerSpecMap: Map<string, PodContainerSpec>;
}

export interface PodContainerSpec {
    name: string;
    cpu?: string;
    cpuParsed?: number;
    memory?: string;
    memoryParsed?: number;
}

export interface ResourceUsage {
    cpuPercent?: number;
    memoryPercent?: number;
}

export interface PodDetail {
    name: string;
    containerMap: Map<string, PodContainerSpec>;
}