import { resourceRequestPercent } from "../../../../../../../utils";
import {
  Pod,
  PodFleetItem,
  PodFleetSort,
  PodRuntimeInfo,
  PodSeverity,
} from "../../../../../../../types/declarations/pods";

export type PodFleetFilter = "all" | PodSeverity;

const severityOrder: Record<PodSeverity, number> = {
  critical: 0,
  warning: 1,
  healthy: 2,
  unknown: 3,
};

export type PodStatusTone = "running" | "failed" | "neutral";

/** chipLabel shows the replica number that fits a 38px fleet chip. */
export function chipLabel(podName: string): string {
  const parts = podName.split("-");
  for (let index = parts.length - 1; index >= 0; index -= 1) {
    if (/^\d+$/.test(parts[index])) return parts[index];
  }
  return parts[parts.length - 1] || podName;
}

/** podStatusTone maps a Kubernetes pod status to the design's status color. */
export function podStatusTone(status?: string): PodStatusTone {
  if (!status) return "neutral";
  if (status.toLowerCase() === "running") return "running";
  if (/(crash|error|fail|oom)/i.test(status)) return "failed";
  return "neutral";
}

/**
 * unhealthyReason returns why a pod is not healthy: a phase other than Running, or any
 * container that is not running (e.g. waiting in CrashLoopBackOff). Undefined means healthy
 * or not yet known.
 */
export function unhealthyReason(runtime?: PodRuntimeInfo): string | undefined {
  if (!runtime?.status) return undefined;
  if (runtime.status !== "Running") return runtime.reason || runtime.status;
  for (const [name, info] of Object.entries(runtime.containerDetailsMap || {})) {
    if (info?.state && info.state !== "Running") {
      return `${name}: ${info.waitingReason || info.lastTerminationReason || info.state}`;
    }
  }
  return undefined;
}

export interface ClassifyPodInput {
  runtime?: PodRuntimeInfo;
  /** Highest CPU/memory usage among all containers, as % of request. */
  maxCPUPercent?: number;
  maxMemoryPercent?: number;
  maxCPUContainer?: string;
  maxMemoryContainer?: string;
}

/**
 * classifyPod applies the fleet colour rules in precedence order Red > Yellow > Green:
 * critical (pod unhealthy), warning (a container above 80% of its CPU or memory
 * request), healthy otherwise. Pods without runtime status are unknown unless an earlier rule
 * already matched.
 */
export function classifyPod({
  runtime,
  maxCPUPercent,
  maxMemoryPercent,
  maxCPUContainer,
  maxMemoryContainer,
}: ClassifyPodInput): { severity: PodSeverity; reason?: string } {
  const unhealthy = unhealthyReason(runtime);
  if (unhealthy) return { severity: "critical", reason: unhealthy };

  const overCPU = maxCPUPercent !== undefined && maxCPUPercent > 80;
  const overMemory = maxMemoryPercent !== undefined && maxMemoryPercent > 80;
  if (overCPU || overMemory) {
    const parts = [
      overCPU
        ? `${maxCPUContainer ? `${maxCPUContainer} ` : ""}CPU ${Math.round(
            maxCPUPercent as number
          )}%`
        : undefined,
      overMemory
        ? `${maxMemoryContainer ? `${maxMemoryContainer} ` : ""}memory ${Math.round(
            maxMemoryPercent as number
          )}%`
        : undefined,
    ].filter(Boolean);
    return { severity: "warning", reason: `${parts.join(", ")} of request` };
  }

  if (!runtime?.status) return { severity: "unknown" };
  return { severity: "healthy" };
}

/** buildPodFleetItems derives display data from the pods-info runtime payload. */
export function buildPodFleetItems(
  pods: Pod[] = [],
  runtimeByName: Map<string, PodRuntimeInfo> = new Map()
): PodFleetItem[] {
  return pods.map((pod) => {
    const runtime = runtimeByName.get(pod.name);
    let maxCPUPercent: number | undefined;
    let maxMemoryPercent: number | undefined;
    let maxCPUContainer: string | undefined;
    let maxMemoryContainer: string | undefined;
    const containerNames = new Set([
      ...pod.containers,
      ...Array.from(pod.containerSpecMap?.keys?.() || []),
      ...Object.keys(runtime?.containerDetailsMap || {}),
    ]);
    const containerResources = Array.from(containerNames).map((name) => {
      const runtimeContainer = runtime?.containerDetailsMap?.[name];
      const cpuPercent = resourceRequestPercent(
        runtimeContainer?.totalCPU,
        runtimeContainer?.requestedCPU
      );
      const memoryPercent = resourceRequestPercent(
        runtimeContainer?.totalMemory,
        runtimeContainer?.requestedMemory
      );
      if (cpuPercent !== undefined && (maxCPUPercent === undefined || cpuPercent > maxCPUPercent)) {
        maxCPUPercent = cpuPercent;
        maxCPUContainer = name;
      }
      if (
        memoryPercent !== undefined &&
        (maxMemoryPercent === undefined || memoryPercent > maxMemoryPercent)
      ) {
        maxMemoryPercent = memoryPercent;
        maxMemoryContainer = name;
      }
      return { name, cpuPercent, memoryPercent };
    });
    const restartCount = Object.values(runtime?.containerDetailsMap || {}).reduce(
      (count, container) => count + (container?.restartCount || 0),
      0
    );
    const { severity, reason } = classifyPod({
      runtime,
      maxCPUPercent,
      maxMemoryPercent,
      maxCPUContainer,
      maxMemoryContainer,
    });
    return {
      pod,
      runtime,
      maxCPUPercent,
      maxMemoryPercent,
      containerResources,
      restartCount,
      severity,
      reason,
    };
  });
}

export function filterAndSortPodFleet(
  items: PodFleetItem[],
  filter: PodFleetFilter,
  search: string,
  sort: PodFleetSort
): PodFleetItem[] {
  const normalizedSearch = search.trim().toLowerCase();
  return items
    .filter((item) => filter === "all" || item.severity === filter)
    .filter((item) => !normalizedSearch || item.pod.name.toLowerCase().includes(normalizedSearch))
    .slice()
    .sort((left, right) => {
      switch (sort) {
        case "cpu":
          return (right.maxCPUPercent ?? -1) - (left.maxCPUPercent ?? -1);
        case "memory":
          return (right.maxMemoryPercent ?? -1) - (left.maxMemoryPercent ?? -1);
        case "restarts":
          return right.restartCount - left.restartCount;
        case "name":
          return left.pod.name.localeCompare(right.pod.name);
        default:
          return (
            severityOrder[left.severity] - severityOrder[right.severity] ||
            left.pod.name.localeCompare(right.pod.name)
          );
      }
    });
}
