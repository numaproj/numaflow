import {
  getPodContainerUsePercentages,
} from "../../../../../../../utils";
import {
  Pod,
  PodDetail,
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

const hasCriticalStatus = (status?: string) =>
  Boolean(status && /(crash|error|fail|oom)/i.test(status));

const hasWarningStatus = (status?: string) =>
  Boolean(status && /(pending|waiting|unknown)/i.test(status));

export function classifyPod(
  maxCPUPercent: number | undefined,
  maxMemoryPercent: number | undefined,
  runtime?: PodRuntimeInfo,
  restartCount = 0
): PodSeverity {
  if (hasCriticalStatus(runtime?.status)) return "critical";
  if (
    (maxCPUPercent !== undefined && maxCPUPercent > 75) ||
    (maxMemoryPercent !== undefined && maxMemoryPercent > 85)
  ) {
    return "critical";
  }
  if (hasWarningStatus(runtime?.status) || restartCount > 0) return "warning";
  if (
    (maxCPUPercent !== undefined && maxCPUPercent > 30) ||
    (maxMemoryPercent !== undefined && maxMemoryPercent > 50)
  ) {
    return "warning";
  }
  if (!runtime?.status || (maxCPUPercent === undefined && maxMemoryPercent === undefined)) {
    return "unknown";
  }
  return runtime.status.toLowerCase() === "running" ? "healthy" : "warning";
}

/** buildPodFleetItems derives display data without mutating the v1 fetch results. */
export function buildPodFleetItems(
  pods: Pod[] = [],
  detailsByName?: Map<string, PodDetail>,
  runtimeByName: Map<string, PodRuntimeInfo> = new Map()
): PodFleetItem[] {
  return pods.map((pod) => {
    const details = detailsByName?.get(pod.name);
    let maxCPUPercent: number | undefined;
    let maxMemoryPercent: number | undefined;
    details?.containerMap.forEach((_container, name) => {
      const usage = getPodContainerUsePercentages(pod, details, name);
      if (usage.cpuPercent !== undefined) {
        maxCPUPercent = Math.max(maxCPUPercent ?? 0, usage.cpuPercent);
      }
      if (usage.memoryPercent !== undefined) {
        maxMemoryPercent = Math.max(maxMemoryPercent ?? 0, usage.memoryPercent);
      }
    });
    const runtime = runtimeByName.get(pod.name);
    const restartCount = Object.values(runtime?.containerDetailsMap || {}).reduce(
      (count, container) => count + (container?.restartCount || 0),
      0
    );
    return {
      pod,
      details,
      runtime,
      maxCPUPercent,
      maxMemoryPercent,
      restartCount,
      severity: classifyPod(maxCPUPercent, maxMemoryPercent, runtime, restartCount),
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
