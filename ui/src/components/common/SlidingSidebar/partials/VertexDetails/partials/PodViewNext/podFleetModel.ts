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

const isCrashLoop = (status?: string) => Boolean(status && /crash/i.test(status));

const hasCriticalStatus = (status?: string) =>
  Boolean(status && /(error|fail|oom)/i.test(status));

const hasWarningStatus = (status?: string) =>
  Boolean(status && /(pending|waiting|unknown)/i.test(status));

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

export function classifyPod(
  maxCPUPercent: number | undefined,
  maxMemoryPercent: number | undefined,
  runtime?: PodRuntimeInfo,
  _restartCount = 0
): PodSeverity {
  if (isCrashLoop(runtime?.status)) return "unknown";
  if (hasCriticalStatus(runtime?.status)) return "critical";
  if (
    (maxCPUPercent !== undefined && maxCPUPercent > 75) ||
    (maxMemoryPercent !== undefined && maxMemoryPercent > 85)
  ) {
    return "critical";
  }
  if (hasWarningStatus(runtime?.status)) return "warning";
  if (
    (maxCPUPercent !== undefined && maxCPUPercent > 30) ||
    (maxMemoryPercent !== undefined && maxMemoryPercent > 50)
  ) {
    return "warning";
  }
  if (!runtime?.status) return "unknown";
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
