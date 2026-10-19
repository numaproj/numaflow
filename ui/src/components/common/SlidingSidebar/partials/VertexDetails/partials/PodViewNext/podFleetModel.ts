import {
  getPodContainerUsePercentages,
} from "../../../../../../../utils";
import {
  Pod,
  PodDetail,
  PodFleetItem,
  PodFleetSort,
  PodReadRates,
  PodRuntimeInfo,
  PodSeverity,
} from "../../../../../../../types/declarations/pods";

export type PodFleetFilter = "all" | PodSeverity;

/** Platform containers are excluded from CPU/memory severity; only user-defined containers count. */
export const PLATFORM_CONTAINERS = new Set(["numa"]);

/** A pod must read nothing for this long, and be at least this old, before it is idle. */
export const IDLE_WINDOW_MS = 15 * 60_000;

export const IDLE_HINT =
  "No messages read in the last 15 min. This usually means there are more replicas than source partitions; reduce replicas or add partitions.";

const severityOrder: Record<PodSeverity, number> = {
  critical: 0,
  warning: 1,
  idle: 2,
  healthy: 3,
  unknown: 4,
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
  /** Highest CPU/memory usage among user-defined containers, as % of request. */
  maxCPUPercent?: number;
  maxMemoryPercent?: number;
  /** Undefined when per-pod rates are unavailable; idle is then never inferred. */
  readRates?: PodReadRates;
  createdAt?: string;
  now?: number;
}

/**
 * classifyPod applies the fleet colour rules in precedence order Red > Yellow > Grey > Green:
 * critical (pod unhealthy), warning (a user-defined container above 80% of its CPU or memory
 * request), idle (no reads for 15 min on a pod whose creation time shows it is at least 15 min
 * old), healthy otherwise. Pods without runtime status are unknown unless an earlier rule
 * already matched. A missing creation time never counts as old enough.
 */
export function classifyPod({
  runtime,
  maxCPUPercent,
  maxMemoryPercent,
  readRates,
  createdAt,
  now = Date.now(),
}: ClassifyPodInput): { severity: PodSeverity; reason?: string } {
  const unhealthy = unhealthyReason(runtime);
  if (unhealthy) return { severity: "critical", reason: unhealthy };

  const overCPU = maxCPUPercent !== undefined && maxCPUPercent > 80;
  const overMemory = maxMemoryPercent !== undefined && maxMemoryPercent > 80;
  if (overCPU || overMemory) {
    const parts = [
      overCPU ? `CPU ${Math.round(maxCPUPercent as number)}%` : undefined,
      overMemory ? `memory ${Math.round(maxMemoryPercent as number)}%` : undefined,
    ].filter(Boolean);
    return { severity: "warning", reason: `${parts.join(", ")} of request` };
  }

  const created = createdAt ? Date.parse(createdAt) : NaN;
  // A missing or unparseable creation time is not evidence the pod is old enough.
  const oldEnough = !Number.isNaN(created) && now - created >= IDLE_WINDOW_MS;
  if (readRates?.rate15m === 0 && oldEnough) {
    return { severity: "idle", reason: IDLE_HINT };
  }

  if (!runtime?.status) return { severity: "unknown" };
  return { severity: "healthy" };
}

/** buildPodFleetItems derives display data without mutating the v1 fetch results. */
export function buildPodFleetItems(
  pods: Pod[] = [],
  detailsByName?: Map<string, PodDetail>,
  runtimeByName: Map<string, PodRuntimeInfo> = new Map(),
  ratesByName?: Map<string, PodReadRates>,
  now: number = Date.now()
): PodFleetItem[] {
  return pods.map((pod) => {
    const details = detailsByName?.get(pod.name);
    let maxCPUPercent: number | undefined;
    let maxMemoryPercent: number | undefined;
    details?.containerMap.forEach((_container, name) => {
      if (PLATFORM_CONTAINERS.has(name)) return;
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
    // A missing entry means this pod was not in the daemon response. That is not an
    // observed zero; idle requires rate15m === 0 from a pod the daemon reported.
    const readRates = ratesByName?.get(pod.name);
    const { severity, reason } = classifyPod({
      runtime,
      maxCPUPercent,
      maxMemoryPercent,
      readRates,
      createdAt: pod.createdAt,
      now,
    });
    return {
      pod,
      details,
      runtime,
      maxCPUPercent,
      maxMemoryPercent,
      readRates,
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
