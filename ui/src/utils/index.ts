import { Pod, PodDetail, ResourceUsage } from "../types/declarations/pods";
import circleCheck from "../../src/images/checkmark-circle.png";
import circleDash from "../../src/images/circle-dash.png";
import heartFill from "../../src/images/heart-fill.png";
import warning from "../../src/images/warning-circle.png";
import critical from "../../src/images/critical.png";

// global constants
export const RUNNING = "Running"
export const ACTIVE = "active"
export const INACTIVE = "inactive"
export const HEALTHY = "healthy"
export const WARNING = "warning"
export const CRITICAL = "critical"

export function getBaseHref(): string {
  if (window.__RUNTIME_CONFIG__?.BASE_HREF) {
    return window.__RUNTIME_CONFIG__.BASE_HREF;
  }
  return "/";
}

export function isDev() {
  return !process.env.NODE_ENV || process.env.NODE_ENV === "development";
}

export function handleCopy(copy: any) {
  navigator.clipboard.writeText(JSON.stringify(copy.src, null, "\t"));
}

export function findSuffix(quantity: string): string {
  let ix = quantity.length - 1;
  // eslint-disable-next-line no-useless-escape
  while (ix >= 0 && !/[\.0-9]/.test(quantity.charAt(ix))) {
    ix--;
  }
  return ix === -1 ? "" : quantity.substring(ix + 1);
}

export function quantityToScalar(quantity: string): number | bigint {
  if (!quantity) {
    return 0;
  }
  const suffix = findSuffix(quantity);
  if (suffix === "") {
    const num = Number(quantity).valueOf();
    if (isNaN(num)) {
      throw new Error("Unknown quantity " + quantity);
    }
    return num;
  }
  switch (suffix) {
    case "n":
      return (
        Number(quantity.substr(0, quantity.length - 1)).valueOf() /
        1_000_000_000.0
      );
    case "u":
      return (
        Number(quantity.substr(0, quantity.length - 1)).valueOf() / 1_000_000.0
      );
    case "m":
      return Number(quantity.substr(0, quantity.length - 1)).valueOf() / 1000.0;
    case "k":
      return BigInt(quantity.substr(0, quantity.length - 1)) * BigInt(1000);
    case "M":
      return (
        BigInt(quantity.substr(0, quantity.length - 1)) * BigInt(1000 * 1000)
      );
    case "G":
      return (
        BigInt(quantity.substr(0, quantity.length - 1)) *
        BigInt(1000 * 1000 * 1000)
      );
    case "T":
      return (
        BigInt(quantity.substr(0, quantity.length - 1)) *
        BigInt(1000 * 1000 * 1000) *
        BigInt(1000)
      );
    case "P":
      return (
        BigInt(quantity.substr(0, quantity.length - 1)) *
        BigInt(1000 * 1000 * 1000) *
        BigInt(1000 * 1000)
      );
    case "E":
      return (
        BigInt(quantity.substr(0, quantity.length - 1)) *
        BigInt(1000 * 1000 * 1000) *
        BigInt(1000 * 1000 * 1000)
      );
    case "Ki":
      return BigInt(quantity.substr(0, quantity.length - 2)) * BigInt(1024);
    case "Mi":
      return (
        BigInt(quantity.substr(0, quantity.length - 2)) * BigInt(1024 * 1024)
      );
    case "Gi":
      return (
        BigInt(quantity.substr(0, quantity.length - 2)) *
        BigInt(1024 * 1024 * 1024)
      );
    case "Ti":
      return (
        BigInt(quantity.substr(0, quantity.length - 2)) *
        BigInt(1024 * 1024 * 1024) *
        BigInt(1024)
      );
    case "Pi":
      return (
        BigInt(quantity.substr(0, quantity.length - 2)) *
        BigInt(1024 * 1024 * 1024) *
        BigInt(1024 * 1024)
      );
    case "Ei":
      return (
        BigInt(quantity.substr(0, quantity.length - 2)) *
        BigInt(1024 * 1024 * 1024) *
        BigInt(1024 * 1024 * 1024)
      );
    default:
      throw new Error(`Unknown suffix: ${suffix}`);
  }
}

export function getPodContainerUsePercentages(
  pod: Pod,
  podDetails: PodDetail,
  containerName: string
): ResourceUsage {
  const usedCPUParsed: number | undefined =
    podDetails?.containerMap?.get(containerName)?.cpuParsed;
  const specCPUParsed: number | undefined =
    pod.containerSpecMap.get(containerName)?.cpuParsed;
  let cpuPercent: number | undefined;
  if (usedCPUParsed && specCPUParsed) {
    cpuPercent = (usedCPUParsed / specCPUParsed) * 100;
  }
  const usedMemParsed: number | undefined =
    podDetails?.containerMap?.get(containerName)?.memoryParsed;
  const specMemParsed: number | undefined =
    pod.containerSpecMap.get(containerName)?.memoryParsed;
  let memoryPercent: number | undefined;
  if (usedMemParsed && specMemParsed) {
    memoryPercent = (usedMemParsed / specMemParsed) * 100;
  }
  return {
    cpuPercent,
    memoryPercent,
  };
}

export function a11yProps(index: number) {
  return {
    id: `info-tab-${index}`,
    "aria-controls": `info-tabpanel-${index}`,
  };
}

// icon maps for each status
export const IconsStatusMap = {
  [RUNNING]: circleCheck,
  [ACTIVE]: circleCheck,
  [INACTIVE]: circleDash,
  [HEALTHY]: heartFill,
  [WARNING]: warning,
  [CRITICAL]: critical,
};
