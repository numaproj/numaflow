import React from "react";
import { Pod, PodDetail, ResourceUsage } from "../types/declarations/pods";
import circleCheck from "../../src/images/checkmark-circle.png";
import circleDash from "../../src/images/circle-dash.png";
import heartFill from "../../src/images/heart-fill.png";
import warning from "../../src/images/warning-circle.png";
import critical from "../../src/images/critical.png";
import heartFill0 from "../../src/images/heart-fill0.png";
import warning0 from "../../src/images/warning0.png";
import critical0 from "../../src/images/critical0.png";
import moment from "moment";
import { IsbServiceSpec } from "../types/declarations/pipeline";
import { styled } from "@mui/material/styles";
import Tooltip, { tooltipClasses, TooltipProps } from "@mui/material/Tooltip";

// global constants
export const ALL = "All";
export const RUNNING = "Running";
export const ACTIVE = "Running";
export const INACTIVE = "Not-Running";
export const INACTIVE_STATUS = "inactive";
export const UNHEALTHY = "unhealthy";
export const HEALTHY = "healthy";
export const WARNING = "warning";
export const CRITICAL0 = "critical0";
export const HEALTHY0 = "healthy0";
export const WARNING0 = "warning0";
export const CRITICAL = "critical";
export const SUCCEEDED = "Succeeded";
export const FAILED = "Failed";
export const PENDING = "Pending";
export const PAUSING = "Pausing";
export const PAUSED = "Paused";
export const DELETING = "Deleting";
export const UNKNOWN = "Unknown";
export const STOPPED = "Stopped";
export const WITH_PIPELINES = "With Pipelines";
export const NO_PIPELINES = "No Pipelines";
export const DEFAULT_ISB = "default";

// ISB types
export const JETSTREAM = "jetstream";
export const REDIS = "redis";

// sorting constatnts
export const ASC = "asc";
export const DESC = "desc";
export const ALPHABETICAL_SORT = "alphabetical";
export const LAST_UPDATED_SORT = "lastUpdated";
export const LAST_CREATED_SORT = "lastCreated";

export function getBaseHref(): string {
  const runtimeConfig = window.__RUNTIME_CONFIG__;
  if (runtimeConfig?.BASE_HREF) {
    let baseHref = runtimeConfig.BASE_HREF;
    if (baseHref.endsWith("/")) {
      baseHref = baseHref.slice(0, -1);
    }
    return baseHref;
  }
  return "";
}

export async function getAPIResponseError(
  response: Response
): Promise<string | undefined> {
  try {
    if (!response.ok) {
      let message = `Response code: ${response.status}`;
      try {
        const data = await response.json();
        if (data.errMsg) {
          message =
            response.status === 403 ? `Error: ${data.errMsg}` : data.errMsg;
        }
      } catch (e) {
        // Ignore
      }
      return message;
    } else {
      const data = await response.json();
      if (data.errMsg) {
        return `Error: ${data.errMsg}`;
      } else {
        return "";
      }
    }
  } catch (e: any) {
    return `Error: ${e.message}`;
  }
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
  if (
    podDetails?.containerMap instanceof Map &&
    pod.containerSpecMap instanceof Map
  ) {
    const usedCPUParsed: number | undefined =
      podDetails?.containerMap?.get(containerName)?.cpuParsed;
    const specCPUParsed: number | undefined =
      pod.containerSpecMap?.get(containerName)?.cpuParsed;
    let cpuPercent: number | undefined;
    if (usedCPUParsed && specCPUParsed) {
      cpuPercent = (usedCPUParsed / specCPUParsed) * 100;
    }
    const usedMemParsed: number | undefined =
      podDetails?.containerMap?.get(containerName)?.memoryParsed;
    const specMemParsed: number | undefined =
      pod.containerSpecMap?.get(containerName)?.memoryParsed;
    let memoryPercent: number | undefined;
    if (usedMemParsed && specMemParsed) {
      memoryPercent = (usedMemParsed / specMemParsed) * 100;
    }
    return {
      cpuPercent,
      memoryPercent,
    };
  }
  return {
    cpuPercent: undefined,
    memoryPercent: undefined,
  };
}

export function a11yProps(index: number) {
  return {
    id: `info-tab-${index}`,
    "aria-controls": `info-tabpanel-${index}`,
  };
}

// icon maps for each status
export const IconsStatusMap: any = {
  [RUNNING]: circleCheck,
  [SUCCEEDED]: circleCheck,
  [FAILED]: critical,
  [PAUSING]: circleDash,
  [DELETING]: circleDash,
  [PENDING]: circleDash,
  [PAUSED]: circleDash,
  [INACTIVE]: circleDash,
  [INACTIVE_STATUS]: circleDash,
  [UNHEALTHY]: critical,
  [HEALTHY]: heartFill,
  [WARNING]: warning,
  [CRITICAL]: critical,
  [HEALTHY0]: heartFill0,
  [WARNING0]: warning0,
  [CRITICAL0]: critical0,
  [STOPPED]: circleDash,
  [UNKNOWN]: circleDash,
};

interface StatusStringType {
  [index: string]: string;
}

export const StatusString: StatusStringType = {
  [ALL]: "All",
  [RUNNING]: "Running",
  [SUCCEEDED]: "Succeeded",
  [FAILED]: "Failed",
  [PENDING]: "Pending",
  [PAUSING]: "Pausing",
  [DELETING]: "Deleting",
  [PAUSED]: "Paused",
  [STOPPED]: "Stopped",
  [INACTIVE]: "Inactive",
  [INACTIVE_STATUS]: "Inactive",
  [UNHEALTHY]: "Unhealthy",
  [HEALTHY]: "Healthy",
  [WARNING]: "Warning",
  [CRITICAL]: "Critical",
  [UNKNOWN]: "Unknown",
  [WARNING0]: "Warning",
  [CRITICAL0]: "Critical",
  [HEALTHY0]: "Healthy",
};

export const ISBStatusString: StatusStringType = {
  [RUNNING]: "Running",
  [FAILED]: "Failed",
  [PENDING]: "Pending",
  [HEALTHY]: "Healthy",
  [WARNING]: "Warning",
  [CRITICAL]: "Critical",
  [UNKNOWN]: "Unknown",
  [WARNING0]: "Warning",
  [CRITICAL0]: "Critical",
  [HEALTHY0]: "Healthy",
};

// returns the duration string in the format of 1d 2hr 3min 4sec 5ms
export const DurationString = (duration: number): string => {
  const diff = moment.duration(duration);
  const years = diff.years();
  const months = diff.months();
  const days = diff.days();
  const hours = diff.hours();
  const minutes = diff.minutes();
  const seconds = diff.seconds();
  const milliseconds = diff.milliseconds();

  if (years > 0) {
    return `${years}yr ${months}mo`;
  } else if (months > 0) {
    return `${months}mo ${days}d`;
  } else if (days > 0) {
    return `${days}d ${hours}hr`;
  } else if (hours > 0) {
    return `${hours}hr ${minutes}min`;
  } else if (minutes > 0) {
    return `${minutes}min ${seconds}sec`;
  } else if (seconds > 0) {
    return `${seconds}sec ${milliseconds}ms`;
  } else {
    return `${milliseconds}ms`;
  }
};

const compareIgnoreCase = (a: string, b: string): boolean => {
  return a.toLowerCase() === b.toLowerCase();
};

export const GetConsolidatedHealthStatus = (
  pipelineStatus: string,
  resourceHealthStatus: string,
  dataHealthStatus: string
): string => {
  if (!pipelineStatus || !resourceHealthStatus || !dataHealthStatus) {
    return UNKNOWN;
  }
  if (
    compareIgnoreCase(pipelineStatus, PAUSING) ||
    compareIgnoreCase(pipelineStatus, PAUSED)
  ) {
    return INACTIVE_STATUS;
  }

  if (
    compareIgnoreCase(pipelineStatus, DELETING) ||
    compareIgnoreCase(resourceHealthStatus, DELETING)
  ) {
    return DELETING;
  }

  if (
    compareIgnoreCase(resourceHealthStatus, HEALTHY) &&
    compareIgnoreCase(dataHealthStatus, HEALTHY)
  ) {
    return HEALTHY;
  }

  if (compareIgnoreCase(resourceHealthStatus, UNHEALTHY)) {
    return UNHEALTHY;
  }

  if (
    compareIgnoreCase(resourceHealthStatus, CRITICAL) ||
    compareIgnoreCase(dataHealthStatus, CRITICAL)
  ) {
    return CRITICAL;
  }

  if (
    compareIgnoreCase(resourceHealthStatus, WARNING) ||
    compareIgnoreCase(dataHealthStatus, WARNING)
  ) {
    return WARNING;
  }

  return UNKNOWN;
};

export const GetISBType = (spec: IsbServiceSpec): string | null => {
  if (spec?.jetstream) {
    return JETSTREAM;
  } else if (spec?.redis) {
    return REDIS;
  }
  return null;
};
export const timeAgo = (timestamp: string) => {
  const time = +new Date(timestamp);

  const time_formats = [
    [60, "seconds", 1], // 60
    [120, "1 minute ago", "1 minute from now"], // 60*2
    [3600, "minutes", 60], // 60*60, 60
    [7200, "1 hour ago", "1 hour from now"], // 60*60*2
    [86400, "hours", 3600], // 60*60*24, 60*60
    [172800, "Yesterday", "Tomorrow"], // 60*60*24*2
    [604800, "days", 86400], // 60*60*24*7, 60*60*24
    [1209600, "Last week", "Next week"], // 60*60*24*7*4*2
    [2419200, "weeks", 604800], // 60*60*24*7*4, 60*60*24*7
    [4838400, "Last month", "Next month"], // 60*60*24*7*4*2
    [29030400, "months", 2419200], // 60*60*24*7*4*12, 60*60*24*7*4
    [58060800, "Last year", "Next year"], // 60*60*24*7*4*12*2
    [2903040000, "years", 29030400], // 60*60*24*7*4*12*100, 60*60*24*7*4*12
    [5806080000, "Last century", "Next century"], // 60*60*24*7*4*12*100*2
    [58060800000, "centuries", 2903040000], // 60*60*24*7*4*12*100*20, 60*60*24*7*4*12*100
  ];
  let seconds = (+new Date() - time) / 1000,
    token = "ago",
    list_choice = 1;

  if (seconds == 0) {
    return "Just now";
  }
  if (seconds < 0) {
    seconds = Math.abs(seconds);
    token = "from now";
    list_choice = 2;
  }
  let i = 0,
    format;
  while ((format = time_formats[i++]))
    if (seconds < +format[0]) {
      if (typeof format[2] == "string") return format[list_choice];
      else
        return Math.floor(seconds / format[2]) + " " + format[1] + " " + token;
    }
  return time;
};

export function formatDuration(seconds: number, precision = 1) {
  const timeUnits = [
    { unit: "d", seconds: 86400 },
    { unit: "h", seconds: 3600 },
    { unit: "m", seconds: 60 },
    { unit: "s", seconds: 1 },
  ];

  let remainingSeconds = Math.abs(Math.round(seconds));
  const figs = [];

  for (const { unit, seconds: unitSeconds } of timeUnits) {
    if (
      remainingSeconds >= unitSeconds ||
      (unit === "s" && Math.round(seconds) === 0)
    ) {
      const value = Math.floor(remainingSeconds / unitSeconds);
      figs.push(`${value}${unit}`);
      remainingSeconds %= unitSeconds;
    }
  }

  return figs.slice(0, precision).join(" ");
}

export const ago = (date: Date, precision?: number) => {
  if (isNaN(date.getTime())) return "Invalid Date";

  const secondsAgo = (new Date().getTime() - date.getTime()) / 1000;
  const duration = formatDuration(secondsAgo, precision);

  if (secondsAgo < 0) return "in " + duration;
  else return duration + " ago";
};

export const PIPELINE_STATUS_TOOLTIP =
  "Running pipeline health is determined by backpressure. Non-Running pipelines are failed, pausing, paused, or deleting.";
export const ISB_SERVICES_TOOLTIP =
  "Inter State Buffer Services are used to transfer data between vertices in a pipeline.";
export const ISB_SERVICES_STATUS_TOOLTIP = (
  <React.Fragment>
    <>
      <b>Healthy:</b>&nbsp; The ISB service is operating optimally. No issues or
      anomalies detected.
      <hr />
      <b>Warning:</b>&nbsp; The ISB service is experiencing minor issues or
      degradation within the data processing pipeline. Consider monitoring and
      further investigation.
      <hr />
      <b>Critical:</b>&nbsp; The ISB service is in a critical state. Immediate
      attention required.
    </>
  </React.Fragment>
);
export const MAX_LAG_TOOLTIP =
  "Max lag is the amount of time between the oldest and newest data that is being processed on.";
export const ERRORS_TOOLTIP =
  "Errors that occur when attempting to retrieve data";
// export const PIPELINES_TOOLTIP = "Pipelines list"
// export const ISB_TOOLTIP = "Inter State Buffer services list"

export const HTMLlTooltip = styled(({ className, ...props }: TooltipProps) => (
  <Tooltip {...props} classes={{ popper: className }} />
))(() => ({
  [`& .${tooltipClasses.tooltip}`]: {
    fontSize: 14,
  },
}));
