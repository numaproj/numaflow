import React from "react";

export enum IndicatorStatus {
  HEALTHY = "HEALTHY",
  WARNING = "WARNING",
  CRITICAL = "CRITICAL",
  RUNNING = "RUNNING",
  STOPPED = "STOPPED",
  PAUSED = "PAUSED",
}
export const IndicatorColorMap = {
  HEALTHY: "#00C14E",
  RUNNING: "#00C14E",
  WARNING: "#FEE508",
  PAUSED: "#FEE508",
  CRITICAL: "#D1071F",
  STOPPED: "#D1071F",
};

export interface StatusIndicatorProps {
  status: IndicatorStatus;
}
export function StatusIndicator({ status }: StatusIndicatorProps) {
  return (
    <svg height="10" width="10">
      <circle cx="5" cy="5" r="4" fill={IndicatorColorMap[status]} />
    </svg>
  );
}
