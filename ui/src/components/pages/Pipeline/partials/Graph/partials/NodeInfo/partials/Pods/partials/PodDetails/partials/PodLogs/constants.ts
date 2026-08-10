export const MAX_LOGS = 1000;

export const LOG_TAIL_SIZES = [500, 1000, 2000, 5000, 10000] as const;

export const DEFAULT_LOG_TAIL_SIZE = 1000;

export const NO_LOGS_MATCHING_SEARCH =
  "No loaded log lines match this search.";

export const LOADING_LOGS = "Loading logs...";

// Matches unwrapped Figma row height (22px).
export const LOG_ROW_HEIGHT_PX = 22;
