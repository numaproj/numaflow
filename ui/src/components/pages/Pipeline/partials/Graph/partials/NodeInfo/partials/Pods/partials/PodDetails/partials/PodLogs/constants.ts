export const MAX_LOGS = 1000;

export const LOG_HISTORY_BATCH_SIZES = [500, 1000, 2000, 5000] as const;

export const DEFAULT_LOG_HISTORY_BATCH_SIZE = 500;

export const MAX_TOTAL_LOGS = 10000;

export const NO_LOGS_MATCHING_SEARCH =
  "No loaded log lines match this search.";

export const LOADING_LOGS = "Loading logs...";

export const LOADING_OLDER_LOGS = "Loading…";

export const END_OF_RETAINED_LOGS = "No more retained lines";

// Matches unwrapped Figma row height (22px).
export const LOG_ROW_HEIGHT_PX = 22;
