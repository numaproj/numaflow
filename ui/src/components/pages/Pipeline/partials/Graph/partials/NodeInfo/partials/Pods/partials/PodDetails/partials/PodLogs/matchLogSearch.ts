import { NO_LOGS_MATCHING_SEARCH } from "./constants";

export type LogSearchMatchInfo = {
  enabled: boolean;
  matchCount: number;
};

export function getLogSearchMatchInfo(
  orderedLogs: string[],
  search: string,
  negateSearch: boolean
): LogSearchMatchInfo {
  if (!search || negateSearch || !orderedLogs.length) {
    return { enabled: false, matchCount: 0 };
  }

  if (
    orderedLogs.length === 1 &&
    orderedLogs[0] === NO_LOGS_MATCHING_SEARCH
  ) {
    return { enabled: true, matchCount: 0 };
  }

  return { enabled: true, matchCount: orderedLogs.length };
}
