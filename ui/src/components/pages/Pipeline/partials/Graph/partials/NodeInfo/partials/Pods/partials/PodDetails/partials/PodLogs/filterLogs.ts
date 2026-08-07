import { NO_LOGS_MATCHING_SEARCH } from "./constants";

export function filterLogs(
  logs: string[],
  search: string,
  negateSearch: boolean
): string[] {
  if (!search) {
    return logs;
  }
  const searchLowerCase = search.toLowerCase();
  const filtered = logs.filter((log) =>
    negateSearch
      ? !log.toLowerCase().includes(searchLowerCase)
      : log.toLowerCase().includes(searchLowerCase)
  );

  if (!filtered.length) {
    return [NO_LOGS_MATCHING_SEARCH];
  }
  return filtered;
}
