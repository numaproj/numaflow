import { useCallback, useEffect, useState } from "react";
import { getLogSearchMatchInfo } from "./matchLogSearch";

type UseLogSearchNavigationArgs = {
  orderedLogs: string[];
  search: string;
  negateSearch: boolean;
  resetKey: string;
};

export function useLogSearchNavigation({
  orderedLogs,
  search,
  negateSearch,
  resetKey,
}: UseLogSearchNavigationArgs) {
  const { enabled, matchCount } = getLogSearchMatchInfo(
    orderedLogs,
    search,
    negateSearch
  );
  const [currentMatch, setCurrentMatch] = useState(0);

  useEffect(() => {
    setCurrentMatch(enabled ? 1 : 0);
  }, [enabled, resetKey]);

  useEffect(() => {
    setCurrentMatch((previousMatch) => {
      if (!enabled || !matchCount) {
        return 0;
      }

      return previousMatch > 0 ? Math.min(previousMatch, matchCount) : 1;
    });
  }, [enabled, matchCount]);

  const goNext = useCallback((): number | null => {
    if (!enabled || !matchCount) {
      return null;
    }

    const nextMatch = currentMatch >= matchCount ? 1 : currentMatch + 1;
    setCurrentMatch(nextMatch);
    return nextMatch - 1;
  }, [enabled, matchCount, currentMatch]);

  const goPrev = useCallback((): number | null => {
    if (!enabled || !matchCount) {
      return null;
    }

    const previousMatch =
      currentMatch <= 1 ? matchCount : currentMatch - 1;
    setCurrentMatch(previousMatch);
    return previousMatch - 1;
  }, [enabled, matchCount, currentMatch]);

  return {
    enabled,
    matchCount,
    currentMatch,
    activeIndex: currentMatch > 0 ? currentMatch - 1 : null,
    goNext,
    goPrev,
  };
}
