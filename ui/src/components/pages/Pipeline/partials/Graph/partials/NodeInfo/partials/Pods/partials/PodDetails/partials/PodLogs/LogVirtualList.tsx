import {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useRef,
} from "react";
import Box from "@mui/material/Box";
import Highlighter from "react-highlight-words";
import { useVirtualizer } from "@tanstack/react-virtual";
import {
  LOG_ROW_HEIGHT_PX,
  NO_LOGS_MATCHING_SEARCH,
  LOADING_LOGS,
} from "./constants";

export type LogVirtualListProps = {
  logs: string[];
  search: string;
  wrapLines: boolean;
  colorMode: string;
  podName: string;
  activeIndex: number | null;
};

export type ScrollAnchor = {
  line: string;
  index: number;
  offsetTop: number;
};

export type LogVirtualListHandle = {
  scrollToIndex: (index: number) => void;
  captureScrollAnchor: () => ScrollAnchor | null;
  restoreScrollAnchor: (
    anchor: ScrollAnchor,
    prependedCount: number,
    logsOrder: string
  ) => void;
};

export const LogVirtualList = forwardRef<
  LogVirtualListHandle,
  LogVirtualListProps
>(function LogVirtualList(
  { logs, search, wrapLines, colorMode, podName, activeIndex },
  ref
) {
  const parentRef = useRef<HTMLDivElement | null>(null);
  const logsRef = useRef(logs);
  logsRef.current = logs;
  const isDark = colorMode === "dark";
  const isEmptyState =
    logs.length === 1 &&
    (logs[0] === NO_LOGS_MATCHING_SEARCH || logs[0] === LOADING_LOGS);

  const virtualizer = useVirtualizer({
    count: isEmptyState ? 0 : logs.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => LOG_ROW_HEIGHT_PX,
    overscan: 10,
    // Non-zero seed so the first paint (and jsdom tests) can resolve rows
    // before the scroll element is measured.
    initialRect: { width: 800, height: 320 },
    measureElement: wrapLines
      ? (element) => element.getBoundingClientRect().height
      : undefined,
  });

  useEffect(() => {
    virtualizer.measure();
  }, [wrapLines, virtualizer]);

  useImperativeHandle(
    ref,
    () => ({
      scrollToIndex: (index: number) => {
        virtualizer.scrollToIndex(index, { align: "center", behavior: "auto" });
      },
      captureScrollAnchor: () => {
        const el = parentRef.current;
        if (!el || isEmptyState) {
          return null;
        }
        const items = virtualizer.getVirtualItems();
        const first = items[0];
        if (!first) {
          return null;
        }
        const line = logsRef.current[first.index];
        if (line === undefined) {
          return null;
        }
        return {
          line,
          index: first.index,
          offsetTop: el.scrollTop,
        };
      },
      restoreScrollAnchor: (anchor, prependedCount, logsOrder) => {
        if (!anchor || prependedCount <= 0) {
          return;
        }
        const el = parentRef.current;
        if (logsOrder === "asc") {
          if (el && !wrapLines) {
            el.scrollTop =
              anchor.offsetTop + prependedCount * LOG_ROW_HEIGHT_PX;
          } else {
            virtualizer.scrollToIndex(anchor.index + prependedCount, {
              align: "start",
              behavior: "auto",
            });
          }
          return;
        }
        // desc: older rows land at the bottom; keep the current top offset.
        if (el) {
          el.scrollTop = anchor.offsetTop;
        }
        virtualizer.measure();
      },
    }),
    [virtualizer, isEmptyState, wrapLines]
  );

  const textTone = isDark ? "PodLogs-line--dark" : "PodLogs-line--light";
  const rowTone = isDark ? "PodLogs-row--dark" : "PodLogs-row--light";

  return (
    <Box
      ref={parentRef}
      data-testid="log-virtual-list"
      className={`PodLogs-virtual-list ${
        isDark ? "PodLogs-virtual-list--dark" : "PodLogs-virtual-list--light"
      }`}
    >
      {isEmptyState ? (
        <div className="PodLogs-empty" data-testid="log-empty-state">
          {logs[0]}
        </div>
      ) : (
        <Box
          className={
            wrapLines
              ? "PodLogs-virtual-inner PodLogs-virtual-inner--wrap"
              : "PodLogs-virtual-inner PodLogs-virtual-inner--scroll"
          }
          sx={{
            height: `${virtualizer.getTotalSize()}px`,
            position: "relative",
          }}
        >
          {virtualizer.getVirtualItems().map((virtualRow) => {
            const line = logs[virtualRow.index];
            const isActive = virtualRow.index === activeIndex;

            return (
              <Box
                key={`${podName}:${virtualRow.index}:${line}`}
                data-index={virtualRow.index}
                data-testid="log-virtual-row"
                data-active={isActive || undefined}
                ref={wrapLines ? virtualizer.measureElement : undefined}
                className={[
                  "PodLogs-row",
                  rowTone,
                  textTone,
                  wrapLines ? "PodLogs-row--wrap" : "PodLogs-row--nowrap",
                  isActive ? "PodLogs-row--active" : "",
                ]
                  .filter(Boolean)
                  .join(" ")}
                sx={{
                  position: "absolute",
                  top: 0,
                  left: 0,
                  transform: `translateY(${virtualRow.start}px)`,
                  height: wrapLines ? "auto" : `${LOG_ROW_HEIGHT_PX}px`,
                  lineHeight: wrapLines
                    ? undefined
                    : `${LOG_ROW_HEIGHT_PX}px`,
                }}
              >
                <Highlighter
                  searchWords={[search]}
                  autoEscape={true}
                  textToHighlight={line}
                  style={{
                    background: "transparent",
                    fontFamily: "inherit",
                    fontSize: "inherit",
                    color: "inherit",
                    fontWeight: "normal",
                  }}
                  highlightStyle={{
                    color: isActive || !isDark ? "#0f172a" : "#f8fafc",
                    backgroundColor: isActive
                      ? "#ffca28"
                      : isDark
                        ? "#665c1e"
                        : "#fff59d",
                    fontWeight: isActive ? "bold" : "normal",
                  }}
                />
              </Box>
            );
          })}
        </Box>
      )}
    </Box>
  );
});
