import {
  forwardRef,
  useEffect,
  useImperativeHandle,
  useRef,
} from "react";
import Box from "@mui/material/Box";
import Highlighter from "react-highlight-words";
import { useVirtualizer } from "@tanstack/react-virtual";
import { LOG_ROW_HEIGHT_PX } from "./constants";

export type LogVirtualListProps = {
  logs: string[];
  search: string;
  wrapLines: boolean;
  colorMode: string;
  podName: string;
  activeIndex: number | null;
};

export type LogVirtualListHandle = {
  scrollToIndex: (index: number) => void;
};

export const LogVirtualList = forwardRef<
  LogVirtualListHandle,
  LogVirtualListProps
>(function LogVirtualList(
  { logs, search, wrapLines, colorMode, podName, activeIndex },
  ref
) {
  const parentRef = useRef<HTMLDivElement | null>(null);

  const virtualizer = useVirtualizer({
    count: logs.length,
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
    }),
    [virtualizer]
  );

  const isLight = colorMode === "light";

  return (
    <Box
      ref={parentRef}
      data-testid="log-virtual-list"
      sx={{
        backgroundColor: isLight ? "grey.50" : "#121212",
        border: "1px solid",
        borderColor: isLight ? "divider" : "grey.800",
        borderRadius: 1,
        padding: "0.8rem",
        height: "calc(100% - 6rem)",
        overflow: "auto",
      }}
    >
      <Box
        sx={{
          height: `${virtualizer.getTotalSize()}px`,
          width: "100%",
          position: "relative",
        }}
      >
        {virtualizer.getVirtualItems().map((virtualRow) => {
          const line = logs[virtualRow.index];
          const isActive = virtualRow.index === activeIndex;
          return (
            <Box
              key={`${virtualRow.index}-${podName}-logs`}
              data-index={virtualRow.index}
              data-testid="log-virtual-row"
              data-active={isActive || undefined}
              ref={wrapLines ? virtualizer.measureElement : undefined}
              component="span"
              sx={{
                position: "absolute",
                top: 0,
                left: 0,
                width: "100%",
                backgroundColor:
                  isActive
                    ? isLight
                      ? "#fff8e1"
                      : "#453b16"
                    : virtualRow.index % 2
                      ? isLight
                        ? "#f7f7f7"
                        : "#191919"
                      : "transparent",
                outline: isActive ? "2px solid #ffb300" : undefined,
                outlineOffset: isActive ? "-2px" : undefined,
                zIndex: isActive ? 1 : undefined,
                transform: `translateY(${virtualRow.start}px)`,
                whiteSpace: wrapLines ? "normal" : "nowrap",
                height: wrapLines ? "auto" : `${LOG_ROW_HEIGHT_PX}px`,
                lineHeight: `${LOG_ROW_HEIGHT_PX}px`,
              }}
            >
              <Highlighter
                searchWords={[search]}
                autoEscape={true}
                textToHighlight={line}
                style={{
                  color: isLight ? "black" : "white",
                  fontFamily: "Consolas,Liberation Mono,Courier,monospace",
                  fontWeight: "normal",
                  background: "transparent",
                  fontSize: "1.4rem",
                  textWrap: wrapLines ? "wrap" : "nowrap",
                  display: "block",
                  padding: "0 0.5rem",
                }}
                highlightStyle={{
                  color: isActive || isLight ? "black" : "white",
                  backgroundColor: isActive
                    ? "#ffca28"
                    : isLight
                      ? "#fff59d"
                      : "#665c1e",
                  fontWeight: isActive ? "bold" : "normal",
                }}
              />
            </Box>
          );
        })}
      </Box>
    </Box>
  );
});
