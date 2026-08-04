import { useEffect, useRef } from "react";
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
};

export function LogVirtualList({
  logs,
  search,
  wrapLines,
  colorMode,
  podName,
}: LogVirtualListProps) {
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

  const isLight = colorMode === "light";

  return (
    <Box
      ref={parentRef}
      data-testid="log-virtual-list"
      sx={{
        backgroundColor: isLight ? "whitesmoke" : "black",
        fontWeight: 600,
        borderRadius: "0.4rem",
        padding: "1rem 0rem",
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
          return (
            <Box
              key={`${virtualRow.index}-${podName}-logs`}
              data-index={virtualRow.index}
              data-testid="log-virtual-row"
              ref={wrapLines ? virtualizer.measureElement : undefined}
              component="span"
              sx={{
                position: "absolute",
                top: 0,
                left: 0,
                width: "100%",
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
                  background: isLight ? "#E6E6E6" : "#333333",
                  fontSize: "1.4rem",
                  textWrap: wrapLines ? "wrap" : "nowrap",
                  border: "1px solid #cacaca",
                }}
                highlightStyle={{
                  color: isLight ? "white" : "black",
                  backgroundColor: isLight ? "black" : "white",
                }}
              />
            </Box>
          );
        })}
      </Box>
    </Box>
  );
}
