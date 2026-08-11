import { createRef } from "react";
import { render, screen } from "@testing-library/react";
import { LogVirtualList, LogVirtualListHandle } from "./LogVirtualList";
import { NO_LOGS_MATCHING_SEARCH } from "./constants";

const VIEWPORT_HEIGHT = 160;

beforeAll(() => {
  Object.defineProperty(HTMLElement.prototype, "clientHeight", {
    configurable: true,
    get(this: HTMLElement) {
      if (this.getAttribute("data-testid") === "log-virtual-list") {
        return VIEWPORT_HEIGHT;
      }
      return 20;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", {
    configurable: true,
    get(this: HTMLElement) {
      if (this.getAttribute("data-testid") === "log-virtual-list") {
        return VIEWPORT_HEIGHT;
      }
      return 20;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "getBoundingClientRect", {
    configurable: true,
    value(this: HTMLElement) {
      const height =
        this.getAttribute("data-testid") === "log-virtual-list"
          ? VIEWPORT_HEIGHT
          : 20;
      return {
        x: 0,
        y: 0,
        width: 400,
        height,
        top: 0,
        left: 0,
        bottom: height,
        right: 400,
        toJSON: () => ({}),
      };
    },
  });
});

describe("LogVirtualList", () => {
  it("renders only a viewport subset of a long log list", () => {
    const logs = Array.from({ length: 100 }, (_, i) => `log-line-${i}`);

    render(
      <div style={{ height: VIEWPORT_HEIGHT }}>
        <LogVirtualList
          logs={logs}
          search=""
          wrapLines={false}
          colorMode="light"
          podName="pod-a"
          activeIndex={null}
        />
      </div>
    );

    expect(screen.getByTestId("log-virtual-list")).toBeInTheDocument();
    expect(screen.getByText("log-line-0")).toBeInTheDocument();

    const rows = screen.getAllByTestId("log-virtual-row");
    expect(rows.length).toBeGreaterThan(0);
    expect(rows.length).toBeLessThan(logs.length);
    expect(rows[0].className).toContain("PodLogs-row--light");
    expect(screen.queryByText("log-line-99")).not.toBeInTheDocument();
  });

  it("highlights search matches in visible rows", () => {
    render(
      <div style={{ height: VIEWPORT_HEIGHT }}>
        <LogVirtualList
          logs={["alpha foo", "beta bar"]}
          search="foo"
          wrapLines={false}
          colorMode="dark"
          podName="pod-a"
          activeIndex={null}
        />
      </div>
    );

    expect(screen.getByText("foo")).toBeInTheDocument();
  });

  it("marks the active match row and exposes scroll navigation", () => {
    const ref = createRef<LogVirtualListHandle>();

    render(
      <div style={{ height: VIEWPORT_HEIGHT }}>
        <LogVirtualList
          ref={ref}
          logs={["first match", "second match"]}
          search="match"
          wrapLines={false}
          colorMode="light"
          podName="pod-a"
          activeIndex={1}
        />
      </div>
    );

    const activeRow = screen.getAllByTestId("log-virtual-row")[1];
    expect(activeRow).toHaveAttribute("data-active", "true");
    expect(activeRow.className).toContain("PodLogs-row--active");
    expect(ref.current).not.toBeNull();
    expect(() => ref.current?.scrollToIndex(1)).not.toThrow();
  });

  it("renders the complete raw log line without field trimming", () => {
    const line = JSON.stringify({
      timestamp: "2026-08-05T05:54:14.095025Z",
      level: "INFO",
      message: "Processed message batch",
      pipeline: "simple-pipeline",
    });

    render(
      <div style={{ height: VIEWPORT_HEIGHT }}>
        <LogVirtualList
          logs={[line]}
          search=""
          wrapLines={false}
          colorMode="dark"
          podName="pod-a"
          activeIndex={null}
        />
      </div>
    );

    const row = screen.getByTestId("log-virtual-row");
    expect(row).toHaveTextContent(line);
    expect(row.className).toContain("PodLogs-row--nowrap");
    expect(row.className).toContain("PodLogs-row--dark");
    expect(row.querySelector(".PodLogs-col-msg--truncate")).toBeNull();
  });

  it("preserves full raw text when wrapping", () => {
    const line =
      '{"level":"INFO","message":"Processed messages per second","extra":"keep-me"}';

    render(
      <div style={{ height: VIEWPORT_HEIGHT }}>
        <LogVirtualList
          logs={[line]}
          search=""
          wrapLines={true}
          colorMode="dark"
          podName="pod-a"
          activeIndex={null}
        />
      </div>
    );

    const row = screen.getByTestId("log-virtual-row");
    expect(row).toHaveTextContent(line);
    expect(row.className).toContain("PodLogs-row--wrap");
  });

  it("shows centered empty state copy", () => {
    render(
      <div style={{ height: VIEWPORT_HEIGHT }}>
        <LogVirtualList
          logs={[NO_LOGS_MATCHING_SEARCH]}
          search="xyz"
          wrapLines={false}
          colorMode="dark"
          podName="pod-a"
          activeIndex={null}
        />
      </div>
    );

    expect(screen.getByTestId("log-empty-state")).toHaveTextContent(
      NO_LOGS_MATCHING_SEARCH
    );
    expect(screen.queryByTestId("log-virtual-row")).not.toBeInTheDocument();
  });
});
