import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { act } from "react-test-renderer";
import { TextEncoder, TextDecoder } from "util";
import { PodLogs } from "./index";
import {
  END_OF_RETAINED_LOGS,
  MAX_LOGS,
  NO_LOGS_MATCHING_SEARCH,
} from "./constants";

Object.assign(global, { TextDecoder, TextEncoder });

// jsdom reports 0 for layout; give the virtual log scroller a viewport.
beforeAll(() => {
  Object.defineProperty(HTMLElement.prototype, "clientHeight", {
    configurable: true,
    get(this: HTMLElement) {
      if (this.getAttribute("data-testid") === "log-virtual-list") {
        return 320;
      }
      return 20;
    },
  });
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", {
    configurable: true,
    get(this: HTMLElement) {
      if (this.getAttribute("data-testid") === "log-virtual-list") {
        return 320;
      }
      return 20;
    },
  });
});

describe("PodLogs", () => {
  let originFetch: any;
  beforeEach(() => {
    originFetch = (global as any).fetch;
  });
  afterEach(() => {
    (global as any).fetch = originFetch;
  });

  it("Load PodLogs screen", async () => {
    const mRes = {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(
            Buffer.from(
              `{"level":"info","ts":"2023-09-04T11:50:19.712416709Z","logger":"numaflow.Source-processor","caller":"publish/publisher.go:180","msg":"Skip publishing the new watermark because it's older than the current watermark","pipeline":"simple-pipeline","vertex":"in","entityID":"simple-pipeline-in-0","otStore":"default-simple-pipeline-in-cat_OT","hbStore":"default-simple-pipeline-in-cat_PROCESSORS","toVertexPartitionIdx":0,"entity":"simple-pipeline-in-0","head":1693828217394,"new":-1}`
            )
          );
          controller.enqueue(
            Buffer.from(
              `{"level":"error","ts":"2023-09-04T11:50:19.712416709Z","logger":"numaflow.Source-processor","caller":"publish/publisher.go:180","msg":"Skip publishing the new watermark because it's older than the current watermark","pipeline":"simple-pipeline","vertex":"in","entityID":"simple-pipeline-in-0","otStore":"default-simple-pipeline-in-cat_OT","hbStore":"default-simple-pipeline-in-cat_PROCESSORS","toVertexPartitionIdx":0,"entity":"simple-pipeline-in-0","head":1693828217394,"new":-1}`
            )
          );
          controller.enqueue(
            Buffer.from(
              `{"level":"warn","ts":"2023-09-04T11:50:19.712416709Z","logger":"numaflow.Source-processor","caller":"publish/publisher.go:180","msg":"Skip publishing the new watermark because it's older than the current watermark","pipeline":"simple-pipeline","vertex":"in","entityID":"simple-pipeline-in-0","otStore":"default-simple-pipeline-in-cat_OT","hbStore":"default-simple-pipeline-in-cat_PROCESSORS","toVertexPartitionIdx":0,"entity":"simple-pipeline-in-0","head":1693828217394,"new":-1}`
            )
          );
          controller.enqueue(
            Buffer.from(
              `{"level":"debug","ts":"2023-09-04T11:50:19.712416709Z","logger":"numaflow.Source-processor","caller":"publish/publisher.go:180","msg":"Skip publishing the new watermark because it's older than the current watermark","pipeline":"simple-pipeline","vertex":"in","entityID":"simple-pipeline-in-0","otStore":"default-simple-pipeline-in-cat_OT","hbStore":"default-simple-pipeline-in-cat_PROCESSORS","toVertexPartitionIdx":0,"entity":"simple-pipeline-in-0","head":1693828217394,"new":-1}`
            )
          );
          controller.close();
        },
      }),
      ok: true,
    };
    const mockedFetch = jest.fn().mockResolvedValue(mRes as any);
    (global as any).fetch = mockedFetch;
    await act(async () => {
      render(
        <PodLogs
          namespaceId={"numaflow-system"}
          containerName={"numa"}
          podName={"simple-mono-vertex-mv-31-abcde"}
        />
      );
    });

    expect(mockedFetch).toBeCalledTimes(1);
    expect(screen.getByText("Container Logs")).toBeInTheDocument();
    expect(screen.getByTestId("log-source-badge")).toHaveTextContent(
      "mv-31/numa"
    );
    expect(screen.getByTestId("log-source-badge")).toHaveAttribute(
      "title",
      "simple-mono-vertex-mv-31-abcde/numa"
    );
    expect(screen.getByTestId("log-loaded-count")).toBeInTheDocument();
    expect(screen.getByTestId("load-older-logs-button")).toHaveTextContent(
      "Load older 500"
    );
    expect(screen.queryByTestId("log-end-of-history")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Negate search")).toBeInTheDocument();
    expect(screen.getByTestId("wrap-lines-button")).toHaveClass(
      "PodLogs-icon-btn--active"
    );
    expect(screen.getByTestId("color-mode-button")).not.toHaveClass(
      "PodLogs-icon-btn--active"
    );
    const showTerminated = screen.getByLabelText("Show terminated");
    expect(showTerminated).not.toBeChecked();

    const searchInput = screen.getByPlaceholderText("Search logs");
    fireEvent.change(searchInput, { target: { value: "load" } });
    fireEvent.change(searchInput, { target: { value: "xyz" } });
    expect(screen.getByText(NO_LOGS_MATCHING_SEARCH)).toBeVisible();

    fireEvent.click(screen.getByTestId("negate-search"));
    expect(screen.getByTestId("clear-button")).toBeVisible();
    fireEvent.click(screen.getByTestId("clear-button"));

    expect(screen.getByTestId("pause-button")).toBeVisible();
    act(() => {
      fireEvent.click(screen.getByTestId("pause-button"));
      fireEvent.click(screen.getByTestId("pause-button"));
    });
    expect(screen.getByTestId("color-mode-button")).toBeVisible();
    fireEvent.click(screen.getByTestId("color-mode-button"));
    expect(screen.getByTestId("order-button")).toBeVisible();
    fireEvent.click(screen.getByTestId("order-button"));

    mockedFetch.mockResolvedValueOnce({
      body: new ReadableStream({
        start(controller) {
          controller.close();
        },
      }),
      ok: true,
    } as any);
    fireEvent.click(showTerminated);
    expect(showTerminated).toBeChecked();
  });

  it("Trigger PodLogs parsing error", async () => {
    const mRes = {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(Buffer.from("something"));
          controller.close();
        },
      }),
      ok: true,
    };
    const mockedFetch = jest.fn().mockResolvedValueOnce(mRes as any);
    (global as any).fetch = mockedFetch;
    await act(async () => {
      render(
        <PodLogs
          namespaceId={"numaflow-system"}
          containerName={"numa"}
          podName={"simple-pipeline-infer-0-xah5w"}
        />
      );
    });

    expect(mockedFetch).toBeCalledTimes(1);
  });

  it("navigates search matches with buttons and keyboard", async () => {
    const mRes = {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(
            Buffer.from("first match\nno result\nsecond match\n")
          );
          controller.close();
        },
      }),
      ok: true,
    };
    (global as any).fetch = jest.fn().mockResolvedValue(mRes as any);

    await act(async () => {
      render(
        <PodLogs
          namespaceId={"numaflow-system"}
          containerName={"numa"}
          podName={"simple-pipeline-infer-0-xah5w"}
        />
      );
    });

    const searchInput = screen.getByPlaceholderText("Search logs");
    fireEvent.change(searchInput, { target: { value: "match" } });

    expect(screen.getByTestId("search-match-count")).toHaveTextContent("1 / 2");
    fireEvent.click(screen.getByTestId("search-match-next"));
    expect(screen.getByTestId("search-match-count")).toHaveTextContent("2 / 2");
    expect(document.querySelector("[data-active='true']")).not.toBeNull();

    fireEvent.keyDown(searchInput, { key: "Enter" });
    expect(screen.getByTestId("search-match-count")).toHaveTextContent("1 / 2");
    fireEvent.keyDown(searchInput, { key: "Enter", shiftKey: true });
    expect(screen.getByTestId("search-match-count")).toHaveTextContent("2 / 2");

    fireEvent.click(screen.getByTestId("negate-search"));
    expect(screen.queryByTestId("search-match-count")).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId("clear-button"));
    expect(screen.queryByTestId("search-match-count")).not.toBeInTheDocument();
  });

  it("loads older logs from the header control and shows end of history", async () => {
    const liveBody = new ReadableStream({
      start(controller) {
        controller.enqueue(Buffer.from("line-b\nline-c\n"));
        // Keep the live stream open so a second history fetch can be asserted.
      },
    });
    const historyBody = new ReadableStream({
      start(controller) {
        controller.enqueue(Buffer.from("line-a\nline-b\nline-c\n"));
        controller.close();
      },
    });
    const emptyHistoryBody = new ReadableStream({
      start(controller) {
        controller.enqueue(Buffer.from("line-a\nline-b\nline-c\n"));
        controller.close();
      },
    });

    const mockedFetch = jest
      .fn()
      .mockResolvedValueOnce({ ok: true, body: liveBody } as any)
      .mockResolvedValueOnce({ ok: true, body: historyBody } as any)
      .mockResolvedValueOnce({ ok: true, body: emptyHistoryBody } as any);
    (global as any).fetch = mockedFetch;

    await act(async () => {
      render(
        <PodLogs
          namespaceId={"numaflow-system"}
          containerName={"numa"}
          podName={"simple-pipeline-infer-0-xah5w"}
        />
      );
    });

    await waitFor(() =>
      expect(screen.getByTestId("log-loaded-count")).toHaveTextContent(
        "2 loaded"
      )
    );

    fireEvent.click(screen.getByTestId("load-older-logs-menu-button"));
    expect(screen.getByTestId("load-older-batch-500")).toBeInTheDocument();
    expect(screen.getByTestId("load-older-batch-1000")).toBeInTheDocument();
    expect(screen.getByTestId("load-older-batch-2000")).toBeInTheDocument();
    expect(screen.getByTestId("load-older-batch-5000")).toBeInTheDocument();

    await act(async () => {
      fireEvent.click(screen.getByTestId("load-older-batch-500"));
    });

    await waitFor(() => {
      expect(mockedFetch).toHaveBeenCalledTimes(2);
      expect(String(mockedFetch.mock.calls[1][0])).toContain("follow=false");
      expect(String(mockedFetch.mock.calls[1][0])).toContain(
        `tailLines=${MAX_LOGS + 500}`
      );
      expect(screen.getByTestId("log-loaded-count")).toHaveTextContent(
        "3 loaded"
      );
    });

    await act(async () => {
      fireEvent.click(screen.getByTestId("load-older-logs-button"));
    });

    await waitFor(() => {
      expect(screen.getByTestId("log-end-of-history")).toHaveTextContent(
        END_OF_RETAINED_LOGS
      );
    });
  });
});
