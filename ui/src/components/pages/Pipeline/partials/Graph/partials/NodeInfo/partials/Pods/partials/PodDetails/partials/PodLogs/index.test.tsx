import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { act } from "react-test-renderer";
import { TextEncoder, TextDecoder } from "util";
import { PodLogs } from "./index";
import { NO_LOGS_MATCHING_SEARCH } from "./constants";

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
    expect(String(mockedFetch.mock.calls[0][0])).toContain("tailLines=1000");
    expect(screen.getByText("Container Logs")).toBeInTheDocument();
    expect(screen.getByTestId("log-source-badge")).toHaveTextContent(
      "mv-31/numa"
    );
    expect(screen.getByTestId("log-source-badge")).toHaveAttribute(
      "title",
      "simple-mono-vertex-mv-31-abcde/numa"
    );
    expect(screen.queryByTestId("log-loaded-count")).not.toBeInTheDocument();
    expect(screen.getByTestId("log-tail-size-button")).toHaveTextContent(
      "1,000 lines"
    );
    expect(screen.getByLabelText("Negate search")).toBeInTheDocument();
    expect(screen.getByTestId("wrap-lines-button")).toHaveClass(
      "PodLogs-icon-btn--active"
    );
    expect(screen.getByTestId("focus-logs-button")).toBeInTheDocument();
    expect(screen.getByTestId("color-mode-button")).not.toHaveClass(
      "PodLogs-icon-btn--active"
    );
    const previousLogsButton = screen.getByTestId("previous-logs");
    expect(previousLogsButton).not.toHaveClass("PodLogs-icon-btn--active");

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
    fireEvent.click(previousLogsButton);
    expect(previousLogsButton).toHaveClass("PodLogs-icon-btn--active");
    expect(screen.getByTestId("previous-container-banner")).toHaveTextContent(
      "Previous container"
    );
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

    // Enter must navigate matches even when focus is on the N-lines button,
    // instead of opening the tail-size dropdown.
    screen.getByTestId("log-tail-size-button").focus();
    fireEvent.keyDown(screen.getByTestId("log-tail-size-button"), {
      key: "Enter",
    });
    expect(screen.getByTestId("search-match-count")).toHaveTextContent("1 / 2");
    expect(screen.queryByTestId("log-tail-size-menu")).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId("negate-search"));
    expect(screen.queryByTestId("search-match-count")).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId("clear-button"));
    expect(screen.queryByTestId("search-match-count")).not.toBeInTheDocument();
  });

  it("auto-pauses and applies window size when selected while live", async () => {
    const firstBody = new ReadableStream({
      start(controller) {
        controller.enqueue(Buffer.from("line-a\nline-b\n"));
        // Keep open so pause/play and snapshot refetch can be asserted.
      },
    });
    const snapshotBody = new ReadableStream({
      start(controller) {
        controller.enqueue(Buffer.from("line-1\nline-2\nline-3\n"));
        controller.close();
      },
    });
    const liveResumeBody = new ReadableStream({
      start(controller) {
        controller.enqueue(Buffer.from("live-after-play\n"));
        // Keep open for follow=true.
      },
    });

    const mockedFetch = jest
      .fn()
      .mockResolvedValueOnce({ ok: true, body: firstBody } as any)
      .mockResolvedValueOnce({ ok: true, body: snapshotBody } as any)
      .mockResolvedValueOnce({ ok: true, body: liveResumeBody } as any);
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

    await waitFor(() => expect(screen.getByText("line-a")).toBeInTheDocument());
    expect(screen.queryByTestId("logs-paused-banner")).not.toBeInTheDocument();
    expect(screen.getByTestId("log-tail-size-button")).toHaveTextContent(
      "1,000 lines"
    );
    expect(screen.getByTestId("log-tail-size-button")).not.toBeDisabled();
    expect(screen.getByTestId("log-tail-size-menu-button")).not.toBeDisabled();

    fireEvent.click(screen.getByTestId("log-tail-size-menu-button"));
    expect(screen.getByTestId("log-tail-size-500")).toBeInTheDocument();
    expect(screen.getByTestId("log-tail-size-1000")).toBeInTheDocument();
    expect(screen.getByTestId("log-tail-size-2000")).toBeInTheDocument();
    expect(screen.getByTestId("log-tail-size-5000")).toBeInTheDocument();
    expect(screen.getByTestId("log-tail-size-10000")).toBeInTheDocument();

    await act(async () => {
      fireEvent.click(screen.getByTestId("log-tail-size-5000"));
    });

    await waitFor(() => {
      expect(mockedFetch).toHaveBeenCalledTimes(2);
      expect(String(mockedFetch.mock.calls[1][0])).toContain("tailLines=5000");
      expect(String(mockedFetch.mock.calls[1][0])).toContain("follow=false");
      expect(screen.getByTestId("log-tail-size-button")).toHaveTextContent(
        "5,000 lines"
      );
      expect(screen.getByTestId("logs-paused-banner")).toHaveTextContent(
        "Logs paused"
      );
      expect(screen.getByText("line-1")).toBeInTheDocument();
    });

    await act(async () => {
      fireEvent.click(screen.getByTestId("pause-button"));
    });

    await waitFor(() => {
      expect(mockedFetch).toHaveBeenCalledTimes(3);
      expect(String(mockedFetch.mock.calls[2][0])).toContain("tailLines=1000");
      expect(String(mockedFetch.mock.calls[2][0])).toContain("follow=true");
      expect(screen.getByTestId("log-tail-size-button")).not.toBeDisabled();
      expect(screen.getByTestId("log-tail-size-button")).toHaveTextContent(
        "1,000 lines"
      );
      expect(screen.queryByTestId("logs-paused-banner")).not.toBeInTheDocument();
    });

    await waitFor(() =>
      expect(screen.getByText("live-after-play")).toBeInTheDocument()
    );
  });

  it("resets the tail window to default when the container changes", async () => {
    const openBody = () =>
      new ReadableStream({
        start(controller) {
          controller.enqueue(Buffer.from("line-a\n"));
        },
      });
    const snapshotBody = new ReadableStream({
      start(controller) {
        controller.enqueue(Buffer.from("snap-1\nsnap-2\n"));
        controller.close();
      },
    });

    const mockedFetch = jest
      .fn()
      .mockResolvedValueOnce({ ok: true, body: openBody() } as any)
      .mockResolvedValueOnce({ ok: true, body: snapshotBody } as any)
      .mockResolvedValueOnce({ ok: true, body: openBody() } as any);
    (global as any).fetch = mockedFetch;

    let view: ReturnType<typeof render>;
    await act(async () => {
      view = render(
        <PodLogs
          namespaceId={"numaflow-system"}
          containerName={"numa"}
          podName={"simple-pipeline-infer-0-xah5w"}
        />
      );
    });

    await waitFor(() => expect(screen.getByText("line-a")).toBeInTheDocument());

    await act(async () => {
      fireEvent.click(screen.getByTestId("pause-button"));
    });
    expect(screen.getByTestId("logs-paused-banner")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("log-tail-size-menu-button"));
    await act(async () => {
      fireEvent.click(screen.getByTestId("log-tail-size-5000"));
    });

    await waitFor(() =>
      expect(screen.getByTestId("log-tail-size-button")).toHaveTextContent(
        "5,000 lines"
      )
    );

    await act(async () => {
      view.rerender(
        <PodLogs
          namespaceId={"numaflow-system"}
          containerName={"udf"}
          podName={"simple-pipeline-infer-0-xah5w"}
        />
      );
    });

    await waitFor(() => {
      expect(screen.getByTestId("log-tail-size-button")).toHaveTextContent(
        "1,000 lines"
      );
      expect(screen.getByTestId("log-tail-size-button")).not.toBeDisabled();
      expect(screen.queryByTestId("logs-paused-banner")).not.toBeInTheDocument();
      expect(String(mockedFetch.mock.calls.at(-1)[0])).toContain(
        "tailLines=1000"
      );
      expect(String(mockedFetch.mock.calls.at(-1)[0])).toContain(
        "container=udf"
      );
      expect(String(mockedFetch.mock.calls.at(-1)[0])).toContain("follow=true");
    });
  });

  it("applies N-lines to previous container logs without a live snapshot", async () => {
    const openBody = () =>
      new ReadableStream({
        start(controller) {
          controller.enqueue(Buffer.from("line-a\n"));
        },
      });
    const previousBody = () =>
      new ReadableStream({
        start(controller) {
          controller.enqueue(Buffer.from("prev-1\n"));
          controller.close();
        },
      });
    const previousLargerBody = () =>
      new ReadableStream({
        start(controller) {
          controller.enqueue(Buffer.from("prev-big-1\nprev-big-2\n"));
          controller.close();
        },
      });

    const liveResumeBody = () =>
      new ReadableStream({
        start(controller) {
          controller.enqueue(Buffer.from("live-again\n"));
        },
      });

    const mockedFetch = jest
      .fn()
      .mockResolvedValueOnce({ ok: true, body: openBody() } as any)
      .mockResolvedValueOnce({ ok: true, body: previousBody() } as any)
      .mockResolvedValueOnce({ ok: true, body: previousLargerBody() } as any)
      .mockResolvedValueOnce({ ok: true, body: liveResumeBody() } as any);
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

    await waitFor(() => expect(screen.getByText("line-a")).toBeInTheDocument());

    await act(async () => {
      fireEvent.click(screen.getByTestId("previous-logs"));
    });

    await waitFor(() => {
      expect(screen.getByTestId("previous-container-banner")).toHaveTextContent(
        "Previous container"
      );
      expect(screen.getByText("prev-1")).toBeInTheDocument();
    });

    expect(screen.getByTestId("log-tail-size-button")).not.toBeDisabled();
    expect(screen.getByTestId("log-tail-size-menu-button")).not.toBeDisabled();

    const fetchCountAfterPrevious = mockedFetch.mock.calls.length;
    expect(
      String(mockedFetch.mock.calls[fetchCountAfterPrevious - 1][0])
    ).toContain("previous=true");
    expect(
      String(mockedFetch.mock.calls[fetchCountAfterPrevious - 1][0])
    ).toContain("follow=false");

    fireEvent.click(screen.getByTestId("log-tail-size-menu-button"));
    await act(async () => {
      fireEvent.click(screen.getByTestId("log-tail-size-5000"));
    });

    await waitFor(() => {
      expect(mockedFetch.mock.calls.length).toBeGreaterThan(
        fetchCountAfterPrevious
      );
      const lastUrl = String(mockedFetch.mock.calls.at(-1)[0]);
      expect(lastUrl).toContain("previous=true");
      expect(lastUrl).toContain("tailLines=5000");
      expect(lastUrl).toContain("follow=false");
      expect(screen.getByTestId("log-tail-size-button")).toHaveTextContent(
        "5,000 lines"
      );
      expect(screen.getByText("prev-big-1")).toBeInTheDocument();
    });

    const liveOnlySnapshots = mockedFetch.mock.calls.filter((call) => {
      const url = String(call[0]);
      return url.includes("follow=false") && !url.includes("previous=true");
    });
    expect(liveOnlySnapshots).toHaveLength(0);

    await act(async () => {
      fireEvent.click(screen.getByTestId("previous-logs"));
    });

    await waitFor(() => {
      expect(
        screen.queryByTestId("previous-container-banner")
      ).not.toBeInTheDocument();
      expect(screen.getByTestId("log-tail-size-button")).toHaveTextContent(
        "1,000 lines"
      );
      const lastUrl = String(mockedFetch.mock.calls.at(-1)[0]);
      expect(lastUrl).toContain("tailLines=1000");
      expect(lastUrl).toContain("follow=true");
      expect(lastUrl).not.toContain("previous=true");
      expect(screen.getByText("live-again")).toBeInTheDocument();
    });
  });

  it("opens and closes the focus logs dialog without a second stream", async () => {
    const mRes = {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(
            Buffer.from(
              `{"level":"info","ts":"2023-09-04T11:50:19.712416709Z","logger":"numaflow.Source-processor","caller":"publish/publisher.go:180","msg":"focus-dialog-log-line","pipeline":"simple-pipeline","vertex":"in"}`
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
          podName={"simple-pipeline-in-0-abcde"}
        />
      );
    });

    expect(screen.getByTestId("focus-logs-button")).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.queryByTestId("logs-focus-placeholder")).not.toBeInTheDocument();

    await act(async () => {
      fireEvent.click(screen.getByTestId("focus-logs-button"));
    });

    const dialog = await screen.findByRole("dialog");
    expect(dialog).toBeInTheDocument();
    expect(screen.getByTestId("logs-focus-dialog")).toBeInTheDocument();
    expect(screen.getByTestId("logs-focus-placeholder")).toHaveTextContent(
      "Logs are open in the focused view"
    );
    expect(
      within(dialog).getByTestId("log-virtual-list")
    ).toBeInTheDocument();
    expect(within(dialog).getByTestId("focus-logs-button")).toHaveClass(
      "PodLogs-icon-btn--active"
    );
    // Still a single PodLogs instance / stream — no second fetch from opening focus.
    expect(mockedFetch).toBeCalledTimes(1);

    await act(async () => {
      fireEvent.click(within(dialog).getByTestId("focus-logs-button"));
    });

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    });
    expect(screen.queryByTestId("logs-focus-placeholder")).not.toBeInTheDocument();
    expect(screen.getByTestId("log-virtual-list")).toBeInTheDocument();
    expect(screen.getByTestId("focus-logs-button")).not.toHaveClass(
      "PodLogs-icon-btn--active"
    );
    expect(mockedFetch).toBeCalledTimes(1);
  });

  it("restores saved scroll offset when toggling Focus logs", async () => {
    const lines = Array.from({ length: 40 }, (_, i) =>
      JSON.stringify({
        level: "info",
        msg: `scroll-line-${i}`,
      })
    ).join("\n");
    const mRes = {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(Buffer.from(lines));
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
          podName={"simple-pipeline-in-0-abcde"}
        />
      );
    });

    await waitFor(() => {
      expect(screen.getByTestId("log-virtual-list")).toBeInTheDocument();
    });

    const listBefore = screen.getByTestId("log-virtual-list") as HTMLElement;
    Object.defineProperty(listBefore, "scrollTop", {
      configurable: true,
      writable: true,
      value: 220,
    });
    fireEvent.scroll(listBefore);

    await act(async () => {
      fireEvent.click(screen.getByTestId("focus-logs-button"));
    });

    const dialog = await screen.findByRole("dialog");
    const listInDialog = within(dialog).getByTestId(
      "log-virtual-list"
    ) as HTMLElement;

    await waitFor(() => {
      // Restored offset should be applied after remount into the dialog.
      expect(listInDialog.scrollTop).toBe(220);
    });

    await act(async () => {
      fireEvent.click(within(dialog).getByTestId("focus-logs-button"));
    });

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    });
    await waitFor(() => {
      expect(screen.getByTestId("log-virtual-list").scrollTop).toBe(220);
    });
  });

  it("shows the N-lines menu above the focused dialog and keeps focusControls focus-only", async () => {
    const mRes = {
      body: new ReadableStream({
        start(controller) {
          controller.enqueue(
            Buffer.from(
              `{"level":"info","ts":"2023-09-04T11:50:19.712416709Z","logger":"numaflow.Source-processor","caller":"publish/publisher.go:180","msg":"focus-menu-line","pipeline":"simple-pipeline","vertex":"in"}`
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
          podName={"simple-pipeline-in-0-abcde"}
          focusControls={
            <div data-testid="logs-focus-controls-probe">Focus controls</div>
          }
        />
      );
    });

    expect(screen.queryByTestId("logs-focus-context")).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("logs-focus-controls-probe")
    ).not.toBeInTheDocument();

    await act(async () => {
      fireEvent.click(screen.getByTestId("focus-logs-button"));
    });

    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByTestId("logs-focus-context")).toBeInTheDocument();
    expect(
      within(dialog).getByTestId("logs-focus-controls-probe")
    ).toBeInTheDocument();

    await act(async () => {
      fireEvent.click(within(dialog).getByTestId("log-tail-size-menu-button"));
    });

    expect(await screen.findByTestId("log-tail-size-menu")).toBeVisible();
    expect(screen.getByTestId("log-tail-size-5000")).toBeInTheDocument();
    // Level Select menu uses the same elevated z-index as the N-lines menu
    // so options are not hidden under the Focus dialog.
    expect(within(dialog).getByTestId("log-level-select")).toBeInTheDocument();

    await act(async () => {
      fireEvent.click(within(dialog).getByTestId("focus-logs-button"));
    });

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    });
    expect(screen.queryByTestId("logs-focus-context")).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("logs-focus-controls-probe")
    ).not.toBeInTheDocument();
  });
});
