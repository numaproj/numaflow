import { TextDecoder, TextEncoder } from "util";
import { act, renderHook, waitFor } from "@testing-library/react";
import {
  appendCapped,
  buildPodLogsUrl,
  mergeOlderLogs,
  usePodLogStream,
  UsePodLogStreamParams,
} from "./usePodLogStream";
import { LOADING_LOGS, MAX_LOGS, MAX_TOTAL_LOGS } from "./constants";

// jsdom lacks these; @stardazed/streams-polyfill's TextDecoderStream needs them.
Object.assign(global, { TextDecoder, TextEncoder });

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function abortError(): Error {
  return Object.assign(new Error("Aborted"), { name: "AbortError" });
}

type FetchHandle = {
  url: string;
  signal: AbortSignal;
  cancelSpy: jest.Mock;
  resolveFetch: () => void;
  pushText: (text: string) => void;
  close: () => void;
};

/**
 * Controllable fetch mock: body.pipeThrough(...).getReader() yields string
 * chunks so tests do not depend on TextDecoderStream in jsdom.
 */
function installFetchMock() {
  const handles: FetchHandle[] = [];
  const fetchMock = jest.fn(
    (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const signal = init?.signal as AbortSignal;
      const cancelSpy = jest.fn().mockResolvedValue(undefined);
      const pendingReads: Array<
        (result: ReadableStreamReadResult<string>) => void
      > = [];
      const queued: ReadableStreamReadResult<string>[] = [];

      const settleRead = (result: ReadableStreamReadResult<string>) => {
        const waiter = pendingReads.shift();
        if (waiter) {
          waiter(result);
          return;
        }
        queued.push(result);
      };

      const reader = {
        read: () => {
          if (queued.length > 0) {
            return Promise.resolve(queued.shift()!);
          }
          return new Promise<ReadableStreamReadResult<string>>((resolve) => {
            pendingReads.push(resolve);
          });
        },
        cancel: (reason?: unknown) => cancelSpy(reason),
      };

      const body = {
        pipeThrough: () => ({
          getReader: () => reader,
        }),
      };

      const d = deferred<Response>();
      let settled = false;

      const rejectOnce = () => {
        if (settled) {
          return;
        }
        settled = true;
        d.reject(abortError());
      };

      const handle: FetchHandle = {
        url,
        signal,
        cancelSpy,
        resolveFetch: () => {
          if (settled) {
            return;
          }
          if (signal?.aborted) {
            rejectOnce();
            return;
          }
          settled = true;
          d.resolve({
            ok: true,
            body,
          } as unknown as Response);
        },
        pushText: (text: string) => {
          settleRead({ done: false, value: text });
        },
        close: () => {
          settleRead({ done: true, value: undefined });
        },
      };

      signal?.addEventListener("abort", () => {
        rejectOnce();
        // Unblock any pending reader.read() calls.
        while (pendingReads.length > 0) {
          pendingReads.shift()!({ done: true, value: undefined });
        }
      });

      handles.push(handle);
      return d.promise;
    }
  );

  global.fetch = fetchMock as unknown as typeof fetch;
  return { handles, fetchMock };
}

const defaultParams: UsePodLogStreamParams = {
  namespaceId: "ns",
  podName: "pod-a",
  containerName: "numa",
  type: "monoVertex",
  host: "http://localhost",
  paused: false,
  enableTimestamp: false,
  levelFilter: "all",
  showPreviousLogs: false,
};

describe("buildPodLogsUrl", () => {
  it("builds the live follow URL", () => {
    expect(
      buildPodLogsUrl({
        host: "http://localhost",
        namespaceId: "ns",
        podName: "pod-a",
        containerName: "numa",
      })
    ).toBe(
      `http://localhost/api/v1/namespaces/ns/pods/pod-a/logs?container=numa&follow=true&tailLines=${MAX_LOGS}`
    );
  });

  it("appends previous=true when requested", () => {
    expect(
      buildPodLogsUrl({
        host: "",
        namespaceId: "ns",
        podName: "pod-a",
        containerName: "numa",
        previous: true,
        tailLines: 500,
      })
    ).toBe(
      "/api/v1/namespaces/ns/pods/pod-a/logs?container=numa&follow=true&tailLines=500&previous=true"
    );
  });

  it("supports follow=false history fetches", () => {
    expect(
      buildPodLogsUrl({
        host: "",
        namespaceId: "ns",
        podName: "pod-a",
        containerName: "numa",
        follow: false,
        tailLines: 1500,
      })
    ).toBe(
      "/api/v1/namespaces/ns/pods/pod-a/logs?container=numa&follow=false&tailLines=1500"
    );
  });
});

describe("appendCapped", () => {
  it("appends without trimming under the cap", () => {
    expect(appendCapped(["a"], ["b", "c"], 10)).toEqual(["a", "b", "c"]);
  });

  it("drops oldest lines when over the cap", () => {
    expect(appendCapped(["a", "b", "c"], ["d", "e"], 3)).toEqual([
      "c",
      "d",
      "e",
    ]);
  });
});

describe("mergeOlderLogs", () => {
  it("prepends only the net-older prefix before the overlap", () => {
    expect(
      mergeOlderLogs(["b", "c", "d"], ["a", "b", "c", "d"])
    ).toEqual({ logs: ["a", "b", "c", "d"], prependedCount: 1 });
  });

  it("strips the loading sentinel before merging", () => {
    expect(
      mergeOlderLogs([LOADING_LOGS, "b", "c"], ["a", "b", "c"])
    ).toEqual({ logs: ["a", "b", "c"], prependedCount: 1 });
  });

  it("returns zero prepended lines when the window did not grow", () => {
    expect(mergeOlderLogs(["a", "b"], ["a", "b"])).toEqual({
      logs: ["a", "b"],
      prependedCount: 0,
    });
  });

  it("returns zero prepended lines when there is no overlap", () => {
    expect(mergeOlderLogs(["x", "y"], ["a", "b"])).toEqual({
      logs: ["x", "y"],
      prependedCount: 0,
    });
  });
});

describe("usePodLogStream lifecycle", () => {
  let consoleErrorSpy: jest.SpyInstance;

  beforeEach(() => {
    consoleErrorSpy = jest.spyOn(console, "error").mockImplementation(() => {
      // silence expected abort noise
    });
  });

  afterEach(() => {
    consoleErrorSpy.mockRestore();
    jest.restoreAllMocks();
  });

  it("aborts the live fetch on unmount", async () => {
    const { handles } = installFetchMock();

    const { unmount } = renderHook(() => usePodLogStream(defaultParams));

    await waitFor(() => expect(handles.length).toBe(1));
    const live = handles[0];

    act(() => {
      unmount();
    });

    expect(live.signal.aborted).toBe(true);
  });

  it("aborts an in-flight live fetch when paused before the reader arrives", async () => {
    const { handles } = installFetchMock();

    const { rerender } = renderHook(
      (props: UsePodLogStreamParams) => usePodLogStream(props),
      { initialProps: defaultParams }
    );

    await waitFor(() => expect(handles.length).toBe(1));
    const live = handles[0];
    expect(live.signal.aborted).toBe(false);

    rerender({ ...defaultParams, paused: true });

    await waitFor(() => expect(live.signal.aborted).toBe(true));

    // Resolving after pause must not append real log lines.
    act(() => {
      live.resolveFetch();
    });

    // No second live fetch while paused.
    expect(handles.filter((h) => !h.url.includes("previous=true"))).toHaveLength(
      1
    );
  });

  it("aborts the first live fetch when podName changes mid-flight", async () => {
    const { handles } = installFetchMock();

    const { result, rerender } = renderHook(
      (props: UsePodLogStreamParams) => usePodLogStream(props),
      { initialProps: defaultParams }
    );

    await waitFor(() =>
      expect(handles.some((h) => h.url.includes("/pods/pod-a/"))).toBe(true)
    );
    const first = handles.find((h) => h.url.includes("/pods/pod-a/"))!;

    rerender({ ...defaultParams, podName: "pod-b" });

    await waitFor(() => expect(first.signal.aborted).toBe(true));
    await waitFor(() =>
      expect(
        handles.some(
          (h) => h.url.includes("/pods/pod-b/") && !h.signal.aborted
        )
      ).toBe(true)
    );

    const second = handles.find(
      (h) => h.url.includes("/pods/pod-b/") && !h.signal.aborted
    )!;

    await act(async () => {
      second.resolveFetch();
      await Promise.resolve();
      second.pushText("only-from-pod-b\n");
      second.close();
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(
        result.current.logs.some((line) => line.includes("only-from-pod-b"))
      ).toBe(true);
    });
    expect(
      result.current.logs.some((line) => line.includes("from-pod-a"))
    ).toBe(false);
  });

  it("aborts the previous-logs fetch when showPreviousLogs is toggled off", async () => {
    const { handles } = installFetchMock();

    const { rerender } = renderHook(
      (props: UsePodLogStreamParams) => usePodLogStream(props),
      {
        initialProps: {
          ...defaultParams,
          showPreviousLogs: true,
        },
      }
    );

    await waitFor(() => {
      expect(handles.some((h) => h.url.includes("previous=true"))).toBe(true);
    });

    const previous = handles.find((h) => h.url.includes("previous=true"));
    expect(previous).toBeDefined();
    expect(previous!.signal.aborted).toBe(false);

    rerender({
      ...defaultParams,
      showPreviousLogs: false,
    });

    await waitFor(() => expect(previous!.signal.aborted).toBe(true));
  });

  it("loads older lines with follow=false without aborting the live stream", async () => {
    const { handles } = installFetchMock();

    const { result } = renderHook(() => usePodLogStream(defaultParams));

    await waitFor(() => expect(handles.length).toBe(1));
    const live = handles[0];

    await act(async () => {
      live.resolveFetch();
      await Promise.resolve();
      live.pushText("line-b\nline-c\n");
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.logs).toEqual(
        expect.arrayContaining(["line-b", "line-c"])
      );
    });

    let loadPromise!: Promise<{ prependedCount: number }>;
    act(() => {
      loadPromise = result.current.loadOlderLogs(500);
    });

    await waitFor(() => expect(handles.length).toBe(2));
    const history = handles[1];
    expect(history.url).toContain("follow=false");
    expect(history.url).toContain(`tailLines=${MAX_LOGS + 500}`);
    expect(live.signal.aborted).toBe(false);

    await act(async () => {
      history.resolveFetch();
      await Promise.resolve();
      history.pushText("line-a\nline-b\nline-c\n");
      history.close();
      await loadPromise;
    });

    await waitFor(() => {
      expect(result.current.logs).toEqual(["line-a", "line-b", "line-c"]);
      expect(result.current.loadedCount).toBe(3);
      expect(result.current.remainingCapacity).toBe(
        MAX_TOTAL_LOGS - (MAX_LOGS + 500)
      );
    });
    expect(live.signal.aborted).toBe(false);
  });

  it("marks history exhausted when a larger window adds no older lines", async () => {
    const { handles } = installFetchMock();

    const { result } = renderHook(() => usePodLogStream(defaultParams));

    await waitFor(() => expect(handles.length).toBe(1));
    const live = handles[0];

    await act(async () => {
      live.resolveFetch();
      await Promise.resolve();
      live.pushText("line-a\nline-b\n");
      await Promise.resolve();
    });

    await waitFor(() => expect(result.current.loadedCount).toBe(2));

    let loadPromise!: Promise<{ prependedCount: number }>;
    act(() => {
      loadPromise = result.current.loadOlderLogs(500);
    });

    await waitFor(() => expect(handles.length).toBe(2));
    const history = handles[1];

    await act(async () => {
      history.resolveFetch();
      await Promise.resolve();
      history.pushText("line-a\nline-b\n");
      history.close();
      await loadPromise;
    });

    await waitFor(() => {
      expect(result.current.hasMoreOlder).toBe(false);
    });
    expect(result.current.logs).toEqual(
      expect.arrayContaining(["line-a", "line-b"])
    );
    expect(result.current.loadedCount).toBe(2);
  });
});
