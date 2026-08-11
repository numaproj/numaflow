import { TextDecoder, TextEncoder } from "util";
import { act, renderHook, waitFor } from "@testing-library/react";
import {
  appendCapped,
  buildPodLogsUrl,
  usePodLogStream,
  UsePodLogStreamParams,
} from "./usePodLogStream";
import { MAX_LOGS } from "./constants";

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
  tailLines: MAX_LOGS,
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

  it("refetches with an absolute larger tailLines window", async () => {
    const { handles } = installFetchMock();

    const { result, rerender } = renderHook(
      (props: UsePodLogStreamParams) => usePodLogStream(props),
      { initialProps: defaultParams }
    );

    await waitFor(() => expect(handles.length).toBe(1));
    expect(handles[0].url).toContain(`tailLines=${MAX_LOGS}`);

    await act(async () => {
      handles[0].resolveFetch();
      await Promise.resolve();
      handles[0].pushText("old-a\nold-b\n");
      await Promise.resolve();
    });

    await waitFor(() => expect(result.current.loadedCount).toBe(2));

    rerender({ ...defaultParams, tailLines: 5000 });

    await waitFor(() => expect(handles.length).toBe(2));
    expect(handles[0].signal.aborted).toBe(true);
    expect(handles[1].url).toContain("tailLines=5000");
    expect(handles[1].url).toContain("follow=true");

    await act(async () => {
      handles[1].resolveFetch();
      await Promise.resolve();
      handles[1].pushText("line-1\nline-2\nline-3\n");
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.logs).toEqual(["line-1", "line-2", "line-3"]);
      expect(result.current.loadedCount).toBe(3);
    });
  });

  it("refetches with an absolute smaller tailLines window", async () => {
    const { handles } = installFetchMock();

    const { rerender } = renderHook(
      (props: UsePodLogStreamParams) => usePodLogStream(props),
      { initialProps: { ...defaultParams, tailLines: 5000 } }
    );

    await waitFor(() => expect(handles.length).toBe(1));
    expect(handles[0].url).toContain("tailLines=5000");

    rerender({ ...defaultParams, tailLines: 1000 });

    await waitFor(() => expect(handles.length).toBe(2));
    expect(handles[0].signal.aborted).toBe(true);
    expect(handles[1].url).toContain("tailLines=1000");
  });

  it("does not fetch a snapshot when pausing alone", async () => {
    const { handles } = installFetchMock();

    const { result, rerender } = renderHook(
      (props: UsePodLogStreamParams) => usePodLogStream(props),
      { initialProps: defaultParams }
    );

    await waitFor(() => expect(handles.length).toBe(1));

    await act(async () => {
      handles[0].resolveFetch();
      await Promise.resolve();
      handles[0].pushText("frozen-a\nfrozen-b\n");
      await Promise.resolve();
    });

    await waitFor(() => expect(result.current.loadedCount).toBe(2));

    rerender({ ...defaultParams, paused: true });

    await waitFor(() => expect(handles[0].signal.aborted).toBe(true));

    expect(handles.filter((h) => h.url.includes("follow=false"))).toHaveLength(
      0
    );
    expect(result.current.logs).toEqual(["frozen-a", "frozen-b"]);
  });

  it("fetches a snapshot when pausing with a new tailLines in one update", async () => {
    const { handles } = installFetchMock();

    const { result, rerender } = renderHook(
      (props: UsePodLogStreamParams) => usePodLogStream(props),
      { initialProps: defaultParams }
    );

    await waitFor(() => expect(handles.length).toBe(1));

    await act(async () => {
      handles[0].resolveFetch();
      await Promise.resolve();
      handles[0].pushText("old-a\nold-b\n");
      await Promise.resolve();
    });

    await waitFor(() => expect(result.current.loadedCount).toBe(2));

    // Mirrors UI auto-pause: set paused + new window size together.
    rerender({ ...defaultParams, paused: true, tailLines: 5000 });

    await waitFor(() =>
      expect(handles.some((h) => h.url.includes("follow=false"))).toBe(true)
    );

    const snapshot = handles.find((h) => h.url.includes("follow=false"))!;
    expect(snapshot.url).toContain("tailLines=5000");

    await act(async () => {
      snapshot.resolveFetch();
      await Promise.resolve();
      snapshot.pushText("snap-1\nsnap-2\nsnap-3\n");
      snapshot.close();
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.logs).toEqual(["snap-1", "snap-2", "snap-3"]);
      expect(result.current.loadedCount).toBe(3);
    });
  });

  it("fetches follow=false when tailLines changes while paused", async () => {
    const { handles } = installFetchMock();

    const { result, rerender } = renderHook(
      (props: UsePodLogStreamParams) => usePodLogStream(props),
      { initialProps: defaultParams }
    );

    await waitFor(() => expect(handles.length).toBe(1));

    await act(async () => {
      handles[0].resolveFetch();
      await Promise.resolve();
      handles[0].pushText("old-a\nold-b\n");
      await Promise.resolve();
    });

    await waitFor(() => expect(result.current.loadedCount).toBe(2));

    rerender({ ...defaultParams, paused: true });
    await waitFor(() => expect(handles[0].signal.aborted).toBe(true));

    rerender({ ...defaultParams, paused: true, tailLines: 5000 });

    await waitFor(() =>
      expect(handles.some((h) => h.url.includes("follow=false"))).toBe(true)
    );

    const snapshot = handles.find((h) => h.url.includes("follow=false"))!;
    expect(snapshot.url).toContain("tailLines=5000");

    await act(async () => {
      snapshot.resolveFetch();
      await Promise.resolve();
      snapshot.pushText("snap-1\nsnap-2\nsnap-3\n");
      snapshot.close();
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.logs).toEqual(["snap-1", "snap-2", "snap-3"]);
      expect(result.current.loadedCount).toBe(3);
    });
  });

  it("aborts an in-flight paused snapshot when unpausing and starts live follow", async () => {
    const { handles } = installFetchMock();

    const { rerender } = renderHook(
      (props: UsePodLogStreamParams) => usePodLogStream(props),
      { initialProps: defaultParams }
    );

    await waitFor(() => expect(handles.length).toBe(1));
    await act(async () => {
      handles[0].resolveFetch();
      await Promise.resolve();
      handles[0].pushText("live-1\n");
      await Promise.resolve();
    });

    rerender({ ...defaultParams, paused: true });
    await waitFor(() => expect(handles[0].signal.aborted).toBe(true));

    rerender({ ...defaultParams, paused: true, tailLines: 2000 });

    await waitFor(() =>
      expect(handles.some((h) => h.url.includes("follow=false"))).toBe(true)
    );
    const snapshot = handles.find((h) => h.url.includes("follow=false"))!;

    rerender({ ...defaultParams, paused: false, tailLines: 2000 });

    await waitFor(() => expect(snapshot.signal.aborted).toBe(true));
    await waitFor(() => {
      const liveAfterPlay = handles.filter(
        (h) => h.url.includes("follow=true") && h.url.includes("tailLines=2000")
      );
      expect(liveAfterPlay.length).toBeGreaterThan(0);
      expect(liveAfterPlay[liveAfterPlay.length - 1].signal.aborted).toBe(
        false
      );
    });
  });

  it("clears loading when a paused snapshot closes with no lines", async () => {
    const { handles } = installFetchMock();

    const { result, rerender } = renderHook(
      (props: UsePodLogStreamParams) => usePodLogStream(props),
      { initialProps: defaultParams }
    );

    await waitFor(() => expect(handles.length).toBe(1));

    await act(async () => {
      handles[0].resolveFetch();
      await Promise.resolve();
      handles[0].pushText("old-a\n");
      await Promise.resolve();
    });

    await waitFor(() => expect(result.current.loadedCount).toBe(1));

    rerender({ ...defaultParams, paused: true });
    await waitFor(() => expect(handles[0].signal.aborted).toBe(true));

    rerender({ ...defaultParams, paused: true, tailLines: 5000 });

    await waitFor(() =>
      expect(handles.some((h) => h.url.includes("follow=false"))).toBe(true)
    );

    const snapshot = handles.find((h) => h.url.includes("follow=false"))!;

    await act(async () => {
      snapshot.resolveFetch();
      await Promise.resolve();
      snapshot.close();
      await Promise.resolve();
    });

    await waitFor(() => {
      expect(result.current.logs).toEqual([]);
      expect(result.current.loadedCount).toBe(0);
    });
  });

  it("does not fetch a live snapshot when tailLines changes while showing previous logs", async () => {
    const { handles } = installFetchMock();

    const { rerender } = renderHook(
      (props: UsePodLogStreamParams) => usePodLogStream(props),
      { initialProps: defaultParams }
    );

    await waitFor(() => expect(handles.length).toBe(1));

    await act(async () => {
      handles[0].resolveFetch();
      await Promise.resolve();
      handles[0].pushText("live-1\n");
      await Promise.resolve();
    });

    rerender({ ...defaultParams, paused: true });
    await waitFor(() => expect(handles[0].signal.aborted).toBe(true));

    const fetchCountAfterPause = handles.length;

    rerender({
      ...defaultParams,
      paused: true,
      showPreviousLogs: true,
      tailLines: 5000,
    });

    await waitFor(() =>
      expect(
        handles.some(
          (h) => h.url.includes("previous=true") && h.url.includes("tailLines=5000")
        )
      ).toBe(true)
    );

    expect(
      handles
        .slice(fetchCountAfterPause)
        .filter((h) => h.url.includes("follow=false") && !h.url.includes("previous=true"))
    ).toHaveLength(0);
  });
});
