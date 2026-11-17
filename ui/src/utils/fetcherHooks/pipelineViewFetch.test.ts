import { findBufferEdge, usePipelineViewFetch } from "./pipelineViewFetch";
import { renderHook, waitFor } from "@testing-library/react";
import { act } from "react-test-renderer";
import "@testing-library/jest-dom";

jest.mock("../fetchWrappers/fetch");

describe("Custom Pipeline hook", () => {
  let originFetch: any;
  beforeEach(() => {
    originFetch = (global as any).fetch;
  });
  afterEach(() => {
    (global as any).fetch = originFetch;
  });
  it("should pass", async () => {
    const mRes1 = {
      json: jest.fn().mockResolvedValueOnce({
        metadata: {
          name: "simple-pipeline",
          namespace: "default",
        },
        spec: {
          vertices: [
            {
              name: "in",
              source: {
                generator: {
                  rpu: 5,
                  duration: "1s",
                  msgSize: 8,
                },
              },
              scale: {},
            },
            {
              name: "cat",
              udf: {
                container: null,
                builtin: {
                  name: "cat",
                },
                groupBy: null,
              },
              scale: {},
            },
            {
              name: "out",
              sink: {
                log: {},
              },
              scale: {},
            },
          ],
          edges: [
            {
              from: "in",
              to: "cat",
              conditions: null,
            },
            {
              from: "cat",
              to: "out",
              conditions: null,
            },
          ],
          watermark: {
            maxDelay: "0s",
          },
        },
      }),
      ok: true,
    };
    const mRes2 = {
      json: jest.fn().mockResolvedValueOnce({
        data: [
          {
            pipeline: "simple-pipeline",
            bufferName: "default-simple-pipeline-in-cat-0",
            pendingCount: 0,
            ackPendingCount: 5,
            totalMessages: 5,
            bufferLength: 30000,
            bufferUsageLimit: 0.8,
            bufferUsage: 0.00016666666666666666,
            isFull: false,
          },
          {
            pipeline: "simple-pipeline",
            bufferName: "default-simple-pipeline-cat-out-0",
            pendingCount: 0,
            ackPendingCount: 0,
            totalMessages: 0,
            bufferLength: 30000,
            bufferUsageLimit: 0.8,
            bufferUsage: 0,
            isFull: false,
          },
        ],
      }),
      ok: true,
    };
    const mRes3 = {
      json: jest
        .fn()
        .mockResolvedValueOnce([{ name: "simple-pipeline-in-0-dlp51" }]),
      ok: true,
    };
    const mRes4 = {
      json: jest
        .fn()
        .mockResolvedValueOnce([{ name: "simple-pipeline-cat-0-zaiiz" }]),
      ok: true,
    };
    const mRes5 = {
      json: jest
        .fn()
        .mockResolvedValueOnce([{ name: "simple-pipeline-out-0-z41ep" }]),
      ok: true,
    };
    const mRes6 = {
      json: jest.fn().mockResolvedValueOnce([
        {
          pipeline: "simple-pipeline",
          vertex: "in",
          processingRates: {
            "15m": 5,
            "1m": 5,
            "5m": 5,
            default: 4.973684210526316,
          },
        },
      ]),
      ok: true,
    };
    const mRes7 = {
      json: jest.fn().mockResolvedValueOnce([
        {
          pipeline: "simple-pipeline",
          vertex: "cat",
          processingRates: {
            "15m": 5.007246376811594,
            "1m": 5,
            "5m": 5.029032258064516,
            default: 5.026315789473684,
          },
          pendings: {
            "15m": 0,
            "1m": 0,
            "5m": 0,
            default: 0,
          },
        },
      ]),
      ok: true,
    };
    const mRes8 = {
      json: jest.fn().mockResolvedValueOnce([
        {
          pipeline: "simple-pipeline",
          vertex: "out",
          processingRates: {
            "15m": 5.0058823529411764,
            "1m": 5.042857142857143,
            "5m": 4.993548387096774,
            default: 4.989473684210527,
          },
          pendings: {
            "15m": 0,
            "1m": 0,
            "5m": 0,
            default: 0,
          },
        },
      ]),
      ok: true,
    };
    const mRes9 = {
      json: jest.fn().mockResolvedValueOnce([
        {
          pipeline: "simple-pipeline",
          edge: "in-cat",
          watermarks: [1686318878094],
          isWatermarkEnabled: true,
        },
        {
          pipeline: "simple-pipeline",
          edge: "cat-out",
          watermarks: [1686318877093],
          isWatermarkEnabled: true,
        },
      ]),
      ok: true,
    };
    const mockedFetch = jest
      .fn()
      .mockResolvedValueOnce(mRes1 as any)
      .mockResolvedValueOnce(mRes2 as any)
      .mockResolvedValueOnce(mRes3 as any)
      .mockResolvedValueOnce(mRes4 as any)
      .mockResolvedValueOnce(mRes5 as any)
      .mockResolvedValueOnce(mRes6 as any)
      .mockResolvedValueOnce(mRes7 as any)
      .mockResolvedValueOnce(mRes8 as any)
      .mockResolvedValueOnce(mRes9 as any);
    (global as any).fetch = mockedFetch;
    await act(async () => {
      const { result } = renderHook(() =>
        usePipelineViewFetch("default", "simple-pipeline", () => {
          return;
        })
      );
    });
    expect(mockedFetch).toBeCalledTimes(4);
    expect(mRes1.json).toBeCalledTimes(1);
    expect(mRes2.json).toBeCalledTimes(1);
  });
  it("should fail", async () => {
    const mRes = {
      json: jest.fn().mockResolvedValueOnce({ dummy: "response" }),
      ok: false,
    };
    const mockedFetch = jest.fn().mockResolvedValueOnce(mRes as any);
    (global as any).fetch = mockedFetch;
    await act(async () => {
      const { result } = renderHook(() =>
        usePipelineViewFetch("default", "simple-pipeline", () => {
          return;
        })
      );
    });
    expect(mockedFetch).toBeCalledTimes(6);
  });

  it("maps edge-owned buffers to their edge and to the vertex reading them", async () => {
    const okJson = (payload: any) => ({
      ok: true,
      json: jest.fn().mockResolvedValue(payload),
    });
    const pipelinePayload = {
      data: {
        pipeline: {
          metadata: {
            name: "fan-in-pipeline",
            namespace: "default",
          },
          spec: {
            vertices: [
              { name: "in-a", source: { generator: {} }, scale: {} },
              { name: "in-b", source: { generator: {} }, scale: {} },
              { name: "cat", udf: { builtin: { name: "cat" } }, scale: {} },
            ],
            edges: [
              { from: "in-a", to: "cat", conditions: null },
              { from: "in-b", to: "cat", conditions: null },
            ],
            watermark: {},
          },
        },
      },
    };
    const buffer = (
      bufferName: string,
      pendingCount: number,
      ackPendingCount: number,
      isFull = false
    ) => ({
      pipeline: "fan-in-pipeline",
      bufferName,
      pendingCount,
      ackPendingCount,
      totalMessages: pendingCount + ackPendingCount,
      bufferLength: 30000,
      bufferUsageLimit: 0.8,
      bufferUsage: 0.5,
      isFull,
    });
    const mockedFetch = jest.fn((url: string) => {
      if (url.includes("/isbs")) {
        return Promise.resolve(
          okJson({
            data: [
              buffer("default-fan-in-pipeline-in-a-cat-0", 7, 2),
              buffer("default-fan-in-pipeline-in-a-cat-1", 1, 0, true),
              buffer("default-fan-in-pipeline-in-b-cat-0", 3, 1),
              buffer("default-fan-in-pipeline-in-b-cat-1", 0, 0),
            ],
          }) as any
        );
      }
      if (url.includes("/watermarks")) {
        return Promise.resolve(
          okJson({
            data: [
              {
                pipeline: "fan-in-pipeline",
                edge: "in-a-cat",
                watermarks: [1686318878094],
                isWatermarkEnabled: true,
              },
              {
                pipeline: "fan-in-pipeline",
                edge: "in-b-cat",
                watermarks: [1686318878094],
                isWatermarkEnabled: true,
              },
            ],
          }) as any
        );
      }
      if (url.includes("/vertices/metrics")) {
        return Promise.resolve(okJson({ data: {} }) as any);
      }
      if (url.includes("/pods")) {
        return Promise.resolve(okJson({ data: [] }) as any);
      }
      return Promise.resolve(okJson(pipelinePayload) as any);
    });
    (global as any).fetch = mockedFetch;

    const { result } = renderHook(() =>
      usePipelineViewFetch("default", "fan-in-pipeline", jest.fn())
    );

    await waitFor(() => {
      expect(result.current.edges).toHaveLength(2);
      expect(result.current.edges[0]?.data?.pendingLabel).toBeDefined();
    });

    // Each edge owns its buffer, so the hover counts are per edge, summed over partitions.
    expect(
      result.current.edges.map((edge) => ({
        id: edge.id,
        pendingLabel: edge.data?.pendingLabel,
        ackPendingLabel: edge.data?.ackPendingLabel,
        isFull: edge.data?.isFull,
      }))
    ).toEqual([
      { id: "in-a-cat", pendingLabel: 8, ackPendingLabel: 2, isFull: true },
      { id: "in-b-cat", pendingLabel: 3, ackPendingLabel: 1, isFull: false },
    ]);

    // A vertex lists the buffers it reads from: every partition of every incoming edge.
    const bufferNames = (vertex: string) => {
      const node = result.current.vertices.find((n) => n.id === vertex);
      const buffers = (node?.data as any)?.buffers;
      return buffers?.map((b: any) => b.bufferName) ?? null;
    };
    expect(bufferNames("cat")).toEqual([
      "default-fan-in-pipeline-in-a-cat-0",
      "default-fan-in-pipeline-in-a-cat-1",
      "default-fan-in-pipeline-in-b-cat-0",
      "default-fan-in-pipeline-in-b-cat-1",
    ]);
    expect(bufferNames("in-a")).toBeNull();
    expect(bufferNames("in-b")).toBeNull();
  });

  it("refreshes data when refresh function is called", async () => {
    const mockedFetch = jest.fn().mockResolvedValueOnce({
      ok: true,
      json: jest.fn().mockResolvedValueOnce({
        data: { pipeline: { id: "1", name: "test-pipeline" } },
      }),
    });

    (global as any).fetch = mockedFetch;

    const { result } = renderHook(() =>
      usePipelineViewFetch("test-namespace", "test-pipeline", jest.fn())
    );

    await act(async () => {
      result.current.refresh();
    });

    expect(mockedFetch).toHaveBeenCalledTimes(6); // fetch is called once on mount and once on refresh
  });

  it("returns loading state correctly", async () => {
    const mockedFetch = jest.fn().mockResolvedValueOnce({
      ok: true,
      json: jest.fn().mockResolvedValueOnce({
        data: { pipeline: { id: "1", name: "test-pipeline" } },
      }),
    });

    (global as any).fetch = mockedFetch;

    const { result } = renderHook(() =>
      usePipelineViewFetch("test-namespace", "test-pipeline", jest.fn())
    );

    await waitFor(() => {
      expect(result.current.loading).toBe(true); // loading is true initially
    });
  });

  it("returns error when fetch fails", async () => {
    const mockedFetch = jest.fn().mockRejectedValue(new Error("fetch error"));

    (global as any).fetch = mockedFetch;

    const addError = jest.fn();
    const { result } = renderHook(() =>
      usePipelineViewFetch("test-namespace", "test-pipeline", addError)
    );

    await waitFor(() => {
      expect(result.current.pipelineErr).toEqual("fetch error");
    });
  });
});

describe("findBufferEdge", () => {
  const nsPl = "default-my-pl-";

  it("matches a buffer to its edge when vertex names contain hyphens", () => {
    const edges = [
      { from: "in-one", to: "cat" },
      { from: "in-two", to: "cat" },
      { from: "cat", to: "out-sink" },
    ];
    expect(findBufferEdge("default-my-pl-in-one-cat-0", nsPl, edges)).toBe(
      edges[0]
    );
    expect(findBufferEdge("default-my-pl-in-two-cat-12", nsPl, edges)).toBe(
      edges[1]
    );
    expect(findBufferEdge("default-my-pl-cat-out-sink-1", nsPl, edges)).toBe(
      edges[2]
    );
  });

  it("does not match an edge whose name is only a prefix of the buffer's edge", () => {
    const edges = [
      { from: "a", to: "b" },
      { from: "a", to: "b-c" },
    ];
    // "a" -> "b" leaves "c-0" after its prefix, which is not a partition index.
    expect(findBufferEdge("default-my-pl-a-b-c-0", nsPl, edges)).toBe(edges[1]);
    expect(findBufferEdge("default-my-pl-a-b-0", nsPl, edges)).toBe(edges[0]);
  });

  it("returns undefined for buffers that belong to no edge", () => {
    const edges = [{ from: "in", to: "cat" }];
    expect(findBufferEdge("default-my-pl-cat-0", nsPl, edges)).toBeUndefined();
    expect(
      findBufferEdge("default-other-pl-in-cat-0", nsPl, edges)
    ).toBeUndefined();
    expect(findBufferEdge("default-my-pl-in-cat-", nsPl, edges)).toBeUndefined();
    expect(findBufferEdge(undefined, nsPl, edges)).toBeUndefined();
    expect(findBufferEdge("default-my-pl-in-cat-0", nsPl, undefined)).toBeUndefined();
  });
});
