import { usePipelineViewFetch } from "./pipelineViewFetch";
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
            bufferName: "default-simple-pipeline-cat-0",
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
            bufferName: "default-simple-pipeline-out-0",
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
