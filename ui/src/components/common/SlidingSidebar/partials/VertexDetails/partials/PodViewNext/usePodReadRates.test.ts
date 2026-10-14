import { createElement, ReactNode } from "react";
import { renderHook, waitFor } from "@testing-library/react";
import { AppContext } from "../../../../../../../App";
import { latestByPod, usePodReadRates } from "./usePodReadRates";

jest.mock("../../../../../../../App", () => {
  const { createContext } = jest.requireActual("react");
  return { AppContext: createContext({}) };
});

const fetchMock = jest.fn();

function wrapper(disableMetricsCharts = false) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(
      AppContext.Provider,
      { value: { host: "", disableMetricsCharts } as any },
      children
    );
  };
}

function respondWith(byDuration: Record<string, any[]>) {
  return (_url: string, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body));
    return Promise.resolve({
      ok: true,
      json: () => Promise.resolve({ data: byDuration[body.duration], errMsg: null }),
    });
  };
}

const series = (pod: string, ...values: number[]) => ({
  metric: { pod },
  values: values.map((value, index) => [index, String(value)]),
});

const ids = { namespaceId: "ns", pipelineId: "mv", vertexId: "mv", type: "monoVertex" };

describe("usePodReadRates", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    global.fetch = fetchMock as any;
  });

  it("queries per-pod MonoVertex read rates and fills silent pods with zero", async () => {
    fetchMock.mockImplementation(
      respondWith({
        "5m": [series("mv-0", 4, 5)],
        "15m": [series("mv-0", 6), series("mv-1", 0.5)],
      })
    );

    const { result } = renderHook(() => usePodReadRates(ids), { wrapper: wrapper() });

    await waitFor(() => expect(result.current).toBeDefined());
    expect(result.current?.get("mv-0")).toEqual({ rate5m: 5, rate15m: 6 });
    expect(result.current?.get("mv-1")).toEqual({ rate5m: 0, rate15m: 0.5 });
    const bodies = fetchMock.mock.calls.map(([, init]) => JSON.parse(init.body));
    expect(bodies.map((body) => body.duration).sort()).toEqual(["15m", "5m"]);
    expect(bodies[0]).toEqual(
      expect.objectContaining({
        metric_name: "monovtx_read_total",
        pattern_name: "mono_vertex_throughput",
        dimension: "pod",
        filters: { namespace: "ns", mvtx_name: "mv" },
      })
    );
  });

  it("returns undefined when the long window has no series or metrics are disabled", async () => {
    fetchMock.mockImplementation(respondWith({ "5m": [], "15m": [] }));
    const { result } = renderHook(() => usePodReadRates(ids), { wrapper: wrapper() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(result.current).toBeUndefined();

    fetchMock.mockClear();
    const disabled = renderHook(() => usePodReadRates(ids), { wrapper: wrapper(true) });
    expect(disabled.result.current).toBeUndefined();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("reads the latest value of each pod series", () => {
    expect(latestByPod([series("a", 1, 2, 3), { metric: {}, values: [[0, "1"]] }])).toEqual(
      new Map([["a", 3]])
    );
    expect(latestByPod(null).size).toBe(0);
  });
});
