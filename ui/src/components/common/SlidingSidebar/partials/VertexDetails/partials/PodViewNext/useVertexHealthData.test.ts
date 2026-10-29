import { createElement, ReactNode } from "react";
import { renderHook, waitFor } from "@testing-library/react";
import { AppContext } from "../../../../../../../App";
import { useVertexHealthData } from "./useVertexHealthData";

jest.mock("../../../../../../../App", () => {
  const { createContext } = jest.requireActual("react");
  return { AppContext: createContext({}) };
});

const fetchMock = jest.fn();

function wrapper(disableMetricsCharts: boolean) {
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(
      AppContext.Provider,
      { value: { host: "", disableMetricsCharts } as any },
      children
    );
  };
}

function respond(url: string, init?: RequestInit) {
  if (url.endsWith("/api/v2/namespaces/ns/pipelines/p/vertices/cat/status")) {
    return Promise.resolve({
      ok: true,
      json: () => Promise.resolve({ replicas: { ready: 3, desired: 5 } }),
    });
  }
  if (url.endsWith("/api/v2/namespaces/ns/pipelines/p/vertices/cat/summary")) {
    return Promise.resolve({
      ok: true,
      json: () => Promise.resolve({ lastScaledAt: "2026-10-12T11:50:00Z" }),
    });
  }
  let data: any = [];
  if (url.endsWith("/health")) data = { dataHealthStatus: "healthy", dataHealthCode: "D1" };
  else if (url.endsWith("/watermarks")) data = [{ from: "cat", to: "out", watermarks: ["1"] }];
  else if (url.includes("/mono-vertices/")) data = { monoVertex: { status: { readyReplicas: 3, desiredReplicas: 3 } } };
  else if (init?.method === "POST") data = [{ values: [[1, "2"]] }];
  return Promise.resolve({ ok: true, json: () => Promise.resolve({ data, errMsg: null }) });
}

const proxyBodies = () =>
  fetchMock.mock.calls
    .filter(([url]) => String(url).endsWith("/api/v1/metrics-proxy"))
    .map(([, init]) => JSON.parse(init.body));

describe("useVertexHealthData", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    fetchMock.mockImplementation(respond);
    global.fetch = fetchMock as any;
  });

  it("requests MonoVertex series in a 30 minute window and the MonoVertex status", async () => {
    const { result } = renderHook(
      () =>
        useVertexHealthData({
          namespaceId: "ns",
          pipelineId: "mv",
          vertexId: "mv",
          type: "monoVertex",
          active: true,
        }),
      { wrapper: wrapper(false) }
    );

    await waitFor(() => expect(result.current.scaleStatus).toEqual({ readyReplicas: 3, desiredReplicas: 3 }));
    await waitFor(() => expect(result.current.series.p99).toEqual([2]));
    const bodies = proxyBodies();
    expect(bodies).toHaveLength(10);
    const p99 = bodies.find((body) => body.metric_name === "monovtx_processing_time_bucket");
    expect(p99).toEqual(
      expect.objectContaining({
        pattern_name: "mono_vertex_histogram",
        quantile: "0.99",
        filters: { namespace: "ns", mvtx_name: "mv" },
      })
    );
    expect(Date.parse(p99.end_time) - Date.parse(p99.start_time)).toBe(30 * 60_000);
    expect(result.current.health?.dataHealthCode).toBe("D1");
    expect(fetchMock.mock.calls.some(([url]) => String(url).endsWith("/watermarks"))).toBe(false);
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes("/api/v2/"))).toBe(false);
  });

  it("fetches pipeline watermarks and health but no MonoVertex status", async () => {
    const { result } = renderHook(
      () =>
        useVertexHealthData({
          namespaceId: "ns",
          pipelineId: "p",
          vertexId: "cat",
          type: "udf",
          active: true,
        }),
      { wrapper: wrapper(false) }
    );

    await waitFor(() =>
      expect(result.current.scaleStatus).toEqual({
        readyReplicas: 3,
        desiredReplicas: 5,
        lastScaledAt: "2026-10-12T11:50:00Z",
      })
    );
    await waitFor(() => expect(result.current.watermarkEdges).toHaveLength(1));
    const urls = fetchMock.mock.calls.map(([url]) => String(url));
    expect(urls).toContain("/api/v1/namespaces/ns/pipelines/p/health");
    expect(urls).toContain("/api/v2/namespaces/ns/pipelines/p/vertices/cat/status");
    expect(urls).toContain("/api/v2/namespaces/ns/pipelines/p/vertices/cat/summary");
    expect(urls.some((url) => url.includes("/mono-vertices/"))).toBe(false);
    await waitFor(() => expect(proxyBodies()).toHaveLength(8));
    expect(proxyBodies().every((body) => body.filters.pipeline === "p")).toBe(true);
  });

  it("skips Prometheus when metrics charts are disabled", async () => {
    const { result } = renderHook(
      () =>
        useVertexHealthData({
          namespaceId: "ns",
          pipelineId: "mv",
          vertexId: "mv",
          type: "monoVertex",
          active: true,
        }),
      { wrapper: wrapper(true) }
    );

    await waitFor(() => expect(result.current.health).toBeDefined());
    expect(result.current.metricsEnabled).toBe(false);
    expect(proxyBodies()).toHaveLength(0);
    expect(result.current.series.p99).toBeUndefined();
  });

  it("does not poll hidden Pod Views", () => {
    const { result } = renderHook(
      () =>
        useVertexHealthData({
          namespaceId: "ns",
          pipelineId: "p",
          vertexId: "cat",
          type: "udf",
          active: false,
        }),
      { wrapper: wrapper(false) }
    );

    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.current.series.p99).toBeUndefined();
  });
});
