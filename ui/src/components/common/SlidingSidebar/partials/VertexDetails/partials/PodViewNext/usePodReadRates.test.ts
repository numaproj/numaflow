import { createElement, ReactNode } from "react";
import { act, renderHook, waitFor } from "@testing-library/react";
import { AppContext } from "../../../../../../../App";
import { Pod } from "../../../../../../../types/declarations/pods";
import { podReadRatesPath, ratesByPodName, usePodReadRates } from "./usePodReadRates";

jest.mock("../../../../../../../App", () => {
  const { createContext } = jest.requireActual("react");
  return { AppContext: createContext({}) };
});

jest.mock("../../../../../../../utils", () => ({
  getBaseHref: () => "",
}));

const fetchMock = jest.fn();

function wrapper() {
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(AppContext.Provider, { value: { host: "" } as any }, children);
  };
}

function pod(name: string): Pod {
  return { name, containers: [], containerSpecMap: new Map() };
}

const pods = [pod("orders-ingest-mv-1-abc"), pod("orders-ingest-mv-10-def"), pod("orders-ingest-mv-2-ghi")];

const ids = {
  namespaceId: "ns",
  pipelineId: "orders",
  vertexId: "orders-ingest",
  type: "monoVertex",
  pods,
};

describe("usePodReadRates", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    global.fetch = fetchMock as any;
  });

  it("loads MonoVertex rates from API v2 and joins hostnames without marking unknown pods idle", async () => {
    fetchMock.mockResolvedValue({
      ok: true,
      json: () =>
        Promise.resolve({
          pods: [
            { hostname: "orders-ingest-mv-1", replica: 1, rate5m: 4, rate15m: 5 },
            { hostname: "orders-ingest-mv-2", replica: 2, rate15m: 0 },
          ],
        }),
    });

    const { result } = renderHook(() => usePodReadRates(ids), { wrapper: wrapper() });

    await waitFor(() => expect(result.current).toBeDefined());
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v2/namespaces/ns/mono-vertices/orders-ingest/pods/read-rates",
      expect.objectContaining({ signal: expect.anything() })
    );
    expect(result.current?.get("orders-ingest-mv-1-abc")).toEqual({ rate5m: 4, rate15m: 5 });
    expect(result.current?.get("orders-ingest-mv-10-def")).toEqual({});
    expect(result.current?.get("orders-ingest-mv-2-ghi")).toEqual({
      rate5m: undefined,
      rate15m: 0,
    });
  });

  it("uses the pipeline vertex path", async () => {
    fetchMock.mockResolvedValue({ ok: true, json: () => Promise.resolve({ pods: [] }) });
    const { result } = renderHook(
      () => usePodReadRates({ ...ids, type: "pipeline", vertexId: "map", pods: [pod("orders-map-0-xyz")] }),
      { wrapper: wrapper() }
    );
    await waitFor(() => expect(result.current).toBeDefined());
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v2/namespaces/ns/pipelines/orders/vertices/map/pods/read-rates",
      expect.anything()
    );
    expect(result.current?.get("orders-map-0-xyz")).toEqual({});
  });

  it("clears rates when the vertex changes and ignores an older response", async () => {
    const resolvers: Array<(value: unknown) => void> = [];
    fetchMock.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolvers.push(resolve);
        })
    );
    const { result, rerender } = renderHook((props: typeof ids) => usePodReadRates(props), {
      wrapper: wrapper(),
      initialProps: ids,
    });

    await act(async () => {
      await Promise.resolve();
    });
    const first = resolvers[0];

    rerender({ ...ids, vertexId: "other", pods: [pod("other-0-zzz")] });
    expect(result.current).toBeUndefined();

    await act(async () => {
      await Promise.resolve();
    });
    const latest = resolvers[resolvers.length - 1];
    await act(async () => {
      latest({
        ok: true,
        json: () => Promise.resolve({ pods: [{ hostname: "other-0", rate15m: 0 }] }),
      });
      first({
        ok: true,
        json: () =>
          Promise.resolve({
            pods: [{ hostname: "orders-ingest-mv-1", rate5m: 4, rate15m: 5 }],
          }),
      });
      await Promise.resolve();
    });
    await waitFor(() =>
      expect(result.current?.get("other-0-zzz")).toEqual({ rate5m: undefined, rate15m: 0 })
    );
    expect(result.current?.get("orders-ingest-mv-1-abc")).toBeUndefined();
  });

  it("does not fetch while the Pods tab is hidden", async () => {
    const { result } = renderHook(() => usePodReadRates({ ...ids, active: false }), {
      wrapper: wrapper(),
    });
    await act(async () => {
      await Promise.resolve();
    });
    expect(fetchMock).not.toHaveBeenCalled();
    expect(result.current).toBeUndefined();
  });

  it("returns undefined when the daemon request fails", async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 503, json: () => Promise.resolve({}) });
    const { result } = renderHook(() => usePodReadRates(ids), { wrapper: wrapper() });
    await waitFor(() => expect(fetchMock).toHaveBeenCalled());
    expect(result.current).toBeUndefined();
  });
});

describe("pod rate helpers", () => {
  it("builds encoded paths", () => {
    expect(podReadRatesPath({ ...ids, namespaceId: "team a" })).toBe(
      "/api/v2/namespaces/team%20a/mono-vertices/orders-ingest/pods/read-rates"
    );
  });

  it("does not let replica 1 match replica 10", () => {
    const rates = ratesByPodName(pods, [{ hostname: "orders-ingest-mv-1", rate15m: 0 }]);
    expect(rates.get("orders-ingest-mv-1-abc")).toEqual({ rate5m: undefined, rate15m: 0 });
    expect(rates.get("orders-ingest-mv-10-def")).toEqual({});
  });
});
