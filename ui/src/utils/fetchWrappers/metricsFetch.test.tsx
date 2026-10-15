import { renderHook, waitFor } from "@testing-library/react";
import { AppContext } from "../../App";
import { useMetricsFetch } from "./metricsFetch";

describe("useMetricsFetch", () => {
  it("aborts the previous request when metric parameters change", async () => {
    const fetchMock = jest.fn(
      () => new Promise<Response>(() => undefined)
    );
    global.fetch = fetchMock as unknown as typeof fetch;
    const filters = { namespace: "default", mvtx_name: "demo" };
    const wrapper = ({ children }: any) => (
      <AppContext.Provider value={{ host: "" } as any}>
        {children}
      </AppContext.Provider>
    );

    const { rerender, unmount } = renderHook(
      ({ metricReq }) => useMetricsFetch({ metricReq, filters }),
      {
        initialProps: {
          metricReq: {
            metric_name: "monovtx_read_total",
            dimension: "mono-vertex",
            duration: "1m",
          },
        },
        wrapper,
      }
    );

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    const firstSignal = fetchMock.mock.calls[0][1]?.signal as AbortSignal;
    expect(firstSignal.aborted).toBe(false);

    rerender({
      metricReq: {
        metric_name: "monovtx_read_total",
        dimension: "mono-vertex",
        duration: "5m",
      },
    });

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    const secondSignal = fetchMock.mock.calls[1][1]?.signal as AbortSignal;
    expect(firstSignal.aborted).toBe(true);
    expect(secondSignal.aborted).toBe(false);

    unmount();
    expect(secondSignal.aborted).toBe(true);
  });
});
