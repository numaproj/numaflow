import { renderHook, waitFor } from "@testing-library/react";
import { useControllerInfoFetch } from "./controllerInfoFetch";

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useLocation: () => ({
    pathname: "/",
  }),
}));

describe("controllerInfoFetch", () => {
  const originalFetch = global.fetch;

  afterEach(() => {
    global.fetch = originalFetch;
    jest.clearAllMocks();
  });

  it("returns controller info when found", async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        data: {
          found: true,
          namespace: "app-ns",
          name: "numaflow-controller",
          version: "v1.7.5",
          image: "quay.io/numaproj/numaflow:v1.7.5",
          namespaced: true,
          managedNamespace: "app-ns",
        },
      }),
    }) as any;

    const { result } = renderHook(() =>
      useControllerInfoFetch({
        host: "",
        namespace: "app-ns",
        managedNamespace: "app-ns",
        namespaced: true,
      })
    );

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.controllerInfo?.found).toBe(true);
    expect(result.current.controllerInfo?.version).toBe("v1.7.5");
    expect(result.current.error).toBe("");
  });

  it("falls back to managedNamespace in cluster mode when current ns has no controller", async () => {
    global.fetch = jest
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          data: { found: false, namespace: "team-a", namespaced: false },
        }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          data: {
            found: true,
            namespace: "numaflow-system",
            name: "numaflow-controller",
            version: "v1.4.0",
            namespaced: false,
          },
        }),
      }) as any;

    const { result } = renderHook(() =>
      useControllerInfoFetch({
        host: "",
        namespace: "team-a",
        managedNamespace: "numaflow-system",
        namespaced: false,
      })
    );

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(global.fetch).toHaveBeenCalledTimes(2);
    expect(result.current.controllerInfo?.found).toBe(true);
    expect(result.current.controllerInfo?.namespace).toBe("numaflow-system");
    expect(result.current.controllerInfo?.version).toBe("v1.4.0");
  });

  it("surfaces fetch errors", async () => {
    global.fetch = jest.fn().mockResolvedValue({
      ok: false,
      status: 500,
    }) as any;

    const { result } = renderHook(() =>
      useControllerInfoFetch({
        host: "",
        namespace: "app-ns",
      })
    );

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.error).toContain("Response code: 500");
    expect(result.current.controllerInfo).toBeUndefined();
  });
});
