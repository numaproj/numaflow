import { act, renderHook, waitFor } from "@testing-library/react";
import { usePipelineISBDebugFetch } from "./pipelineISBDebugFetch";
import { useFetch } from "./fetch";

jest.mock("../fetchWrappers/fetch");
const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;

describe("pipelineISBDebugFetch test", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("clears stale data while request parameters change", async () => {
    let mode: "initial" | "loading" | "next" = "initial";
    const loadingResponse = {
      data: undefined,
      error: undefined,
      loading: true,
    };
    const responses = {
      initial: {
        streams: {
          data: { data: { streams: [{ stream: "cat-stream" }] } },
          error: undefined,
          loading: false,
        },
        consumers: {
          data: { data: { consumers: [] } },
          error: undefined,
          loading: false,
        },
        kvStores: {
          data: { data: { kvStores: [] } },
          error: undefined,
          loading: false,
        },
      },
      next: {
        streams: {
          data: { data: { streams: [{ stream: "dog-stream" }] } },
          error: undefined,
          loading: false,
        },
        consumers: {
          data: { data: { consumers: [] } },
          error: undefined,
          loading: false,
        },
        kvStores: {
          data: { data: { kvStores: [] } },
          error: undefined,
          loading: false,
        },
      },
    };
    mockedUseFetch.mockImplementation((url: string) => {
      if (mode === "loading") {
        return loadingResponse;
      }
      if (url.includes("/streams")) {
        return responses[mode].streams;
      }
      if (url.includes("/consumers")) {
        return responses[mode].consumers;
      }
      return responses[mode].kvStores;
    });
    const { result, rerender } = renderHook(
      ({ vertexId }) =>
        usePipelineISBDebugFetch({
          namespaceId: "ns",
          pipelineId: "pl",
          vertexId,
          enabled: true,
        }),
      { initialProps: { vertexId: "cat" } }
    );

    await waitFor(() => {
      expect(result.current.data?.streams?.streams[0].stream).toEqual(
        "cat-stream"
      );
    });

    mode = "loading";
    rerender({ vertexId: "dog" });

    await waitFor(() => {
      expect(result.current.data).toBeUndefined();
      expect(result.current.loading).toBe(true);
    });

    mode = "next";
    rerender({ vertexId: "dog" });

    await waitFor(() => {
      expect(result.current.data?.streams?.streams[0].stream).toEqual(
        "dog-stream"
      );
    });
  });

  it("clears stale data on refresh", async () => {
    let loading = false;
    const loadingResponse = {
      data: undefined,
      error: undefined,
      loading: true,
    };
    const streamsResponse = {
      data: { data: { streams: [{ stream: "cat-stream" }] } },
      error: undefined,
      loading: false,
    };
    const consumersResponse = {
      data: { data: { consumers: [] } },
      error: undefined,
      loading: false,
    };
    const kvStoresResponse = {
      data: { data: { kvStores: [] } },
      error: undefined,
      loading: false,
    };
    mockedUseFetch.mockImplementation((url: string) => {
      if (loading) {
        return loadingResponse;
      }
      if (url.includes("/streams")) {
        return streamsResponse;
      }
      if (url.includes("/consumers")) {
        return consumersResponse;
      }
      return kvStoresResponse;
    });
    const { result } = renderHook(() =>
      usePipelineISBDebugFetch({
        namespaceId: "ns",
        pipelineId: "pl",
        vertexId: "cat",
        enabled: true,
      })
    );

    await waitFor(() => {
      expect(result.current.data?.streams?.streams[0].stream).toEqual(
        "cat-stream"
      );
    });

    loading = true;
    act(() => {
      result.current.refresh();
    });

    await waitFor(() => {
      expect(result.current.data).toBeUndefined();
      expect(result.current.loading).toBe(true);
    });
  });
});
