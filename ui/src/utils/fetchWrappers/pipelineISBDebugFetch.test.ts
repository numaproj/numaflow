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

  it("excludes stale retained data for a failed endpoint", async () => {
    const streamsResponse = {
      data: { data: { streams: [{ stream: "stale-stream" }] } },
      error: "streams failed",
      loading: false,
    };
    const consumersResponse = {
      data: { data: { consumers: [{ consumer: "current-consumer" }] } },
      error: undefined,
      loading: false,
    };
    const kvStoresResponse = {
      data: { data: { kvStores: [{ bucket: "current-kv" }] } },
      error: undefined,
      loading: false,
    };
    mockedUseFetch.mockImplementation((url: string) => {
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
      expect(result.current.data?.streams).toBeUndefined();
      expect(result.current.data?.consumers?.consumers[0].consumer).toEqual(
        "current-consumer"
      );
      expect(result.current.data?.kvStores?.kvStores[0].bucket).toEqual(
        "current-kv"
      );
      expect(result.current.error).toEqual("streams failed");
    });
  });

  it("clears aggregate data when every endpoint has stale retained data and an error", async () => {
    const streamsResponse = {
      data: { data: { streams: [{ stream: "stale-stream" }] } },
      error: "streams failed",
      loading: false,
    };
    const consumersResponse = {
      data: { data: { consumers: [{ consumer: "stale-consumer" }] } },
      error: "consumers failed",
      loading: false,
    };
    const kvStoresResponse = {
      data: { data: { kvStores: [{ bucket: "stale-kv" }] } },
      error: "kv failed",
      loading: false,
    };
    mockedUseFetch.mockImplementation((url: string) => {
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
      expect(result.current.data).toBeUndefined();
      expect(result.current.error).toEqual("streams failed");
    });
  });
});
