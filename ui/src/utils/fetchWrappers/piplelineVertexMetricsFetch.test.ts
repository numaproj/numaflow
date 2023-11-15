// Tests pipelineVertexMetricsFetch.ts

import { usePiplelineVertexMetricsFetch } from "./piplelineVertexMetricsFetch";
import { useFetch } from "./fetch";
import { renderHook } from "@testing-library/react";

jest.mock("../fetchWrappers/fetch");
const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;

describe("pipelineVertexMetricsFetch test", () => {
  const mockData = {
    data: {
      "test-vertex": [
        {
          processingRates: {
            "1m": 1,
            "5m": 5,
            "15m": 15,
          },
        },
      ],
    },
  };

  it("should return vertex metrics data", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: mockData,
      error: null,
      loading: false,
    }));

    const { result } = renderHook(() =>
      usePiplelineVertexMetricsFetch({
        namespace: "default",
        pipeline: "test-pipeline",
        loadOnRefresh: false,
      })
    );
    expect(
      result?.current?.data && result?.current?.data[0]?.metrics[0].fifteenM
    ).toEqual("15.00");
  });
  it("should return undefined vertex metrics data when loading", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: null,
      error: null,
      loading: true,
    }));

    const { result } = renderHook(() =>
      usePiplelineVertexMetricsFetch({
        namespace: "default",
        pipeline: "test-pipeline",
        loadOnRefresh: false,
      })
    );
    expect(result?.current?.data).toEqual(undefined);
  });

  it("should return undefined vertex metrics data when error", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: null,
      error: "error",
      loading: false,
    }));

    const { result } = renderHook(() =>
      usePiplelineVertexMetricsFetch({
        namespace: "default",
        pipeline: "test-pipeline",
        loadOnRefresh: false,
      })
    );
    expect(result.current.error).toEqual("error");
  });
});
