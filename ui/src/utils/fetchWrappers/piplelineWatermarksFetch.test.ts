// Tests pipelineWatermarksFetch.ts

import { usePiplelineWatermarksFetch } from "./piplelineWatermarksFetch";
import { useFetch } from "./fetch";
import { renderHook } from "@testing-library/react";

jest.mock("../fetchWrappers/fetch");
const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;

describe("pipelineWatermarksFetch test", () => {
  const mockData = {
    data: [
      {
        edge: "test-edge",
        watermarks: [1, 2, 3],
      },
    ],
  };

  it("should return watermarks data", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: mockData,
      error: null,
      loading: false,
    }));

    const { result } = renderHook(() =>
      usePiplelineWatermarksFetch({
        namespace: "default",
        pipeline: "test-pipeline",
        loadOnRefresh: false,
      })
    );
    expect(
      result?.current?.data && result?.current?.data[0]?.watermarks[0].watermark
    ).toEqual(1);
  });
  it("should return undefined watermarks data when loading", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: null,
      error: null,
      loading: true,
    }));

    const { result } = renderHook(() =>
      usePiplelineWatermarksFetch({
        namespace: "default",
        pipeline: "test-pipeline",
        loadOnRefresh: false,
      })
    );
    expect(result?.current?.data).toEqual(undefined);
  });

  it("should return undefined watermarks data when error", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: null,
      error: "error",
      loading: false,
    }));

    const { result } = renderHook(() =>
      usePiplelineWatermarksFetch({
        namespace: "default",
        pipeline: "test-pipeline",
        loadOnRefresh: false,
      })
    );
    expect(result?.current?.error).toEqual("error");
  });
});
