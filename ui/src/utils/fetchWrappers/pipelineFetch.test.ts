// Tests pipelineFetch.ts

import { usePipelineSummaryFetch } from "./pipelineFetch";
import { useFetch } from "./fetch";
import { renderHook } from "@testing-library/react";

jest.mock("../fetchWrappers/fetch");
const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;

describe("pipelineFetch test", () => {
  const mockData = {
    data: {
      id: "test-pipeline",
      name: "test-pipeline",
      namespace: "default",
      status: "Running",
      isb: "test-isb",
      isbStatus: "Running",
      isbNamespace: "default",
      isbService: "test-isb",
      isbServicePort: 8080,
      isbServicePath: "/",
      isbServiceUrl: "http://test-isb.default.svc.cluster.local:8080/",
      isbServiceType: "ClusterIP",
      isbServiceClusterIP: "",
      isbServiceExternalIPs: [],
      pipeline: {
        spec: {
          interStepBufferServiceName: "test-isb",
        },
      },
    },
  };

  it("should return pipeline data", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: mockData,
      error: null,
      loading: false,
    }));

    const { result } = renderHook(() =>
      usePipelineSummaryFetch({
        namespaceId: "default",
        pipelineId: "test-pipeline",
        addError: null,
      })
    );
    expect(result?.current?.data?.pipelineData.name).toEqual("test-pipeline");
  });

  it("Should test default isb", async () => {
    const mockData = {
      data: {
        id: "test-pipeline",
        name: "test-isb",
        namespace: "default",
        status: "Running",
        isb: "test-isb",
        isbStatus: "Running",
        isbNamespace: "default",
        isbService: "test-isb",
        isbServicePort: 8080,
        isbServicePath: "/",
        isbServiceUrl: "http://test-isb.default.svc.cluster.local:8080/",
        isbServiceType: "ClusterIP",
        isbServiceClusterIP: "",
        isbServiceExternalIPs: [],
      },
    };
    mockedUseFetch.mockImplementation(() => ({
      data: mockData,
      error: null,
      loading: false,
    }));

    const { result } = renderHook(() =>
      usePipelineSummaryFetch({
        namespaceId: "default",
        pipelineId: "test-pipeline",
        addError: null,
      })
    );
    expect(result?.current?.data?.isbData.name).toEqual("test-isb");
  });

  it("should return undefined pipeline data when loading", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: null,
      error: null,
      loading: true,
    }));

    const { result } = renderHook(() =>
      usePipelineSummaryFetch({
        namespaceId: "default",
        pipelineId: "test-pipeline",
        addError: null,
      })
    );
    expect(result?.current?.data?.pipelineData).toEqual(undefined);
  });

  it("should return error", async () => {
    mockedUseFetch.mockImplementation(() => ({
      data: null,
      error: "error",
      loading: false,
    }));

    const { result } = renderHook(() =>
      usePipelineSummaryFetch({
        namespaceId: "default",
        pipelineId: "test-pipeline",
        addError: null,
      })
    );
    expect(result?.current?.error).toEqual("error");
  });
});
