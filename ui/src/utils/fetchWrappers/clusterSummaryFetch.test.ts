import React from "react";
import { renderHook, waitFor } from "@testing-library/react";
import { useClusterSummaryFetch } from "./clusterSummaryFetch";
import { useFetch } from "./fetch";

jest.mock("../fetchWrappers/fetch");
const mockedUseFetch = useFetch as jest.MockedFunction<typeof useFetch>;

describe("ClusterSummary test", () => {
  const mockData = {
    data: {
      namespacesCount: 3,
      pipelinesCount: 1,
      pipelinesActiveCount: 1,
      pipelinesInactiveCount: 0,
      pipelinesHealthyCount: 1,
      pipelinesWarningCount: 0,
      pipelinesCriticalCount: 0,
      isbsCount: 5,
      isbsActiveCount: 5,
      isbsInactiveCount: 0,
      isbsHealthyCount: 5,
      isbsWarningCount: 0,
      isbsCriticalCount: 0,
      nameSpaceSummaries: [
        {
          name: "default",
          isEmpty: true,
          pipelinesCount: 0,
          pipelinesActiveCount: 0,
          pipelinesInactiveCount: 0,
          pipelinesHealthyCount: 0,
          pipelinesWarningCount: 0,
          pipelinesCriticalCount: 0,
          isbsCount: 0,
          isbsActiveCount: 0,
          isbsInactiveCount: 0,
          isbsHealthyCount: 0,
          isbsWarningCount: 0,
          isbsCriticalCount: 0,
        },
        {
          name: "local-path-storage",
          isEmpty: true,
          pipelinesCount: 0,
          pipelinesActiveCount: 0,
          pipelinesInactiveCount: 0,
          pipelinesHealthyCount: 0,
          pipelinesWarningCount: 0,
          pipelinesCriticalCount: 0,
          isbsCount: 0,
          isbsActiveCount: 0,
          isbsInactiveCount: 0,
          isbsHealthyCount: 0,
          isbsWarningCount: 0,
          isbsCriticalCount: 0,
        },
        {
          name: "numaflow-system",
          isEmpty: false,
          pipelinesCount: 1,
          pipelinesActiveCount: 1,
          pipelinesInactiveCount: 0,
          pipelinesHealthyCount: 1,
          pipelinesWarningCount: 0,
          pipelinesCriticalCount: 0,
          isbsCount: 5,
          isbsActiveCount: 5,
          isbsInactiveCount: 0,
          isbsHealthyCount: 5,
          isbsWarningCount: 0,
          isbsCriticalCount: 0,
        },
      ],
    },
    loading: false,
  };

  // it("should return cluster summary", async () => {
  //   mockedUseFetch.mockReturnValue({
  //     data: mockData,
  //     error: null,
  //     loading: false,
  //   });
  //   const { result } = renderHook(() =>
  //     useClusterSummaryFetch({
  //       loadOnRefresh: false,
  //       addError: (error) => {},
  //     })
  //   );
  //   await waitFor(() => {
  //     expect(result.current.data).toEqual(mockData);
  //   });
  // });
  test.todo("");
});
