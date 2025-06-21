import React, { act } from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { Cluster } from "./index";
import { BrowserRouter } from "react-router-dom";
import { AppContext } from "../../../App";
import { useClusterSummaryFetch } from "../../../utils/fetchWrappers/clusterSummaryFetch";

jest.mock("../../../utils/fetchWrappers/clusterSummaryFetch");

window.ResizeObserver = class ResizeObserver {
  observe() {
    // do nothing
  }
  unobserve() {
    // do nothing
  }
  disconnect() {
    // do nothing
  }
};
const mockData = {
  namespacesCount: 3,
  pipelinesCount: 4,
  pipelinesActiveCount: 3,
  pipelinesInactiveCount: 1,
  pipelinesHealthyCount: 3,
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
      pipelinesCount: 4,
      pipelinesActiveCount: 3,
      pipelinesInactiveCount: 1,
      pipelinesHealthyCount: 3,
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
};

describe("Cluster", () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });
  it("renders without crashing", async () => {
    useClusterSummaryFetch.mockImplementation(() => ({
      data: mockData,
      loading: false,
      error: null,
    }));
    render(
      <AppContext.Provider value="">
        <BrowserRouter>
          <Cluster />
        </BrowserRouter>
      </AppContext.Provider>
    );
    await act(() => {
      expect(screen.getByTestId("summary-page-layout")).toBeInTheDocument();
    });
  });

  it("renders loading indicator", async () => {
    useClusterSummaryFetch.mockImplementation(() => ({
      data: null,
      loading: true,
      error: null,
    }));
    render(
      <AppContext.Provider value="">
        <BrowserRouter>
          <Cluster />
        </BrowserRouter>
      </AppContext.Provider>
    );

    await act(() => {
      expect(screen.getByTestId("cluster-loading-icon")).toBeInTheDocument();
    });
  });

  it("Loads when data is null", async () => {
    useClusterSummaryFetch.mockImplementation(() => ({
      data: null,
      loading: false,
      error: null,
    }));
    render(
      <AppContext.Provider value="">
        <BrowserRouter>
          <Cluster />
        </BrowserRouter>
      </AppContext.Provider>
    );

    await act(() => {
      expect(screen.getByTestId("summary-page-layout")).toBeInTheDocument();
    });
  });

  it("renders error message", async () => {
    useClusterSummaryFetch.mockImplementation(() => ({
      data: null,
      loading: false,
      error: "Error",
    }));
    render(
      <AppContext.Provider value="">
        <BrowserRouter>
          <Cluster />
        </BrowserRouter>
      </AppContext.Provider>
    );

    await act(() => {
      expect(
        screen.getByText("Error loading cluster namespaces")
      ).toBeInTheDocument();
    });
  });
});
