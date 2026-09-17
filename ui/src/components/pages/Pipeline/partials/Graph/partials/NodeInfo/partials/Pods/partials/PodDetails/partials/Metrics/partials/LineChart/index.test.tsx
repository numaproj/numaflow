import { render, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import LineChartComponent from "./index";
import { AppContext } from "../../../../../../../../../../../../../../../App";

import "@testing-library/jest-dom";

const mockUseMetricsFetch = jest.fn();

jest.mock(
  "../../../../../../../../../../../../../../../utils/fetchWrappers/metricsFetch",
  () => ({
    useMetricsFetch: (props: any) => mockUseMetricsFetch(props),
  })
);

const metric = {
  metric_name: "monovtx_read_total",
  pattern_name: "rate",
  display_name: "MonoVertex Read Processing Rate",
  dimensions: [
    {
      name: "mono-vertex",
      params: [{ Name: "duration", Required: true }],
      filters: [
        { Name: "namespace", Required: true },
        { Name: "mvtx_name", Required: true },
      ],
    },
  ],
};

describe("LineChart URL restore", () => {
  beforeEach(() => {
    mockUseMetricsFetch.mockReset();
    mockUseMetricsFetch.mockReturnValue({
      chartData: [{ metric: { mvtx_name: "simple-mono-vertex" }, values: [[1, "1"]] }],
      error: null,
      isLoading: false,
    });
  });

  it("keeps dropdown defaults when the URL only names the metric", async () => {
    render(
      <MemoryRouter
        initialEntries={[
          "/?namespace=default&pipeline=simple-mono-vertex&type=monoVertex&vertex=simple-mono-vertex&vertexTab=metrics&metric=monovtx_read_total",
        ]}
      >
        <AppContext.Provider
          value={{ addError: jest.fn(), host: "", disableMetricsCharts: false } as any}
        >
          <LineChartComponent
            namespaceId="default"
            pipelineId="simple-mono-vertex"
            type="monoVertex"
            vertexId="simple-mono-vertex"
            metric={metric}
          />
        </AppContext.Provider>
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(mockUseMetricsFetch).toHaveBeenCalled();
    });
    const latest = mockUseMetricsFetch.mock.calls.at(-1)?.[0];
    expect(latest.metricReq.dimension).toBe("mono-vertex");
    expect(latest.metricReq.duration).toBe("1m");
    expect(latest.filters.namespace).toBe("default");
    expect(latest.filters.mvtx_name).toBe("simple-mono-vertex");
  });
});
