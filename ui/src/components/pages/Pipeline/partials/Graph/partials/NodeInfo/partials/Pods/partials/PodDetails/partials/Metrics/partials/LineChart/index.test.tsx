import { fireEvent, render, screen, waitFor } from "@testing-library/react";
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

jest.mock("react-bootstrap-daterangepicker", () => ({
  __esModule: true,
  default: ({ children }: { children: any }) => children,
}));

const metric = {
  metric_name: "monovtx_read_total",
  pattern_name: "rate",
  display_name: "MonoVertex Read Processing Rate",
  dimensions: [
    {
      name: "mono-vertex",
      params: [
        { Name: "duration", Required: true },
        { Name: "start_time", Required: false },
        { Name: "end_time", Required: false },
      ],
      filters: [
        { Name: "namespace", Required: true },
        { Name: "mvtx_name", Required: true },
      ],
    },
    {
      name: "pod",
      params: [
        { Name: "duration", Required: true },
        { Name: "start_time", Required: false },
        { Name: "end_time", Required: false },
      ],
      filters: [
        { Name: "namespace", Required: true },
        { Name: "mvtx_name", Required: true },
        { Name: "pod", Required: false },
      ],
    },
  ],
};

const renderChart = (search: string) =>
  render(
    <MemoryRouter initialEntries={[`/${search}`]}>
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
    renderChart(
      "?namespace=default&pipeline=simple-mono-vertex&type=monoVertex&vertex=simple-mono-vertex&vertexTab=metrics&metric=monovtx_read_total"
    );

    await waitFor(() => {
      expect(mockUseMetricsFetch).toHaveBeenCalled();
    });
    const latest = mockUseMetricsFetch.mock.calls.at(-1)?.[0];
    expect(latest.metricReq.dimension).toBe("mono-vertex");
    expect(latest.metricReq.duration).toBe("1m");
    expect(latest.filters.namespace).toBe("default");
    expect(latest.filters.mvtx_name).toBe("simple-mono-vertex");
    expect(latest.metricReq.start_time).toBeTruthy();
    expect(latest.metricReq.end_time).toBeTruthy();
    expect(new Date(latest.metricReq.start_time).toISOString()).toBe(
      latest.metricReq.start_time
    );
    expect(new Date(latest.metricReq.end_time).toISOString()).toBe(
      latest.metricReq.end_time
    );
    const startMs = new Date(latest.metricReq.start_time).getTime();
    const endMs = new Date(latest.metricReq.end_time).getTime();
    expect(endMs - startMs).toBeGreaterThanOrEqual(55 * 60 * 1000);
    expect(endMs - startMs).toBeLessThanOrEqual(65 * 60 * 1000);
  });

  it("uses metricStart and metricEnd from the URL instead of last hour", async () => {
    renderChart(
      "?namespace=default&pipeline=simple-mono-vertex&type=monoVertex&vertex=simple-mono-vertex&vertexTab=metrics&metric=monovtx_read_total&metricStart=2026-01-01T00:00:00.000Z&metricEnd=2026-01-01T00:10:00.000Z"
    );

    await waitFor(() => {
      const latest = mockUseMetricsFetch.mock.calls.at(-1)?.[0];
      expect(latest?.metricReq.start_time).toBeTruthy();
      expect(latest?.metricReq.end_time).toBeTruthy();
    });
    const latest = mockUseMetricsFetch.mock.calls.at(-1)?.[0];
    expect(new Date(latest.metricReq.start_time).toISOString()).toBe(
      "2026-01-01T00:00:00.000Z"
    );
    expect(new Date(latest.metricReq.end_time).toISOString()).toBe(
      "2026-01-01T00:10:00.000Z"
    );
  });

  it("keeps a live Dimension change to Pod when the URL still says mono-vertex", async () => {
    renderChart(
      "?namespace=default&pipeline=simple-mono-vertex&type=monoVertex&vertex=simple-mono-vertex&vertexTab=metrics&metric=monovtx_read_total&metricDimension=mono-vertex"
    );

    await waitFor(() => {
      expect(screen.getByLabelText("Dimension")).toHaveTextContent("MonoVertex");
    });

    fireEvent.mouseDown(screen.getByLabelText("Dimension"));
    fireEvent.click(await screen.findByRole("option", { name: "Pod" }));

    await waitFor(() => {
      expect(screen.getByLabelText("Dimension")).toHaveTextContent("Pod");
      const latest = mockUseMetricsFetch.mock.calls.at(-1)?.[0];
      expect(latest.metricReq.dimension).toBe("pod");
    });
  });

  it("keeps container series when the pod object is missing", async () => {
    mockUseMetricsFetch.mockReturnValue({
      chartData: [
        { metric: { container: "numa" }, values: [[1, "1"]] },
        { metric: { container: "udf" }, values: [[1, "2"]] },
      ],
      error: null,
      isLoading: false,
    });

    const containerMetric = {
      metric_name: "container_cpu",
      pattern_name: "gauge",
      display_name: "Container CPU Utilization",
      dimensions: [
        {
          name: "container",
          params: [{ Name: "duration", Required: true }],
          filters: [{ Name: "namespace", Required: true }],
        },
      ],
    };

    render(
      <MemoryRouter
        initialEntries={[
          "/?namespace=default&pipeline=simple-mono-vertex&type=monoVertex&vertex=simple-mono-vertex&vertexTab=metrics&metric=container_cpu",
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
            metric={containerMetric}
            podName="simple-mono-vertex-mv-0"
          />
        </AppContext.Provider>
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(
        screen.queryByText("No data for the selected filters.")
      ).not.toBeInTheDocument();
    });
  });
});
