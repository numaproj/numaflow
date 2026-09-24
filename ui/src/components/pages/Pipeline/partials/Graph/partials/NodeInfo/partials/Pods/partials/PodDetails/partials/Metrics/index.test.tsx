import { useState } from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { Metrics } from "./index";
import { VertexDetailsContext } from "../../../../../../../../../../../../common/SlidingSidebar/partials/VertexDetails";

import "@testing-library/jest-dom";

const mockUseMetricsDiscoveryDataFetch = jest.fn();

jest.mock(
  "../../../../../../../../../../../../../utils/fetchWrappers/metricsDiscoveryDataFetch",
  () => ({
    useMetricsDiscoveryDataFetch: (props: any) =>
      mockUseMetricsDiscoveryDataFetch(props),
  })
);

jest.mock("./partials/LineChart", () => ({
  __esModule: true,
  default: ({ podName }: { podName?: string }) => (
    <div data-testid="line-chart">{podName || "vertex-wide"}</div>
  ),
}));

const SearchProbe = () => {
  const location = useLocation();
  return <div data-testid="location-search">{location.search}</div>;
};

const discoveredMetrics = {
  data: [
    {
      metric_name: "rate_a",
      display_name: "Rate A",
      metric_description: "Rate A",
    },
    {
      metric_name: "rate_b",
      display_name: "Rate B",
      metric_description: "Rate B",
    },
  ],
};

const MetricsHarness = ({
  search,
  podName,
  initialExpanded,
}: {
  search: string;
  podName?: string;
  initialExpanded?: string[];
}) => {
  const [expanded, setExpanded] = useState(
    new Set<string>(initialExpanded || [])
  );
  return (
    <MemoryRouter initialEntries={[`/${search}`]}>
      <VertexDetailsContext.Provider
        value={{
          openMetrics: jest.fn(),
          expanded,
          setExpanded,
          presets: undefined,
          setPresets: jest.fn(),
        }}
      >
        <SearchProbe />
        <Metrics
          namespaceId="default"
          pipelineId="demo"
          type="udf"
          vertexId="cat"
          podName={podName}
        />
      </VertexDetailsContext.Provider>
    </MemoryRouter>
  );
};

const renderMetrics = (
  search: string,
  options?: { podName?: string; initialExpanded?: string[] }
) => render(<MetricsHarness search={search} {...options} />);

describe("Metrics", () => {
  const writeText = jest.fn();

  beforeEach(() => {
    writeText.mockReset();
    writeText.mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: { writeText },
    });
    mockUseMetricsDiscoveryDataFetch.mockReturnValue({
      metricsDiscoveryData: discoveredMetrics,
      error: undefined,
      loading: false,
    });
  });

  it("restores multiple panels from the URL", async () => {
    renderMetrics(
      "?vertex=cat&vertexTab=metrics&metricPanels=rate_a-panel,rate_b-panel"
    );

    await waitFor(() => {
      expect(screen.getAllByTestId("line-chart")).toHaveLength(2);
    });
  });

  it("keeps sibling panels in the URL when another panel is expanded", async () => {
    renderMetrics("?vertex=cat&vertexTab=metrics&metricPanels=rate_a-panel");

    await waitFor(() => {
      expect(screen.getByText("Rate A")).toBeInTheDocument();
    });
    fireEvent.click(screen.getByText("Rate B"));
    await waitFor(() => {
      const search = screen.getByTestId("location-search").textContent || "";
      expect(new URLSearchParams(search).get("metricPanels")).toBe(
        "rate_a-panel,rate_b-panel"
      );
    });
  });

  it("renders one copy icon per visible metric row", async () => {
    renderMetrics("?vertex=cat&vertexTab=metrics");

    await waitFor(() => {
      expect(screen.getByTestId("copy-metric-view-rate_a")).toBeInTheDocument();
      expect(screen.getByTestId("copy-metric-view-rate_b")).toBeInTheDocument();
    });
    expect(screen.queryByTestId("copy-view-link")).not.toBeInTheDocument();
  });

  it("copies only the clicked metric without changing the current URL or expanding the panel", async () => {
    renderMetrics(
      "?vertex=cat&vertexTab=metrics&metric=rate_a&metricPanels=rate_a-panel&metricDuration=5m&metricDimension=pod&metricFilter=pod:cat-0"
    );

    await waitFor(() => {
      expect(screen.getByTestId("copy-metric-view-rate_b")).toBeInTheDocument();
    });
    fireEvent.click(screen.getByTestId("copy-metric-view-rate_b"));

    await waitFor(() => {
      const copied = writeText.mock.calls[0][0] as string;
      const copiedParams = new URLSearchParams(copied.split("?")[1] || "");
      expect(copiedParams.get("vertexTab")).toBe("metrics");
      expect(copiedParams.get("metric")).toBe("rate_b");
      expect(copiedParams.get("metricPanels")).toBe("rate_b-panel");
      expect(copiedParams.get("metricDuration")).toBe("5m");
      expect(copiedParams.get("metricDimension")).toBe("pod");
      expect(copiedParams.get("metricFilter")).toBe("pod:cat-0");
      expect(screen.getByTestId("location-search")).toHaveTextContent(
        "metric=rate_a"
      );
      expect(screen.getByTestId("location-search")).toHaveTextContent(
        "metricDuration=5m"
      );
      expect(screen.getAllByTestId("line-chart")).toHaveLength(1);
      expect(screen.getByTestId("copy-metric-view-rate_b")).toHaveAttribute(
        "aria-label",
        "Copy Rate B view copied"
      );
      expect(screen.getByTestId("copy-metric-view-rate_a")).toHaveAttribute(
        "aria-label",
        "Copy Rate A view"
      );
    });
  });

  it("preserves the active metric controls when copying that same row", async () => {
    renderMetrics(
      "?vertex=cat&vertexTab=metrics&metric=rate_a&metricPanels=rate_a-panel&metricDuration=5m&metricFilter=pod:cat-0"
    );

    await waitFor(() => {
      expect(screen.getByTestId("copy-metric-view-rate_a")).toBeInTheDocument();
    });
    fireEvent.click(screen.getByTestId("copy-metric-view-rate_a"));

    await waitFor(() => {
      const copied = writeText.mock.calls[0][0] as string;
      const copiedParams = new URLSearchParams(copied.split("?")[1] || "");
      expect(copiedParams.get("metric")).toBe("rate_a");
      expect(copiedParams.get("metricPanels")).toBe("rate_a-panel");
      expect(copiedParams.get("metricDuration")).toBe("5m");
      expect(copiedParams.get("metricFilter")).toBe("pod:cat-0");
    });
  });

  it("passes a restored pod name into the chart", async () => {
    renderMetrics("?vertex=cat&vertexTab=metrics&pod=cat-0", {
      podName: "cat-0",
      initialExpanded: ["rate_a-panel"],
    });

    expect(await screen.findByTestId("line-chart")).toHaveTextContent("cat-0");
  });
});
