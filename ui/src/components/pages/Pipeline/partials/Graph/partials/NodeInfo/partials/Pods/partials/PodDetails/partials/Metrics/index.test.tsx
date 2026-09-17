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
  beforeEach(() => {
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

  it("does not render a duplicate list-level copy action", async () => {
    renderMetrics("?vertex=cat&vertexTab=metrics");

    await waitFor(() => {
      expect(screen.getByText("Rate A")).toBeInTheDocument();
    });
    expect(screen.queryByTestId("copy-view-link")).not.toBeInTheDocument();
  });

  it("passes a restored pod name into the chart", async () => {
    renderMetrics("?vertex=cat&vertexTab=metrics&pod=cat-0", {
      podName: "cat-0",
      initialExpanded: ["rate_a-panel"],
    });

    expect(await screen.findByTestId("line-chart")).toHaveTextContent("cat-0");
  });
});
