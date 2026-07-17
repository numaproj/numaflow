import React from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { MetricsModal } from "./index";
import { VertexDetailsContext } from "../../../SlidingSidebar/partials/VertexDetails";

import "@testing-library/jest-dom";

jest.mock(
  "../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodDetails/partials/Metrics",
  () => {
    const ReactModule = jest.requireActual("react");
    return {
      Metrics: ({ setMetricsFound }) => {
        ReactModule.useEffect(() => {
          setMetricsFound?.(true);
        }, [setMetricsFound]);
        return <div>Mocked metric chart</div>;
      },
    };
  }
);

describe("MetricsModal", () => {
  it("redirects with the metric panel, presets, and pod context", async () => {
    const openMetrics = jest.fn();
    const handleCloseModal = jest.fn();
    const presets = { duration: "5m" };
    const pod = { name: "test-pod", containers: ["udf"] } as any;

    render(
      <VertexDetailsContext.Provider
        value={{
          openMetrics,
          expanded: new Set(),
          setExpanded: jest.fn(),
          presets: undefined,
          setPresets: jest.fn(),
        }}
      >
        <MetricsModal
          isModalOpen
          handleCloseModal={handleCloseModal}
          metricDisplayName="Test Metric"
          discoveredMetrics={{
            data: [
              {
                display_name: "Test Metric",
                metric_name: "test_metric",
              },
            ],
          }}
          namespaceId="test-namespace"
          pipelineId="test-pipeline"
          vertexId="test-vertex"
          type="udf"
          presets={presets}
          pod={pod}
        />
      </VertexDetailsContext.Provider>
    );

    fireEvent.click(
      await screen.findByText("Click to see detailed view with additional filters")
    );

    expect(handleCloseModal).toHaveBeenCalledTimes(1);
    expect(openMetrics).toHaveBeenCalledWith({
      panelId: "test_metric-panel",
      presets,
      pod,
    });
  });
});
