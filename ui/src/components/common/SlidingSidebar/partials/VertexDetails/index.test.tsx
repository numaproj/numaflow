import React from "react";
import {
  render,
  screen,
  waitFor,
  fireEvent,
  act,
} from "@testing-library/react";
import { VertexDetails } from "./index";
import { BrowserRouter } from "react-router-dom";
import { AppContext } from "../../../../../App";

import "@testing-library/jest-dom";

const mockBuffersComponent = jest.fn();
const mockMetricsComponent = jest.fn();

jest.mock("./partials/VertexUpdate", () => {
  const originalModule = jest.requireActual("./partials/VertexUpdate");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    VertexUpdate: ({ setModalOnClose }) => (
      <div>
        Mocked vertexupdate
        <button
          data-testid="set-update-modal-open"
          onClick={() => setModalOnClose({})}
        />
      </div>
    ),
  };
});
jest.mock("./partials/ProcessingRates", () => {
  const originalModule = jest.requireActual("./partials/ProcessingRates");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    ProcessingRates: () => <div>Mocked processingrates</div>,
  };
});
jest.mock("../K8sEvents", () => {
  const originalModule = jest.requireActual("../K8sEvents");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    K8sEvents: () => <div>Mocked k8sevents</div>,
  };
});
jest.mock("./partials/Buffers", () => {
  const originalModule = jest.requireActual("./partials/Buffers");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    Buffers: (props) => {
      mockBuffersComponent(props);
      return <div>Mocked buffers</div>;
    },
  };
});
jest.mock(
  "../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods",
  () => {
    const originalModule = jest.requireActual(
      "../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods"
    );
    // Mock any module exports here
    return {
      __esModule: true,
      ...originalModule,
      // Named export mocks
      Pods: () => <div>Mocked pods</div>,
    };
  }
);
jest.mock(
  "../../../../pages/Pipeline/partials/Graph/partials/NodeInfo/partials/Pods/partials/PodDetails/partials/Metrics",
  () => {
    const ReactModule = jest.requireActual("react");
    return {
      Metrics: (props) => {
        const { VertexDetailsContext } = jest.requireActual("./index");
        const context = ReactModule.useContext(VertexDetailsContext);
        mockMetricsComponent(props, context);
        return (
          <div>
            <div>Mocked metrics</div>
            <div data-testid="metrics-pod">
              {props.pod?.name || "vertex-wide"}
            </div>
            <div data-testid="metrics-expanded">
              {context.expanded.has("test-panel").toString()}
            </div>
            <button
              data-testid="redirect-to-metrics"
              onClick={() =>
                context.openMetrics({
                  panelId: "test-panel",
                  pod: { name: "test-pod" },
                })
              }
            />
          </div>
        );
      },
    };
  }
);

describe("VertexDetails", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("SOURCE vertex", async () => {
    render(
      <VertexDetails
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpecs={{}}
        vertexMetrics={{}}
        buffers={[]}
        type="source"
        setModalOnClose={jest.fn()}
        refresh={jest.fn()}
      />,
      { wrapper: BrowserRouter }
    );

    await waitFor(() => {
      expect(screen.getByText("Input Vertex")).toBeInTheDocument();
      expect(screen.getByText("Mocked pods")).toBeInTheDocument();
    });
  });

  it("REDUCE vertex", async () => {
    render(
      <VertexDetails
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpecs={{ udf: { groupBy: {} } }}
        vertexMetrics={{}}
        buffers={[]}
        type="udf"
        setModalOnClose={jest.fn()}
        refresh={jest.fn()}
      />,
      { wrapper: BrowserRouter }
    );

    await waitFor(() => {
      expect(screen.getByText("Processor Vertex")).toBeInTheDocument();
      expect(screen.getByText("Mocked pods")).toBeInTheDocument();
    });
  });

  it("MAP vertex", async () => {
    render(
      <VertexDetails
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpecs={{}}
        vertexMetrics={{}}
        buffers={[]}
        type="udf"
        setModalOnClose={jest.fn()}
        refresh={jest.fn()}
      />,
      { wrapper: BrowserRouter }
    );

    await waitFor(() => {
      expect(screen.getByText("Processor Vertex")).toBeInTheDocument();
      expect(screen.getByText("Mocked pods")).toBeInTheDocument();
    });
  });

  it("SINK vertex", async () => {
    render(
      <VertexDetails
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpecs={{}}
        vertexMetrics={{}}
        buffers={[]}
        type="sink"
        setModalOnClose={jest.fn()}
        refresh={jest.fn()}
      />,
      { wrapper: BrowserRouter }
    );

    await waitFor(() => {
      expect(screen.getByText("Sink Vertex")).toBeInTheDocument();
      expect(screen.getByText("Mocked pods")).toBeInTheDocument();
    });
  });

  it("renders Metrics immediately after Pods View", async () => {
    render(
      <AppContext.Provider
        value={{ addError: jest.fn(), disableMetricsCharts: false } as any}
      >
        <BrowserRouter>
          <VertexDetails
            namespaceId="test-namespace"
            pipelineId="test-pipeline"
            vertexId="test-vertex"
            vertexSpecs={{}}
            vertexMetrics={{}}
            buffers={[]}
            type="sink"
            setModalOnClose={jest.fn()}
            refresh={jest.fn()}
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    const tabs = screen.getAllByRole("tab");
    expect(tabs.slice(0, 3).map((tab) => tab.textContent)).toEqual([
      "Pods View",
      "Metrics",
      "Spec",
    ]);

    fireEvent.click(screen.getByTestId("metrics-tab"));
    await waitFor(() => {
      expect(screen.getByText("Mocked metrics")).toBeInTheDocument();
    });
  });

  it("preserves redirected pod context and expansion state", async () => {
    render(
      <AppContext.Provider
        value={{ addError: jest.fn(), disableMetricsCharts: false } as any}
      >
        <BrowserRouter>
          <VertexDetails
            namespaceId="test-namespace"
            pipelineId="test-pipeline"
            vertexId="test-vertex"
            vertexSpecs={{}}
            vertexMetrics={{}}
            buffers={[]}
            type="sink"
            setModalOnClose={jest.fn()}
            refresh={jest.fn()}
          />
        </BrowserRouter>
      </AppContext.Provider>
    );

    fireEvent.click(screen.getByTestId("metrics-tab"));
    expect(await screen.findByTestId("metrics-pod")).toHaveTextContent(
      "vertex-wide"
    );

    fireEvent.click(screen.getByTestId("redirect-to-metrics"));
    await waitFor(() => {
      expect(screen.getByTestId("metrics-pod")).toHaveTextContent("test-pod");
      expect(screen.getByTestId("metrics-expanded")).toHaveTextContent("true");
    });

    fireEvent.click(screen.getByTestId("pods-tab"));
    fireEvent.click(screen.getByTestId("metrics-tab"));
    await waitFor(() => {
      expect(screen.getByTestId("metrics-pod")).toHaveTextContent(
        "vertex-wide"
      );
      expect(screen.getByTestId("metrics-expanded")).toHaveTextContent("true");
    });
  });

  it("returns to Pods View when metrics become disabled", async () => {
    const Harness = () => {
      const [disableMetricsCharts, setDisableMetricsCharts] =
        React.useState(false);
      return (
        <AppContext.Provider
          value={{ addError: jest.fn(), disableMetricsCharts } as any}
        >
          <button
            data-testid="disable-metrics"
            onClick={() => setDisableMetricsCharts(true)}
          />
          <BrowserRouter>
            <VertexDetails
              namespaceId="test-namespace"
              pipelineId="test-pipeline"
              vertexId="test-vertex"
              vertexSpecs={{}}
              vertexMetrics={{}}
              buffers={[]}
              type="sink"
              setModalOnClose={jest.fn()}
              refresh={jest.fn()}
            />
          </BrowserRouter>
        </AppContext.Provider>
      );
    };

    render(<Harness />);
    fireEvent.click(screen.getByTestId("metrics-tab"));
    expect(await screen.findByText("Mocked metrics")).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("disable-metrics"));
    await waitFor(() => {
      expect(screen.queryByTestId("metrics-tab")).not.toBeInTheDocument();
      expect(screen.getByText("Mocked pods")).toBeInTheDocument();
    });
  });

  it("Click through tabs", async () => {
    render(
      <VertexDetails
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpecs={{}}
        vertexMetrics={{}}
        buffers={[]}
        type="sink"
        setModalOnClose={jest.fn()}
        refresh={jest.fn()}
      />,
      { wrapper: BrowserRouter }
    );
    await waitFor(() => {
      expect(screen.getByText("Sink Vertex")).toBeInTheDocument();
      expect(screen.getByText("Mocked pods")).toBeInTheDocument();
    });
    act(() => {
      const tab = screen.getByTestId("spec-tab");
      fireEvent.click(tab);
    });
    await waitFor(() => {
      expect(screen.getByText("Mocked vertexupdate")).toBeInTheDocument();
    });
    act(() => {
      const tab = screen.getByTestId("pr-tab");
      fireEvent.click(tab);
    });
    await waitFor(() => {
      expect(screen.getByText("Mocked processingrates")).toBeInTheDocument();
    });
    act(() => {
      const tab = screen.getByTestId("events-tab");
      fireEvent.click(tab);
    });
    await waitFor(() => {
      expect(screen.getByText("Mocked k8sevents")).toBeInTheDocument();
    });
    act(() => {
      const tab = screen.getByTestId("buffers-tab");
      fireEvent.click(tab);
    });
    await waitFor(() => {
      expect(screen.getByText("Mocked buffers")).toBeInTheDocument();
      expect(screen.queryByTestId("isb-tab")).not.toBeInTheDocument();
    });
  });

  it("renders source ISB details under Buffers when buffer rows are absent", async () => {
    render(
      <VertexDetails
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpecs={{}}
        vertexMetrics={{}}
        buffers={null}
        type="source"
        setModalOnClose={jest.fn()}
        refresh={jest.fn()}
      />,
      { wrapper: BrowserRouter }
    );

    await waitFor(() => {
      expect(screen.getByText("Input Vertex")).toBeInTheDocument();
      expect(screen.getByTestId("buffers-tab")).toBeInTheDocument();
      expect(screen.queryByTestId("isb-tab")).not.toBeInTheDocument();
    });

    act(() => {
      const tab = screen.getByTestId("buffers-tab");
      fireEvent.click(tab);
    });

    await waitFor(() => {
      expect(screen.getByText("Mocked buffers")).toBeInTheDocument();
      expect(mockBuffersComponent).toHaveBeenCalledWith(
        expect.objectContaining({
          buffers: [],
          namespaceId: "test-namespace",
          pipelineId: "test-pipeline",
          vertexId: "test-vertex",
          type: "source",
        })
      );
    });
  });

  it("Update modal opens", async () => {
    render(
      <VertexDetails
        namespaceId="test-namespace"
        pipelineId="test-pipeline"
        vertexId="test-vertex"
        vertexSpecs={{}}
        vertexMetrics={{}}
        buffers={[]}
        type="sink"
        setModalOnClose={jest.fn()}
        refresh={jest.fn()}
      />,
      { wrapper: BrowserRouter }
    );
    await waitFor(() => {
      expect(screen.getByText("Sink Vertex")).toBeInTheDocument();
      expect(screen.getByText("Mocked pods")).toBeInTheDocument();
    });
    // go to spec tab
    act(() => {
      const tab = screen.getByTestId("spec-tab");
      fireEvent.click(tab);
    });
    await waitFor(() => {
      expect(screen.getByText("Mocked vertexupdate")).toBeInTheDocument();
    });
    // set update modal open on navigate away
    act(() => {
      const btn = screen.getByTestId("set-update-modal-open");
      fireEvent.click(btn);
    });
    await waitFor(() => {
      expect(screen.getByText("Mocked vertexupdate")).toBeInTheDocument();
    });
    // change tab
    act(() => {
      const tab = screen.getByTestId("pr-tab");
      fireEvent.click(tab);
    });
    // Check modal opened
    await waitFor(() => {
      expect(screen.getByTestId("close-modal-cancel")).toBeInTheDocument();
    });
    // Click cancel
    act(() => {
      const btn = screen.getByTestId("close-modal-cancel");
      fireEvent.click(btn);
    });
    // change tab
    act(() => {
      const tab = screen.getByTestId("pr-tab");
      fireEvent.click(tab);
    });
    // Check modal opened
    await waitFor(() => {
      expect(screen.getByTestId("close-modal-cancel")).toBeInTheDocument();
    });
    // Click confirm
    act(() => {
      const btn = screen.getByTestId("close-modal-confirm");
      fireEvent.click(btn);
    });
    // Check tab changed to intended tab
    await waitFor(() => {
      expect(screen.getByText("Mocked processingrates")).toBeInTheDocument();
    });
  });
});
