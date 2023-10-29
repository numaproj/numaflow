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

import "@testing-library/jest-dom";

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
    Buffers: () => <div>Mocked buffers</div>,
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
