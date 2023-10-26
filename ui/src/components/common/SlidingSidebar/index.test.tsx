import React from "react";
import { render, screen, waitFor, fireEvent } from "@testing-library/react";
import { SidebarType, SlidingSidebar } from "./index";
import { AppContext } from "../../../App";
import { AppContextProps } from "../../../types/declarations/app";

import "@testing-library/jest-dom";

const mockSetSidebarProps = jest.fn();

const mockContext: AppContextProps = {
  setSidebarProps: mockSetSidebarProps,
  systemInfo: {
    managedNamespace: "numaflow-system",
    namespaced: false,
  },
  systemInfoError: null,
  errors: [],
  addError: jest.fn(),
  clearErrors: jest.fn(),
  setUserInfo: jest.fn(),
};

// Mock K8sEvents
jest.mock("./partials/K8sEvents", () => {
  const originalModule = jest.requireActual("./partials/K8sEvents");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    K8sEvents: () => <div data-testid="k8s-mock">Mocked</div>,
  };
});
// Mock vertex details
jest.mock("./partials/VertexDetails", () => {
  const originalModule = jest.requireActual("./partials/VertexDetails");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    VertexDetails: () => <div data-testid="vertex-details-mock">Mocked</div>,
  };
});
// Mock edge details
jest.mock("./partials/EdgeDetails", () => {
  const originalModule = jest.requireActual("./partials/EdgeDetails");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    EdgeDetails: () => <div data-testid="edge-details-mock">Mocked</div>,
  };
});
// Mock GeneratorDetails
jest.mock("./partials/GeneratorDetails", () => {
  const originalModule = jest.requireActual("./partials/GeneratorDetails");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    GeneratorDetails: () => (
      <div data-testid="generator-details-mock">Mocked</div>
    ),
  };
});
// Mock Errors
jest.mock("./partials/Errors", () => {
  const originalModule = jest.requireActual("./partials/Errors");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    Errors: () => <div data-testid="errors-mock">Mocked</div>,
  };
});
// Mock PipelineCreate
jest.mock("./partials/PipelineCreate", () => {
  const originalModule = jest.requireActual("./partials/PipelineCreate");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    PipelineCreate: () => <div data-testid="pipeline-create-mock">Mocked</div>,
  };
});
// Mock PipelineUpdate
jest.mock("./partials/PipelineUpdate", () => {
  const originalModule = jest.requireActual("./partials/PipelineUpdate");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    PipelineUpdate: () => <div data-testid="pipeline-update-mock">Mocked</div>,
  };
});
// Mock ISBCreate
jest.mock("./partials/ISBCreate", () => {
  const originalModule = jest.requireActual("./partials/ISBCreate");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    ISBCreate: () => <div data-testid="isb-create-mock">Mocked</div>,
  };
});
// Mock ISBUpdate
jest.mock("./partials/ISBUpdate", () => {
  const originalModule = jest.requireActual("./partials/ISBUpdate");
  // Mock any module exports here
  return {
    __esModule: true,
    ...originalModule,
    // Named export mocks
    ISBUpdate: () => <div data-testid="isb-update-mock">Mocked</div>,
  };
});

describe("SlidingSidebar", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should render missing props", async () => {
    render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar pageWidth={2000} slide={true} type={"unknown"} />
      </AppContext.Provider>
    );

    await waitFor(() => {
      expect(screen.getByText("Missing Props")).toBeInTheDocument();
    });
  });

  it("should render K8s events", async () => {
    const { rerender } = render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.PIPELINE_K8s}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByText("Missing Props")).toBeInTheDocument();
    });

    rerender(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.PIPELINE_K8s}
          k8sEventsProps={{
            namespaceId: "test-namespace",
          }}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("k8s-mock")).toBeInTheDocument();
    });

    rerender(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.NAMESPACE_K8s}
          k8sEventsProps={{
            namespaceId: "test-namespace",
          }}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("k8s-mock")).toBeInTheDocument();
    });
  });

  it("should render PipelineCreate", async () => {
    const { rerender } = render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.PIPELINE_CREATE}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByText("Missing Props")).toBeInTheDocument();
    });

    rerender(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.PIPELINE_CREATE}
          specEditorProps={{
            namespaceId: "test-namespace",
          }}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("pipeline-create-mock")).toBeInTheDocument();
    });
  });

  it("should render PipelineUpdate", async () => {
    const { rerender } = render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.PIPELINE_UPDATE}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByText("Missing Props")).toBeInTheDocument();
    });

    rerender(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.PIPELINE_UPDATE}
          specEditorProps={{
            namespaceId: "test-namespace",
            pipelineId: "test-pipeline",
          }}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("pipeline-update-mock")).toBeInTheDocument();
    });
  });

  it("should render ISBCreate", async () => {
    const { rerender } = render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.ISB_CREATE}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByText("Missing Props")).toBeInTheDocument();
    });

    rerender(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.ISB_CREATE}
          specEditorProps={{
            namespaceId: "test-namespace",
          }}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("isb-create-mock")).toBeInTheDocument();
    });
  });

  it("should render ISBUpdate", async () => {
    const { rerender } = render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.ISB_UPDATE}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByText("Missing Props")).toBeInTheDocument();
    });

    rerender(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.ISB_UPDATE}
          specEditorProps={{
            namespaceId: "test-namespace",
            isbId: "test-isb",
          }}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("isb-update-mock")).toBeInTheDocument();
    });
  });

  it("should render VertexDetails", async () => {
    const { rerender } = render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.VERTEX_DETAILS}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByText("Missing Props")).toBeInTheDocument();
    });

    rerender(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.VERTEX_DETAILS}
          vertexDetailsProps={{
            namespaceId: "test-namespace",
            pipelineId: "test-pipeline",
            vertexId: "test-vertex",
            vertexSpecs: {},
            vertexMetrics: {},
            buffers: [],
            type: "source",
            refresh: jest.fn(),
            setModalOnClose: jest.fn(),
          }}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("vertex-details-mock")).toBeInTheDocument();
    });
  });

  it("should render EdgeDetails", async () => {
    const { rerender } = render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.EDGE_DETAILS}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByText("Missing Props")).toBeInTheDocument();
    });

    rerender(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.EDGE_DETAILS}
          edgeDetailsProps={{
            edgeId: "test-edge",
            watermarks: [],
          }}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("edge-details-mock")).toBeInTheDocument();
    });
  });

  it("should render GeneratorDetails", async () => {
    const { rerender } = render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.GENERATOR_DETAILS}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByText("Missing Props")).toBeInTheDocument();
    });

    rerender(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.GENERATOR_DETAILS}
          generatorDetailsProps={{
            vertexId: "test-vertex",
            generatorDetails: {},
          }}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("generator-details-mock")).toBeInTheDocument();
    });
  });

  it("should render Errors", async () => {
    render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.ERRORS}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("errors-mock")).toBeInTheDocument();
    });
  });

  it("parent close indicator change, should call setSidebarProps. Also pagewidth change.", async () => {
    const { rerender } = render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.ERRORS}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("errors-mock")).toBeInTheDocument();
    });
    rerender(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={100}
          slide={true}
          type={SidebarType.ERRORS}
          parentCloseIndicator="test"
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      expect(mockSetSidebarProps).toHaveBeenCalledWith(undefined);
    });
  });

  it("Drag slider", async () => {
    render(
      <AppContext.Provider value={mockContext}>
        <SlidingSidebar
          pageWidth={2000}
          slide={true}
          type={SidebarType.ISB_CREATE}
          specEditorProps={{
            namespaceId: "test-namespace",
          }}
        />
      </AppContext.Provider>
    );
    await waitFor(() => {
      const mouse = [{ pageX: 0 }, { pageX: -50 }];
      const dragIcon = screen.getByTestId("sidebar-drag-icon");
      fireEvent.mouseDown(dragIcon, mouse[0]);
      fireEvent.mouseMove(dragIcon, mouse[1]);
      fireEvent.mouseUp(dragIcon);
    });
  });
});
