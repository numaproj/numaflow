import React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import "@testing-library/jest-dom";
import fetchMock from "jest-fetch-mock";

import Graph from "./index";

import { AppContext } from "../../../../../App";
import { AppContextProps } from "../../../../../types/declarations/app";
import { GraphData } from "../../../../../types/declarations/pipeline";
import { Position } from "@xyflow/react";

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

const mockSetSidebarProps = jest.fn();
const mockSetUserInfo = jest.fn();

const mockContext: AppContextProps = {
  setSidebarProps: mockSetSidebarProps,
  systemInfo: {
    managedNamespace: "numaflow-system",
    namespaced: false,
  },
  systemInfoError: null,
  addError: function (error: string): void {
    console.log(error);
  },
  clearErrors: function (): void {
    throw new Error("Function not implemented.");
  },
  setUserInfo: mockSetUserInfo,
};
class DOMMatrixReadOnly {
  m22: number;
  constructor(transform: string) {
    const scale = transform?.match(/scale\(([1-9.])\)/)?.[1];
    this.m22 = scale !== undefined ? +scale : 1;
  }
}

let init = false;

export const mockReactFlow = () => {
  if (init) return;
  init = true;
  global.ResizeObserver = ResizeObserver;
  global.DOMMatrixReadOnly = DOMMatrixReadOnly;
  Object.defineProperties(global.HTMLElement.prototype, {
    offsetHeight: {
      get() {
        return parseFloat(this.style.height) || 1;
      },
    },
    offsetWidth: {
      get() {
        return parseFloat(this.style.width) || 1;
      },
    },
  });
  (global.SVGElement as any).prototype.getBBox = () => ({
    x: 0,
    y: 0,
    width: 0,
    height: 0,
  });
};

const mockData: GraphData = {
  edges: [
    {
      id: "in-cat",
      source: "in",
      target: "cat",
      data: {
        conditions: null,
        backpressureLabel: 0,
        isFull: false,
        source: "in",
        target: "cat",
        fwdEdge: true,
        backEdge: false,
        selfEdge: false,
        backEdgeHeight: 0,
        fromNodeOutDegree: 1,
        edgeWatermark: {
          isWaterMarkEnabled: true,
          watermarks: [1699517989897],
          WMFetchTime: 1699517991711,
        },
      },
      animated: true,
      type: "custom",
      sourceHandle: "0",
      targetHandle: "0",
    },
    {
      id: "cat-out",
      source: "cat",
      target: "out",
      data: {
        conditions: null,
        backpressureLabel: 0,
        isFull: false,
        source: "cat",
        target: "out",
        fwdEdge: true,
        backEdge: false,
        selfEdge: false,
        backEdgeHeight: 0,
        fromNodeOutDegree: 1,
        edgeWatermark: {
          isWaterMarkEnabled: true,
          watermarks: [1699517988896],
          WMFetchTime: 1699517991711,
        },
      },
      animated: true,
      type: "custom",
      sourceHandle: "0",
      targetHandle: "0",
    },
    {
      id: "myticker-cat",
      source: "myticker",
      target: "cat",
      data: {
        source: "myticker",
        target: "cat",
        sideInputEdge: true,
      },
      animated: true,
      type: "custom",
      sourceHandle: "2",
      targetHandle: "3-0",
      hidden: true,
    },
  ],
  vertices: [
    {
      id: "in",
      data: {
        name: "in",
        podnum: 0,
        nodeInfo: {
          name: "in",
          source: {
            generator: {
              rpu: 5,
              duration: "1s",
              msgSize: 8,
            },
          },
          scale: {},
        },
        type: "source",
        vertexMetrics: null,
        buffers: null,
        centerSourceHandle: false,
        centerTargetHandle: false,
        quadHandle: false,
      },
      position: {
        x: 0,
        y: 0,
      },
      draggable: false,
      type: "custom",
      targetPosition: Position.Left,
      sourcePosition: Position.Right,
    },
    {
      id: "cat",
      data: {
        name: "cat",
        podnum: 0,
        nodeInfo: {
          name: "cat",
          udf: {
            container: null,
            builtin: {
              name: "cat",
            },
            groupBy: null,
          },
          sideInputs: ["myticker"],
          scale: {},
        },
        type: "udf",
        vertexMetrics: null,
        buffers: [
          {
            pipeline: "simple-pipeline",
            bufferName: "numaflow-system-simple-pipeline-cat-0",
            pendingCount: 0,
            ackPendingCount: 0,
            totalMessages: 0,
            bufferLength: 30000,
            bufferUsageLimit: 0.8,
            bufferUsage: 0,
            isFull: false,
          },
        ],
        centerSourceHandle: false,
        centerTargetHandle: false,
        quadHandle: false,
      },
      position: {
        x: 492,
        y: 0,
      },
      draggable: false,
      type: "custom",
      targetPosition: Position.Left,
      sourcePosition: Position.Right,
    },
    {
      id: "out",
      data: {
        name: "out",
        podnum: 0,
        nodeInfo: {
          name: "out",
          sink: {
            log: {},
          },
          scale: {},
        },
        type: "sink",
        test: "out",
        vertexMetrics: null,
        buffers: [
          {
            pipeline: "simple-pipeline",
            bufferName: "numaflow-system-simple-pipeline-out-0",
            pendingCount: 0,
            ackPendingCount: 0,
            totalMessages: 0,
            bufferLength: 30000,
            bufferUsageLimit: 0.8,
            bufferUsage: 0,
            isFull: false,
          },
        ],
        centerSourceHandle: false,
        centerTargetHandle: false,
        quadHandle: false,
      },
      position: {
        x: 984,
        y: 0,
      },
      draggable: false,
      type: "custom",
      targetPosition: Position.Left,
      sourcePosition: Position.Right,
    },
    {
      id: "generator",
      data: {
        sideInputCount: 1,
        type: "generator",
      },
      position: {
        x: 126,
        y: 108,
      },
      draggable: false,
      type: "custom",
      targetPosition: Position.Left,
      sourcePosition: Position.Right,
    },
    {
      id: "myticker",
      data: {
        name: "myticker",
        nodeInfo: {
          name: "myticker",
          container: {
            image: "quay.io/numaio/numaflow-go/sideinput-example:v0.5.0",
            resources: {},
            imagePullPolicy: "Always",
          },
          trigger: {
            schedule: "0 */2 * * * *",
            timezone: null,
          },
        },
        type: "sideInput",
        sideHandle: true,
      },
      position: {
        x: 126,
        y: 162,
      },
      draggable: false,
      type: "custom",
      targetPosition: Position.Left,
      sourcePosition: Position.Right,
    },
  ],
  pipeline: {
    metadata: {
      name: "simple-pipeline",
      namespace: "numaflow-system",
      uid: "87775ef4-fd4b-497e-b40d-7ba47b821a92",
      resourceVersion: "30209",
      generation: 13,
      creationTimestamp: "2023-10-12T14:36:00Z",
      annotations: {
        "kubectl.kubernetes.io/last-applied-configuration":
          '{"apiVersion":"numaflow.numaproj.io/v1alpha1","kind":"Pipeline","metadata":{"annotations":{},"name":"simple-pipeline","namespace":"numaflow-system"},"spec":{"edges":[{"from":"in","to":"cat"},{"from":"cat","to":"out"}],"vertices":[{"name":"in","source":{"generator":{"duration":"1s","rpu":5}}},{"name":"cat","udf":{"builtin":{"name":"cat"}}},{"name":"out","sink":{"log":{}}}]}}\n',
      },
      finalizers: ["pipeline-controller"],
      managedFields: [
        {
          manager: "kubectl-client-side-apply",
          operation: "Update",
          apiVersion: "numaflow.numaproj.io/v1alpha1",
          time: "2023-10-12T14:36:00Z",
          fieldsType: "FieldsV1",
          fieldsV1: {
            "f:metadata": {
              "f:annotations": {
                ".": {},
                "f:kubectl.kubernetes.io/last-applied-configuration": {},
              },
            },
            "f:spec": {
              ".": {},
              "f:edges": {},
              "f:lifecycle": {
                ".": {},
                "f:deleteGracePeriodSeconds": {},
              },
              "f:limits": {
                ".": {},
                "f:bufferMaxLength": {},
                "f:bufferUsageLimit": {},
                "f:readBatchSize": {},
                "f:readTimeout": {},
              },
              "f:watermark": {
                ".": {},
                "f:disabled": {},
                "f:maxDelay": {},
              },
            },
          },
        },
        {
          manager: "numaflow",
          operation: "Update",
          apiVersion: "numaflow.numaproj.io/v1alpha1",
          time: "2023-10-12T16:38:25Z",
          fieldsType: "FieldsV1",
          fieldsV1: {
            "f:metadata": {
              "f:finalizers": {
                ".": {},
                'v:"pipeline-controller"': {},
              },
            },
            "f:spec": {
              "f:lifecycle": {
                "f:desiredPhase": {},
              },
              "f:vertices": {},
            },
          },
        },
        {
          manager: "numaflow",
          operation: "Update",
          apiVersion: "numaflow.numaproj.io/v1alpha1",
          time: "2023-10-13T14:36:41Z",
          fieldsType: "FieldsV1",
          fieldsV1: {
            "f:status": {
              ".": {},
              "f:conditions": {},
              "f:lastUpdated": {},
              "f:phase": {},
              "f:sinkCount": {},
              "f:sourceCount": {},
              "f:udfCount": {},
              "f:vertexCount": {},
            },
          },
          subresource: "status",
        },
      ],
    },
    spec: {
      sideInputs: [
        {
          name: "myticker",
          container: {
            image: "quay.io/numaio/numaflow-go/sideinput-example:v0.5.0",
            resources: {},
            imagePullPolicy: "Always",
          },
          trigger: {
            schedule: "0 */2 * * * *",
            timezone: null,
          },
        },
      ],
      vertices: [
        {
          name: "in",
          source: {
            generator: {
              rpu: 5,
              duration: "1s",
              msgSize: 8,
            },
          },
        },
        {
          name: "cat",
          udf: {
            container: null,
            builtin: {
              name: "cat",
            },
            groupBy: null,
          },
        },
        {
          name: "out",
          sink: {
            log: {},
          },
        },
      ],
      edges: [
        {
          from: "in",
          to: "cat",
          conditions: null,
        },
        {
          from: "cat",
          to: "out",
          conditions: null,
        },
      ],
      watermark: {
        maxDelay: "0s",
      },
    },
    status: {
      conditions: [
        {
          type: "Configured",
          status: "True",
          lastTransitionTime: "2023-10-13T14:36:41Z",
          reason: "Successful",
          message: "Successful",
        },
        {
          type: "Deployed",
          status: "True",
          lastTransitionTime: "2023-10-13T14:36:41Z",
          reason: "Successful",
          message: "Successful",
        },
      ],
      phase: "Paused",
      lastUpdated: "2023-10-13T14:36:41Z",
      vertexCount: 3,
      sourceCount: 1,
      sinkCount: 1,
      udfCount: 1,
    },
  },
};

fetchMock.enableMocks();

describe("Graph", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockReactFlow();
  });

  it("Renders Graph", async () => {
    render(
      <AppContext.Provider value={mockContext}>
        <Graph
          namespaceId="test"
          data={mockData}
          pipelineId="simple-pipeline"
          refresh={() => {
            return;
          }}
        />
      </AppContext.Provider>
    );

    await waitFor(() => {
      expect(screen.getByTestId("graph")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(screen.getByText("cat")).toBeInTheDocument();
    });
    await waitFor(() => {
      fireEvent.mouseOver(
        document.getElementsByClassName("sideInput_handle")[0]
      );
      fireEvent.mouseOut(
        document.getElementsByClassName("sideInput_handle")[0]
      );
    });
  });

  it("should not crash if data is null", () => {
    const mockData: GraphData = {
      edges: [],
      vertices: [],
      pipeline: {
        spec: {
          vertices: [],
          edges: [],
          watermark: {},
        },
        metadata: {
          name: "simple-pipeline",
          namespace: "numaflow-system",
          uid: "87775ef4-fd4b-497e-b40d-7ba47b821a92",
          resourceVersion: "30209",
          generation: 13,
          creationTimestamp: "2023-10-12T14:36:00Z",
        },
      },
    };
    render(
      <AppContext.Provider value={mockContext}>
        <Graph
          namespaceId="test"
          data={mockData}
          pipelineId="simple-pipeline"
          refresh={() => {
            return;
          }}
        />
      </AppContext.Provider>
    );

    // Add assertions relevant to your component
  });

  it("should render a different number of nodes based on the data prop", () => {
    const dataWithTwoNodes = {
      ...mockData,
      vertices: [mockData.vertices[0], mockData.vertices[1]],
    };

    const { rerender } = render(
      <AppContext.Provider value={mockContext}>
        <Graph
          namespaceId="test"
          data={mockData}
          pipelineId="simple-pipeline"
          refresh={function (): void {
            throw new Error("Function not implemented.");
          }}
        />
      </AppContext.Provider>
    );

    // Assuming your nodes have a class of 'node'
    let node1 = screen.getAllByTestId("rf__node-in").length;
    let node2 = screen.getAllByTestId("rf__node-cat").length;
    let node3 = screen.getAllByTestId("rf__node-out").length;
    expect(node1 + node2 + node3).toBe(3);

    rerender(
      <AppContext.Provider value={mockContext}>
        <Graph
          namespaceId="test"
          data={dataWithTwoNodes}
          pipelineId="simple-pipeline"
          refresh={() => {
            return;
          }}
        />
      </AppContext.Provider>
    );

    node1 = screen.getAllByTestId("rf__node-in").length;
    node2 = screen.getAllByTestId("rf__node-cat").length;
    try {
      node3 = screen.getAllByTestId("rf__node-out").length;
    } catch (e) {
      node3 = 0;
    }

    expect(node1 + node2 + node3).toBe(2);
  });

  it("Tests refresh method", async () => {
    const refresh = jest.fn();
    fetchMock.mockResponseOnce(JSON.stringify({ random: "value" }));
    const user = userEvent.setup();
    render(
      <AppContext.Provider value={mockContext}>
        <Graph
          namespaceId="test"
          data={mockData}
          pipelineId="simple-pipeline"
          refresh={refresh}
        />
      </AppContext.Provider>
    );

    await waitFor(() => {
      expect(screen.getByTestId("graph")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(screen.getByText("cat")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(screen.getByTestId("resume")).toBeInTheDocument();
    });
    await user.click(screen.getByTestId("resume"));
    await waitFor(() => {
      expect(screen.getByTestId("pipeline-status")).toBeInTheDocument();
    });
  });

  it("Tests pause method", async () => {
    const refresh = jest.fn();
    fetchMock.mockResponseOnce(JSON.stringify({ key: "value" }));
    const user = userEvent.setup();
    const updatedMockData = { ...mockData };
    updatedMockData.pipeline.status.phase = "Running";
    render(
      <AppContext.Provider value={mockContext}>
        <Graph
          namespaceId="test"
          data={updatedMockData}
          pipelineId="simple-pipeline"
          refresh={refresh}
        />
      </AppContext.Provider>
    );

    await waitFor(() => {
      expect(screen.getByTestId("graph")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(screen.getByText("cat")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(screen.getByTestId("pause")).toBeInTheDocument();
    });
    await user.click(screen.getByTestId("pause"));
    await waitFor(() => {
      expect(screen.getByTestId("pipeline-status")).toBeInTheDocument();
    });
  });

  it("Tests handlePaneClick method", async () => {
    const refresh = jest.fn();
    render(
      <AppContext.Provider value={mockContext}>
        <Graph
          namespaceId="test"
          data={mockData}
          pipelineId="simple-pipeline"
          refresh={refresh}
        />
      </AppContext.Provider>
    );

    await waitFor(() => {
      expect(screen.getByTestId("graph")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(screen.getByTestId("rf__wrapper")).toBeInTheDocument();
    });

    fireEvent.click(document.getElementsByClassName("react-flow__pane")[0]);
  });

  it("Tests handleNodeClick method", async () => {
    const refresh = jest.fn();
    render(
      <AppContext.Provider value={mockContext}>
        <Graph
          namespaceId="test"
          data={mockData}
          pipelineId="simple-pipeline"
          refresh={refresh}
        />
      </AppContext.Provider>
    );

    await waitFor(() => {
      expect(screen.getByTestId("graph")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(screen.getByText("in")).toBeInTheDocument();
    });
    fireEvent.click(screen.getByTestId("rf__node-in"));
  });

  it("Tests sideInput click", async () => {
    const refresh = jest.fn();
    render(
      <AppContext.Provider
        value={{
          ...mockContext,
          sidebarProps: { type: 8 },
        }}
      >
        <Graph
          namespaceId="test"
          data={mockData}
          pipelineId="simple-pipeline"
          refresh={refresh}
        />
      </AppContext.Provider>
    );

    await waitFor(() => {
      expect(screen.getByTestId("graph")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(
        document.getElementsByClassName("sideInput_node_ele")[0]
      ).toBeInTheDocument();
    });
    fireEvent.click(screen.getByTestId("sideInput-myticker"));
  });

  it("Tests interaction toolbar", async () => {
    const refresh = jest.fn();
    render(
      <AppContext.Provider
        value={{
          ...mockContext,
          sidebarProps: { type: 8 },
        }}
      >
        <Graph
          namespaceId="test"
          data={mockData}
          pipelineId="simple-pipeline"
          refresh={refresh}
        />
      </AppContext.Provider>
    );

    await waitFor(() => {
      expect(screen.getByTestId("graph")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(screen.getByTestId("lock")).toBeInTheDocument();
    });
    fireEvent.click(screen.getByTestId("lock"));
    fireEvent.click(screen.getByTestId("panOnScroll"));
    fireEvent.click(screen.getByTestId("fitView"));
    fireEvent.click(screen.getByTestId("zoomIn"));
    fireEvent.click(screen.getByTestId("zoomOut"));
  });
});
