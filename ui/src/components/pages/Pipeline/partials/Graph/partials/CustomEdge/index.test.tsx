import React from "react";
import { render, screen, waitFor } from "@testing-library/react";
import "@testing-library/jest-dom";
import { Position, ReactFlowProvider } from "@xyflow/react";
import CustomEdge from "./index";
import { HighlightContext } from "../..";

describe("Custom Edge", () => {
  it("should render", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {},
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
          setHidden: jest.fn(),
        }}
      >
        <ReactFlowProvider>
          <svg>
            <CustomEdge
              id="test"
              sourceX={0}
              sourceY={0}
              targetX={0}
              targetY={0}
              sourcePosition={Position.Top}
              targetPosition={Position.Bottom}
              data={{
                fwdEdge: true,
                backEdge: false,
                fromNodeOutDegree: 1,
                backEdgeHeight: 1,
              }}
              markerEnd="test"
              source={"first"}
              target={"second"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("test")).toBeInTheDocument();
    });
  });

  it("Fwd edge not full", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {},
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
          setHidden: jest.fn(),
        }}
      >
        <ReactFlowProvider>
          <svg>
            <CustomEdge
              id={"first-second"}
              sourceX={240}
              sourceY={36}
              targetX={334}
              targetY={36}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                backpressureLabel: "0",
                edgeWatermark: {
                  isWaterMarkEnabled: true,
                },
                fwdEdge: true,
                fromNodeOutDegree: 1,
              }}
              source={"first"}
              target={"second"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-second`)).toBeInTheDocument()
    );
  });

  it("Fwd edge but full with delay in mo", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {},
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
          setHidden: jest.fn(),
        }}
      >
        <ReactFlowProvider>
          <svg>
            <CustomEdge
              id={"first-second"}
              sourceX={240}
              sourceY={36}
              targetX={334}
              targetY={36}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                isFull: "true",
                backpressureLabel: "0",
                edgeWatermark: {
                  isWaterMarkEnabled: true,
                  watermarks: [Date.now() - 2678400000],
                },
                fwdEdge: true,
              }}
              source={"first"}
              target={"second"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-second`)).toBeInTheDocument()
    );
  });

  it("Edge branches with delays in ms, sec, min, hr, d", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {},
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
          setHidden: jest.fn(),
        }}
      >
        <ReactFlowProvider>
          <svg>
            <CustomEdge
              id={"first-second"}
              sourceX={240}
              sourceY={36}
              targetX={334}
              targetY={40}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                isFull: "false",
                backpressureLabel: "0",
                edgeWatermark: {
                  isWaterMarkEnabled: true,
                  watermarks: [Date.now() - 1],
                },
                fwdEdge: true,
              }}
              source={"first"}
              target={"second"}
            />
            <CustomEdge
              id={"first-third"}
              sourceX={240}
              sourceY={36}
              targetX={334}
              targetY={38}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                isFull: "false",
                backpressureLabel: "0",
                edgeWatermark: {
                  isWaterMarkEnabled: true,
                  watermarks: [Date.now() - 1000],
                },
                fwdEdge: true,
              }}
              source={"first"}
              target={"third"}
            />
            <CustomEdge
              id={"first-fourth"}
              sourceX={240}
              sourceY={36}
              targetX={334}
              targetY={36}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                isFull: "false",
                backpressureLabel: "0",
                edgeWatermark: {
                  isWaterMarkEnabled: true,
                  watermarks: [Date.now() - 60000],
                },
                fwdEdge: true,
              }}
              source={"first"}
              target={"fourth"}
            />
            <CustomEdge
              id={"first-fifth"}
              sourceX={240}
              sourceY={36}
              targetX={334}
              targetY={34}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                isFull: "false",
                backpressureLabel: "0",
                edgeWatermark: {
                  isWaterMarkEnabled: true,
                  watermarks: [Date.now() - 3600000],
                },
                fwdEdge: true,
              }}
              source={"first"}
              target={"fifth"}
            />
            <CustomEdge
              id={"first-sixth"}
              sourceX={240}
              sourceY={36}
              targetX={334}
              targetY={32}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                isFull: "false",
                backpressureLabel: "0",
                edgeWatermark: {
                  isWaterMarkEnabled: true,
                  watermarks: [Date.now() - 86400000],
                },
                fwdEdge: true,
              }}
              source={"first"}
              target={"sixth"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-second`)).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-third`)).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-fourth`)).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-fifth`)).toBeInTheDocument()
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-sixth`)).toBeInTheDocument()
    );
  });
  it("Forward edges with out-degree > 1", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {},
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
          setHidden: jest.fn(),
        }}
      >
        <ReactFlowProvider>
          <svg>
            <CustomEdge
              id={"first-second"}
              sourceX={240}
              sourceY={36}
              targetX={334}
              targetY={40}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                isFull: "true",
                backpressureLabel: "0",
                edgeWatermark: {
                  isWaterMarkEnabled: true,
                  watermarks: [Date.now() - 2678400000],
                },
                fwdEdge: true,
                fromNodeOutDegree: 2,
              }}
              source={"first"}
              target={"second"}
            />
            <CustomEdge
              id={"first-third"}
              sourceX={240}
              sourceY={36}
              targetX={334}
              targetY={32}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                isFull: "true",
                backpressureLabel: "0",
                edgeWatermark: {
                  isWaterMarkEnabled: true,
                  watermarks: [Date.now() - 2678400000],
                },
                fwdEdge: true,
                fromNodeOutDegree: 2,
              }}
              source={"first"}
              target={"third"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId(`first-second`)).toBeInTheDocument();
      expect(screen.getByTestId(`first-third`)).toBeInTheDocument();
    });
  });
  it("Back edges", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {},
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
          setHidden: jest.fn(),
        }}
      >
        <ReactFlowProvider>
          <svg>
            <CustomEdge
              id={"first-second"}
              sourceX={334}
              sourceY={36}
              targetX={240}
              targetY={36}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                isFull: "true",
                backpressureLabel: "0",
                edgeWatermark: {
                  isWaterMarkEnabled: true,
                  watermarks: [Date.now() - 2678400000],
                },
                backEdge: true,
              }}
              source={"first"}
              target={"second"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-second`)).toBeInTheDocument()
    );
  });
  it("Self edges", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {},
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
          setHidden: jest.fn(),
        }}
      >
        <ReactFlowProvider>
          <svg>
            <CustomEdge
              id={"first-first"}
              sourceX={334}
              sourceY={36}
              targetX={300}
              targetY={36}
              sourcePosition={Position.Top}
              targetPosition={Position.Top}
              data={{
                isFull: "true",
                backpressureLabel: "0",
                edgeWatermark: {
                  isWaterMarkEnabled: true,
                  watermarks: [Date.now() - 2678400000],
                },
                selfEdge: true,
              }}
              source={"first"}
              target={"second"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() =>
      expect(screen.getByTestId(`first-first`)).toBeInTheDocument()
    );
  });
});
