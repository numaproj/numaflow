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
  it("MonoVertex internal edge remains solid and thin", async () => {
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
              id={"mono-vertex-bypass-source-mono-vertex-bypass-udf"}
              sourceX={140}
              sourceY={80}
              targetX={220}
              targetY={80}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                source: "mono-vertex-bypass-source",
                target: "mono-vertex-bypass-udf",
                monoVertexInternalEdge: true,
              }}
              source={"mono-vertex-bypass-source"}
              target={"mono-vertex-bypass-udf"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => {
      const edge = screen.getByTestId(
        "mono-vertex-bypass-source-mono-vertex-bypass-udf"
      );
      expect(edge).toBeInTheDocument();
      expect(edge).toHaveStyle("stroke: #8D9096");
      expect(edge).toHaveStyle("stroke-width: 2");
      expect(edge.getAttribute("style")).not.toContain("stroke-dasharray");
    });
  });
  it("MonoVertex bypass edge remains dotted and bypass-colored when highlighted", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {
            "mono-vertex-bypass-udf-mono-vertex-bypass-onSuccess-bypass": true,
          },
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
          setHidden: jest.fn(),
        }}
      >
        <ReactFlowProvider>
          <svg>
            <CustomEdge
              id={
                "mono-vertex-bypass-udf-mono-vertex-bypass-onSuccess-bypass"
              }
              sourceX={194}
              sourceY={92}
              targetX={290}
              targetY={60}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                source: "mono-vertex-bypass-udf",
                target: "mono-vertex-bypass-onSuccess",
                monoVertexBypassEdge: true,
                bypassTarget: "onSuccess",
                operator: "and",
                values: ["audit", "high-priority"],
              }}
              source={"mono-vertex-bypass-udf"}
              target={"mono-vertex-bypass-onSuccess"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => {
      const edge = screen.getByTestId(
        "mono-vertex-bypass-udf-mono-vertex-bypass-onSuccess-bypass"
      );
      expect(edge).toBeInTheDocument();
      expect(edge).toHaveStyle("stroke: var(--mono-vertex-bypass-color)");
      expect(edge).toHaveStyle("stroke-width: 2");
      expect(edge).toHaveStyle(
        "animation: monoVertexBypassFlow 0.8s linear infinite"
      );
      expect(edge).toHaveStyle("pointer-events: none");
      expect(edge).toHaveStyle("stroke-dasharray: 5 5");
      expect(edge).toHaveStyle("stroke-dashoffset: 0");
      expect(edge).toHaveAttribute(
        "d",
        "M 194 92 C 211.28 92 211.28 114 242 114 C 272.72 114 272.72 60 290 60"
      );
    });
  });
  it("MonoVertex fan-out onSuccess bypass edge keeps the upper arc", async () => {
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
              id={
                "mono-vertex-bypass-transformer-mono-vertex-bypass-onSuccess-bypass"
              }
              sourceX={132}
              sourceY={108}
              targetX={312}
              targetY={44}
              sourcePosition={Position.Bottom}
              targetPosition={Position.Left}
              data={{
                source: "mono-vertex-bypass-transformer",
                target: "mono-vertex-bypass-onSuccess",
                monoVertexBypassEdge: true,
                bypassSourceStage: "transformer",
                bypassTarget: "onSuccess",
                bypassTargetFanOut: true,
              }}
              source={"mono-vertex-bypass-transformer"}
              target={"mono-vertex-bypass-onSuccess"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => {
      expect(
        screen.getByTestId(
          "mono-vertex-bypass-transformer-mono-vertex-bypass-onSuccess-bypass"
        )
      ).toHaveAttribute(
        "d",
        "M 132 108 C 164.4 108 164.4 20 222 20 C 279.6 20 279.6 44 312 44"
      );
    });
  });
  it("dims unrelated MonoVertex internal edge during bypass highlight", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {
            monoVertexBypass: true,
            "mono-vertex-bypass-udf": true,
          },
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
          setHidden: jest.fn(),
        }}
      >
        <ReactFlowProvider>
          <svg>
            <CustomEdge
              id={"mono-vertex-bypass-source-mono-vertex-bypass-udf"}
              sourceX={140}
              sourceY={80}
              targetX={220}
              targetY={80}
              sourcePosition={Position.Right}
              targetPosition={Position.Left}
              data={{
                source: "mono-vertex-bypass-source",
                target: "mono-vertex-bypass-udf",
                monoVertexInternalEdge: true,
              }}
              source={"mono-vertex-bypass-source"}
              target={"mono-vertex-bypass-udf"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => {
      expect(
        screen.getByTestId("mono-vertex-bypass-source-mono-vertex-bypass-udf")
      ).toHaveStyle("opacity: 0.25");
    });
  });
  it("does not dim Pipeline edge during MonoVertex bypass highlight", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: { monoVertexBypass: true },
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
                source: "first",
                target: "second",
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
    await waitFor(() => {
      const edge = screen.getByTestId("first-second");
      expect(edge).toBeInTheDocument();
      expect(edge.getAttribute("style")).not.toContain("opacity");
    });
  });
  it("dims unrelated MonoVertex bypass edge during bypass highlight", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {
            monoVertexBypass: true,
            "mono-vertex-bypass-udf-mono-vertex-bypass-onSuccess-bypass": true,
          },
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
          setHidden: jest.fn(),
        }}
      >
        <ReactFlowProvider>
          <svg>
            <CustomEdge
              id={
                "mono-vertex-bypass-transformer-mono-vertex-bypass-fallback-bypass"
              }
              sourceX={132}
              sourceY={108}
              targetX={312}
              targetY={122}
              sourcePosition={Position.Bottom}
              targetPosition={Position.Left}
              data={{
                source: "mono-vertex-bypass-transformer",
                target: "mono-vertex-bypass-fallback",
                monoVertexBypassEdge: true,
                bypassSourceStage: "transformer",
                bypassTarget: "fallback",
              }}
              source={"mono-vertex-bypass-transformer"}
              target={"mono-vertex-bypass-fallback"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => {
      expect(
        screen.getByTestId(
          "mono-vertex-bypass-transformer-mono-vertex-bypass-fallback-bypass"
        )
      ).toHaveStyle("opacity: 0.25");
    });
  });
  it("MonoVertex bypass edge routes sink and fallback targets in separate arc lanes", async () => {
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
              id={"mono-vertex-bypass-transformer-mono-vertex-bypass-sink-bypass"}
              sourceX={132}
              sourceY={108}
              targetX={244}
              targetY={92}
              sourcePosition={Position.Bottom}
              targetPosition={Position.Left}
              data={{
                source: "mono-vertex-bypass-transformer",
                target: "mono-vertex-bypass-sink",
                monoVertexBypassEdge: true,
                bypassSourceStage: "transformer",
                bypassTarget: "sink",
              }}
              source={"mono-vertex-bypass-transformer"}
              target={"mono-vertex-bypass-sink"}
            />
            <CustomEdge
              id={
                "mono-vertex-bypass-transformer-mono-vertex-bypass-fallback-bypass"
              }
              sourceX={132}
              sourceY={108}
              targetX={312}
              targetY={122}
              sourcePosition={Position.Bottom}
              targetPosition={Position.Left}
              data={{
                source: "mono-vertex-bypass-transformer",
                target: "mono-vertex-bypass-fallback",
                monoVertexBypassEdge: true,
                bypassSourceStage: "transformer",
                bypassTarget: "fallback",
              }}
              source={"mono-vertex-bypass-transformer"}
              target={"mono-vertex-bypass-fallback"}
            />
          </svg>
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => {
      expect(
        screen.getByTestId(
          "mono-vertex-bypass-transformer-mono-vertex-bypass-sink-bypass"
        )
      ).toHaveAttribute(
        "d",
        "M 132 108 C 152.16 108 152.16 130 188 130 C 223.84 130 223.84 92 244 92"
      );
      expect(
        screen.getByTestId(
          "mono-vertex-bypass-transformer-mono-vertex-bypass-fallback-bypass"
        )
      ).toHaveAttribute(
        "d",
        "M 132 108 C 164.4 108 164.4 150 222 150 C 279.6 150 279.6 122 312 122"
      );
    });
  });
});
