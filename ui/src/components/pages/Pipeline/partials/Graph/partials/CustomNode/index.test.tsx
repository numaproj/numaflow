import React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { ReactFlowProvider } from "@xyflow/react";
import CustomNode from "./index";

import "@testing-library/jest-dom";
import { HighlightContext } from "../..";

describe("Graph screen test", () => {
  it("Source vertex", async () => {
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
          <CustomNode
            data={{ type: "source", name: "source" }}
            id={"input"}
            selected={false}
            type={""}
            zIndex={0}
            isConnectable={false}
            xPos={0}
            yPos={0}
            dragging={false}
          />
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() =>
      expect(screen.getByTestId("source")).toBeInTheDocument()
    );
  });

  it("UDF vertex", async () => {
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
          <CustomNode
            data={{ type: "udf", name: "udf" }}
            id={"input"}
            selected={false}
            type={""}
            zIndex={0}
            isConnectable={false}
            xPos={0}
            yPos={0}
            dragging={false}
          />
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => expect(screen.getByTestId("udf")).toBeInTheDocument());
  });

  it("UDF vertex with side inputs", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: { xyz: true, "---": true },
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map([
            [
              "xyz",
              {
                id: "xyz",
                data: {
                  name: "xyz",
                  nodeInfo: { name: "xyz" },
                  type: "sideInput",
                  sideHandle: true,
                },
                position: { x: 0, y: 0 },
              },
            ],
          ]),
          sideInputEdges: new Map([["xyz-udf", "3-0"]]),
          setHidden: jest.fn(),
        }}
      >
        <ReactFlowProvider>
          <CustomNode
            data={{
              type: "udf",
              name: "udf",
              nodeInfo: { sideInputs: ["xyz"] },
            }}
            id={"input"}
            selected={false}
            type={""}
            zIndex={0}
            isConnectable={false}
            xPos={0}
            yPos={0}
            dragging={false}
          />
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => expect(screen.getByTestId("udf")).toBeInTheDocument());
    fireEvent.mouseOver(document.getElementsByClassName("sideInput_handle")[0]);
    fireEvent.mouseOut(document.getElementsByClassName("sideInput_handle")[0]);
    fireEvent.click(document.getElementsByClassName("sideInput_handle")[0]);
  });

  it("Sink vertex", async () => {
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
          <CustomNode
            data={{ type: "sink", name: "sink" }}
            id={"input"}
            selected={false}
            type={""}
            zIndex={0}
            isConnectable={false}
            xPos={0}
            yPos={0}
            dragging={false}
          />
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => expect(screen.getByTestId("sink")).toBeInTheDocument());
  });

  it("Generator vertex", async () => {
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
          <CustomNode
            data={{ type: "generator", name: "generator" }}
            id={"input"}
            selected={false}
            type={""}
            zIndex={0}
            isConnectable={false}
            xPos={0}
            yPos={0}
            dragging={false}
          />
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() =>
      expect(screen.getByText("Generator")).toBeInTheDocument()
    );
    //no propagation
    fireEvent.click(document.getElementsByClassName("generator_node")[0]);
  });

  it("SideInput vertex", async () => {
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
          <CustomNode
            data={{ type: "sideInput", name: "sideInput" }}
            id={"input"}
            selected={false}
            type={""}
            zIndex={0}
            isConnectable={false}
            xPos={0}
            yPos={0}
            dragging={false}
          />
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() =>
      expect(screen.getByTestId("sideInput-sideInput")).toBeInTheDocument()
    );
  });

  it("MonoVertex internal vertex with bypass markers", async () => {
    const setHighlightValues = jest.fn();
    const setHidden = jest.fn();
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {},
          setHighlightValues,
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
          setHidden,
        }}
      >
        <ReactFlowProvider>
          <CustomNode
            data={{
              type: "monoVertexInternal",
              name: "mono-vertex-bypass-udf",
              monoVertexStage: "udf",
              bypassTargets: [
                {
                  id: "mono-vertex-bypass-udf-mono-vertex-bypass-fallback-bypass",
                  target: "fallback",
                  source: "udf",
                  sourceNodeId: "mono-vertex-bypass-udf",
                  targetNodeId: "mono-vertex-bypass-fallback",
                  operator: "or",
                  values: ["corrupted", "parse-error"],
                },
                {
                  id: "mono-vertex-bypass-udf-mono-vertex-bypass-onSuccess-bypass",
                  target: "onSuccess",
                  source: "udf",
                  sourceNodeId: "mono-vertex-bypass-udf",
                  targetNodeId: "mono-vertex-bypass-onSuccess",
                  operator: "and",
                  values: ["audit", "high-priority"],
                },
              ],
            }}
            id={"mono-vertex-bypass-udf"}
            selected={false}
            type={""}
            zIndex={0}
            isConnectable={false}
            xPos={0}
            yPos={0}
            dragging={false}
          />
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() =>
      expect(screen.getByTestId("mono-vertex-bypass-udf")).toBeInTheDocument()
    );
    expect(screen.getByAltText("udf-bypass")).toBeInTheDocument();
    expect(screen.queryByAltText("udf-fallback-bypass")).not.toBeInTheDocument();
    expect(screen.queryByAltText("udf-onSuccess-bypass")).not.toBeInTheDocument();
    fireEvent.mouseOver(screen.getByAltText("udf-bypass"));
    expect(setHidden).toHaveBeenCalled();
    const updateHidden = setHidden.mock.calls[0][0];
    expect(
      updateHidden({
        "mono-vertex-bypass-udf-mono-vertex-bypass-fallback-bypass": true,
        "mono-vertex-bypass-udf-mono-vertex-bypass-onSuccess-bypass": true,
      })
    ).toEqual({
      "mono-vertex-bypass-udf-mono-vertex-bypass-fallback-bypass": false,
      "mono-vertex-bypass-udf-mono-vertex-bypass-onSuccess-bypass": false,
    });
    expect(setHighlightValues).toHaveBeenCalledWith({
      monoVertexBypass: true,
      "mono-vertex-bypass-udf": true,
      "mono-vertex-bypass-fallback": true,
      "mono-vertex-bypass-onSuccess": true,
      "mono-vertex-bypass-udf-mono-vertex-bypass-fallback-bypass": true,
      "mono-vertex-bypass-udf-mono-vertex-bypass-onSuccess-bypass": true,
    });
    expect(
      screen.queryByText("Bypass from udf to fallback")
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText("Bypass from udf to onSuccess")
    ).not.toBeInTheDocument();
    fireEvent.mouseOut(screen.getByAltText("udf-bypass"));
    expect(setHighlightValues).toHaveBeenLastCalledWith({});
  });

  it("dims unrelated MonoVertex internal vertex during bypass highlight", async () => {
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
          <CustomNode
            data={{
              type: "monoVertexInternal",
              name: "mono-vertex-bypass-source",
              monoVertexStage: "source",
              bypassTargets: [],
            }}
            id={"mono-vertex-bypass-source"}
            selected={false}
            type={""}
            zIndex={0}
            isConnectable={false}
            xPos={0}
            yPos={0}
            dragging={false}
          />
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() =>
      expect(screen.getByTestId("mono-vertex-bypass-source")).toHaveStyle(
        "opacity: 0.35"
      )
    );
  });

  it("Source vertex with error", async () => {
    const { container } = render(
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
          <CustomNode
            data={{
              name: "input",
              type: "source",
              vertexMetrics: { error: true },
              podnum: 1,
            }}
            id={"input"}
            selected={false}
            type={""}
            zIndex={0}
            isConnectable={false}
            xPos={0}
            yPos={0}
            dragging={false}
          />
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("input")).toBeVisible();
      fireEvent.mouseEnter(
        container.getElementsByClassName("react-flow__node-input")[0]
      );
      fireEvent.mouseLeave(
        container.getElementsByClassName("react-flow__node-input")[0]
      );
    });
  });

  it("Source vertex without error", async () => {
    const { container } = render(
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
          <CustomNode
            data={{
              name: "input",
              type: "source",
              vertexMetrics: { error: false },
              podnum: 2,
            }}
            id={"input"}
            selected={false}
            type={""}
            zIndex={0}
            isConnectable={false}
            xPos={0}
            yPos={0}
            dragging={false}
          />
        </ReactFlowProvider>
      </HighlightContext.Provider>
    );
    await waitFor(() => {
      expect(screen.getByTestId("input")).toBeVisible();
      fireEvent.mouseEnter(
        container.getElementsByClassName("react-flow__node-input")[0]
      );
      fireEvent.mouseLeave(
        container.getElementsByClassName("react-flow__node-input")[0]
      );
    });
  });
});
