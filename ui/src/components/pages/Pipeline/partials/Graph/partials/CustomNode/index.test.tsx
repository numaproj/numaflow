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
