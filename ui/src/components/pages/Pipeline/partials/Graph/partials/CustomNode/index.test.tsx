import React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { ReactFlowProvider } from "reactflow";
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
  it("Sink vertex", async () => {
    render(
      <HighlightContext.Provider
        value={{
          highlightValues: {},
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
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
  it("Source vertex with error", async () => {
    const { container } = render(
      <HighlightContext.Provider
        value={{
          highlightValues: {},
          setHighlightValues: jest.fn(),
          sideInputNodes: new Map(),
          sideInputEdges: new Map(),
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
