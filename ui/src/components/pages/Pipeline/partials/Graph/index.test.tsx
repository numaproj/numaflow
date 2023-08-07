import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Position } from "reactflow";
import Graph from "./index";

global.ResizeObserver = require("resize-observer-polyfill");

describe("Graph screen test", () => {
  const data = {
    edges: [
      {
        id: "in-cat",
        source: "in",
        target: "cat",
        data: {
          conditions: null,
          backpressureLabel: 0,
          isFull: false,
          edgeWatermark: {
            isWaterMarkEnabled: true,
            watermarks: [1690811029780],
          },
        },
        animated: true,
        type: "custom",
      },
      {
        id: "cat-out",
        source: "cat",
        target: "out",
        data: {
          conditions: null,
          backpressureLabel: 0,
          isFull: false,
          edgeWatermark: {
            isWaterMarkEnabled: true,
            watermarks: [1690811028779],
          },
        },
        animated: true,
        type: "custom",
      },
    ],
    vertices: [
      {
        id: "in",
        data: {
          name: "in",
          podnum: 1,
          nodeInfo: {
            name: "in",
            source: {
              generator: {
                rpu: 5,
              },
            },
            scale: {},
          },
          type: "source",
          vertexMetrics: null,
          buffers: null,
        },
        position: {
          x: 86.0001576029452,
          y: 18,
        },
        targetPosition: Position.Left,
        sourcePosition: Position.Right,
      },
      {
        id: "cat",
        data: {
          name: "cat",
          podnum: 1,
          nodeInfo: {
            name: "cat",
            udf: {
              container: null,
              builtin: {
                name: "cat",
              },
              groupBy: null,
            },
            scale: {},
          },
          type: "udf",
          vertexMetrics: null,
          buffers: [
            {
              pipeline: "simple-pipeline",
              bufferName: "default-simple-pipeline-cat-0",
              pendingCount: 0,
              ackPendingCount: 0,
              totalMessages: 0,
              bufferLength: 30000,
              bufferUsageLimit: 0.8,
              bufferUsage: 0,
              isFull: false,
            },
          ],
        },
        position: {
          x: 338.0002889015671,
          y: 18,
        },
        type: "custom",
        targetPosition: Position.Left,
        sourcePosition: Position.Right,
      },
      {
        id: "out",
        data: {
          name: "out",
          podnum: 1,
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
              bufferName: "default-simple-pipeline-out-0",
              pendingCount: 0,
              ackPendingCount: 0,
              totalMessages: 0,
              bufferLength: 30000,
              bufferUsageLimit: 0.8,
              bufferUsage: 0,
              isFull: false,
            },
          ],
        },
        position: {
          x: 590.0004071133181,
          y: 18,
        },
        type: "custom",
        targetPosition: Position.Left,
        sourcePosition: Position.Right,
      },
    ],
  };

  it("Load Graph screen", async () => {
    const { container } = render(
      <Graph
        data={data}
        pipelineId={"simple-pipeline"}
        namespaceId={"default"}
      />
    );
    await waitFor(() => {
      expect(screen.getByTestId("graph")).toBeVisible();
      fireEvent.click(screen.getByTestId("rf__node-in"));
      fireEvent.click(container.getElementsByClassName("react-flow__pane")[0]);
    });
    await waitFor(() => expect(screen.getByTestId("card")).toBeVisible());
  });
});
