import { fireEvent, render, screen } from "@testing-library/react";
import NodeInfo from "./index";

describe("NodeInfo", () => {
  const node = {
    id: "node",
    position: undefined,
    data: {
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
      vertexMetrics: {
        ratePerMin: "5.00",
        ratePerFiveMin: "5.00",
        ratePerFifteenMin: "5.00",
        podMetrics: [
          {
            pipeline: "simple-pipeline",
            vertex: "in",
            processingRates: {
              "15m": 5,
              "1m": 5,
              "5m": 5,
              default: 5,
            },
          },
        ],
        error: false,
      },
      buffers: [
        {
          pipeline: "simple-pipeline",
          bufferName: "default-simple-pipeline-cat-0",
          pendingCount: 0,
          ackPendingCount: 2,
          totalMessages: 2,
          bufferLength: 30000,
          bufferUsageLimit: 0.8,
          bufferUsage: 0.01,
          isFull: false,
        },
        {
          pipeline: "simple-pipeline",
          bufferName: "default-simple-pipeline-cat-1",
          pendingCount: 0,
          ackPendingCount: 2,
          totalMessages: 2,
          bufferLength: 30000,
          bufferUsageLimit: 0.8,
          bufferUsage: 0.02,
          isFull: true,
        },
      ],
    },
  };
  it("loads", () => {
    render(
      <NodeInfo
        node={node}
        namespaceId={"default"}
        pipelineId={"simple-pipeline"}
      />
    );
    expect(screen.getByTestId("pods-view")).toBeVisible();
    fireEvent.click(screen.getByTestId("spec"));
  });

  it("namespace empty", () => {
    render(<NodeInfo node={node} namespaceId={""} pipelineId={"pipeline-1"} />);
    expect(screen.queryByTestId("dialog")).toBeNull();
  });
});
