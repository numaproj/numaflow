import EdgeInfo from "./EdgeInfo";
import { render, screen } from "@testing-library/react";

describe("EdgeInfo", () => {
  const edge = {
    data: {
      conditions: { keyIn: "log" },
      isFull: true,
      bufferUsage: 0.8,
      label: 8000,
      id: "postproc-log",
      fromVertex: "postproc",
      toVertex: "log",
    },
    id: "postproc-log",
    source: "postproc",
    target: "log",
  };
  const edges = [
    {
      id: "postproc-log",
      label: "0",
      source: "postproc",
      target: "log",
      data: {
        fromVertex: "postproc",
        toVertex: "log",
        pendingCount: 0,
        ackPendingCount: 0,
        totalMessages: 0,
        bufferLength: 30000,
        bufferUsageLimit: 0.8,
        bufferUsage: 0,
        bufferName: "random-postproc-log",
        isFull: true,
        conditions: { keyIn: "log" },
        pending: 0,
        ackPending: 0
      }
    }
  ]

  it("loads", () => {
    render(<EdgeInfo edge={edge} edges={edges}/>);
    expect(screen.getByTestId("conditions")).toBeVisible();
    expect(screen.getByTestId("isFull")).toBeVisible();
    expect(screen.getByTestId("pending")).toBeVisible();
    expect(screen.getByTestId("usage")).toBeVisible();
    expect(screen.getByTestId("bufferLength")).toBeVisible();
  });
});
