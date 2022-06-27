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
    },
    id: "postproc-log",
    source: "postproc",
    target: "log",
  };
  it("loads", () => {
    render(<EdgeInfo edge={edge} />);
    expect(screen.getByTestId("conditions")).toBeVisible();
    expect(screen.getByTestId("isFull")).toBeVisible();
    expect(screen.getByTestId("pending")).toBeVisible();
    expect(screen.getByTestId("usage")).toBeVisible();
    expect(screen.getByTestId("bufferLength")).toBeVisible();
  });
});
