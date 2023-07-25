import EdgeInfo from "./index";
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

  it("loads", () => {
    render(<EdgeInfo edge={edge} />);
    expect(screen.getByTestId("conditions")).toBeVisible();
  });
});
