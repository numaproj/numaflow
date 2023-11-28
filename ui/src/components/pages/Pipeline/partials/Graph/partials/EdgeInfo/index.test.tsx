import { fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import EdgeInfo from "./index";

describe("EdgeInfo", () => {
  const edge = {
    data: {
      conditions: { keyIn: "log" },
      isFull: true,
      backpressureLabel: 8000,
      edgeWatermark: {
        isWaterMarkEnabled: true,
        watermarks: [1690792996319],
      },
    },
    id: "postproc-log",
    source: "postproc",
    target: "log",
  };

  it("loads", () => {
    render(<EdgeInfo edge={edge} />);
    expect(screen.getByTestId("conditions")).toBeVisible();
    expect(screen.getByTestId("watermarks")).toBeVisible();
    fireEvent.click(screen.getByTestId("conditions"));
  });
});
