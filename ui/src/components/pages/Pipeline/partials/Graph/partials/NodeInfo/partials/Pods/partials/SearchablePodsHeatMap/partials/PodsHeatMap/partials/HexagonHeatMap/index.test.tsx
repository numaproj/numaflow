import { fireEvent, render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import HexagonHeatMap from "./index";

const data = [
  {
    name: "simple-pipeline-input-0",
    type: "cpu",
  },
  {
    name: "simple-pipeline-input-1",
    type: "cpu",
  },
  {
    name: "simple-pipeline-input-2",
    type: "cpu",
  },
  {
    name: "simple-pipeline-input-3",
    type: "cpu",
  },
  {
    name: "simple-pipeline-input-4",
    type: "cpu",
  },
  {
    name: "simple-pipeline-input-5",
    type: "cpu",
  },
  {
    name: "simple-pipeline-input-6",
    type: "cpu",
  },
];
const handleClick = jest.fn();
const tooltipComponent = jest.fn();

describe("HexagonHeatMap", () => {
  it("loads screen", () => {
    render(
      <HexagonHeatMap
        data={data}
        handleClick={handleClick}
        tooltipComponent={tooltipComponent}
        selected={"true"}
      />
    );
    expect(
      screen.getByTestId("hexagon_simple-pipeline-input-0-cpu")
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("hexagonImage_simple-pipeline-input-0-cpu")
    ).toBeVisible();
    const ele = screen.getByTestId("hexagonImage_simple-pipeline-input-0-cpu");
    fireEvent.click(ele);
    fireEvent.mouseMove(ele);
    fireEvent.mouseOut(ele);
  });
});
