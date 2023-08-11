import { render, screen } from "@testing-library/react";
import HexagonHeatMap from "./index";

const data = [
  {
    name: "simple-pipeline-input-0",
  },
  {
    name: "simple-pipeline-input-1",
  },
  {
    name: "simple-pipeline-input-2",
  },
  {
    name: "simple-pipeline-input-3",
  },
  {
    name: "simple-pipeline-input-4",
  },
  {
    name: "simple-pipeline-input-5",
  },
  {
    name: "simple-pipeline-input-6",
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
      screen.getByTestId("hexagon_simple-pipeline-input-0-undefined")
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("hexagon_simple-pipeline-input-0-undefined")
    ).toBeVisible();
  });
});
