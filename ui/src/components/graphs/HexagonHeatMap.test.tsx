import HexagonHeatMap from "./HexagonHeatMap";
import {fireEvent, render, screen} from "@testing-library/react";

const data = [{
    "name": "simple-pipeline-input-0-rwodz",
    "data": {
        "name": "simple-pipeline-input-0-rwodz",
        "pod": {"name": "simple-pipeline-input-0-rwodz", "containers": ["numa"], "containerSpecMap": {}},
        "details": {"name": "simple-pipeline-input-0-rwodz", "containerMap": {}},
        "maxCPUPerc": 74.295736,
        "maxMemPerc": 14.5172119140625,
        "container": [{
            "name": "numa",
            "cpu": "74295736n",
            "mem": "19028Ki",
            "cpuPercent": 74.295736,
            "memoryPercent": 14.5172119140625
        }]
    },
    "healthPercent": 14.5172119140625,
    "fill": "rgb(127, 208, 0)"
}]
const handleClick = jest.fn()
const tooltipComponent = jest.fn()

describe("HexagonHeatMap", () => {
    it("loads screen", () => {
        render(
            <HexagonHeatMap data={data} handleClick={handleClick} tooltipComponent={tooltipComponent}
                            selected={"true"}/>)
        fireEvent.click(screen.getByTestId("hexagon"))
        expect(screen.getByTestId("hexagon")).toBeInTheDocument();
        expect(screen.getByTestId("hexagon")).toBeVisible();
    })

    it("loads tooltip on hexagon click", () => {
        render(
            <HexagonHeatMap data={data} handleClick={handleClick} tooltipComponent={tooltipComponent}
                            selected={"true"}/>)
        fireEvent.mouseMove(screen.getByTestId("hexagon"))
        expect(screen.getByTestId("hexagon")).toBeVisible();
        expect(screen.getByTestId("tooltip")).toBeVisible();
    })
})