import { fill, getColorCode, rgba2rgb } from "./gradients"

describe("gradients", () => {

    const cpuColors = {
        infinite: [100, 100000],
        red: [76, 1000],
        orange: [51, 75],
        yellow: [31, 50],
        green: [0, 30],
    };

    it("rgba2rgb", () => {
        expect(rgba2rgb([255, 255, 255], [127, 208, 0], 1)).toEqual("#7fd000")
    })

    it("getColorCode", () => {
        expect(getColorCode( cpuColors, 60)).toEqual("orange")
        expect(getColorCode(cpuColors, 33)).toEqual("yellow")
        expect(getColorCode(cpuColors, 70)).toEqual("orange")
    })

    it("fill", () => {
        expect(fill(cpuColors, 8, 0.5, 10)).toEqual("rgb(247, 87, 108)")
    })
})