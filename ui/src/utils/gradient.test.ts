import { fill, getColorCode, imageForStatus, rgba2rgb } from "./gradients";

const cpuColors = {
  infinite: [100, 100000],
  red: [76, 1000],
  orange: [51, 75],
  yellow: [31, 50],
  green: [0, 30],
};
describe("gradients", () => {
  it("rgba2rgb", () => {
    expect(rgba2rgb([255, 255, 255], [127, 208, 0], 1)).toEqual("#7fd000");
  });

  it("getColorCode", () => {
    expect(getColorCode(cpuColors, 60)).toEqual("orange");
    expect(getColorCode(cpuColors, 33)).toEqual("yellow");
    expect(getColorCode(cpuColors, 70)).toEqual("orange");
    expect(getColorCode(cpuColors, -1)).toEqual("grey");
    expect(getColorCode(cpuColors, 101)).toEqual("infinite");
    expect(getColorCode(cpuColors, 0)).toEqual("green");
  });

  it("fill", () => {
    expect(fill(cpuColors, 8, 0.5, 10)).toEqual("rgb(247, 87, 108)");
  });
});

// write test for imageForStatus
describe("imageForStatus", () => {
  it("imageForStatus", () => {
    expect(imageForStatus(cpuColors, 60, 100)).toEqual("exclamation.svg");
  });
});
