import { scaleLinear } from "d3-scale";

const INFINITE = "infinite";
const RED = "red";
const ORANGE = "orange";
const YELLOW = "yellow";
const GREY = "grey";
const GREEN = "green";

export const rgba2rgb = (background: number[], color: number[], alpha: number) => {
  const r = Math.floor((1 - alpha) * background[0] + alpha * color[0]);
  const g = Math.floor((1 - alpha) * background[1] + alpha * color[1]);
  const b = Math.floor((1 - alpha) * background[2] + alpha * color[2]);
  return "#" + ((r << 16) | (g << 8) | b).toString(16);
};

export const getColorCode = (type: FillType, value: number) => {
  if (value > 100) {
    return INFINITE;
  }

  if (value >= type[RED][0]) {
    return RED;
  }

  if (value >= type[ORANGE][0]) {
    return ORANGE;
  }

  if (value >= type[YELLOW][0]) {
    return YELLOW;
  }

  if (value == null || value < 0) {
    return GREY;
  }

  return GREEN;
};
// type of fill
interface FillType {
    infinite: number[];
    red: number[];
    orange: number[];
    yellow: number[];
    green: number[];
}


export const fill = (
  type: FillType,
  value: number,
  opacity: number,
  limit = 100
) => {
  let pct = (value / limit) * 100;

  if (value == null) {
    pct = -1;
  }

  const colorMap = {
    [INFINITE]: scaleLinear<string>()
      .domain(type[RED] || [])
      .range([
        rgba2rgb([255, 255, 255], [185, 0, 0], opacity),
        rgba2rgb([255, 255, 255], [185, 0, 0], opacity),
      ]),
    [RED]: scaleLinear<string>()
      .domain(type[RED] || [])
      .range([
        rgba2rgb([255, 255, 255], [242, 90, 82], opacity),
        rgba2rgb([255, 255, 255], [185, 0, 0], opacity),
      ]),
    [ORANGE]: scaleLinear<string>()
      .domain(type[ORANGE] || [])
      .range([
        rgba2rgb([255, 255, 255], [255, 173, 0], opacity),
        rgba2rgb([255, 255, 255], [255, 128, 0], opacity),
      ]),
    [YELLOW]: scaleLinear<string>()
      .domain(type[YELLOW] || [])
      .range([
        rgba2rgb([255, 255, 255], [255, 220, 0], opacity),
        rgba2rgb([255, 255, 255], [255, 173, 0], opacity),
      ]),
    [GREEN]: scaleLinear<string>()
      .domain(type[GREEN] || [])
      .range([
        rgba2rgb([255, 255, 255], [127, 208, 0], opacity),
        rgba2rgb([255, 255, 255], [127, 208, 0], opacity),
      ]),
    [GREY]: scaleLinear<string>()
      .domain([-1, -1])
      .range([
        rgba2rgb([255, 255, 255], [186, 190, 197], opacity),
        rgba2rgb([255, 255, 255], [186, 190, 197], opacity),
      ]),
  };

  return colorMap[getColorCode(type, pct)](pct);
};
