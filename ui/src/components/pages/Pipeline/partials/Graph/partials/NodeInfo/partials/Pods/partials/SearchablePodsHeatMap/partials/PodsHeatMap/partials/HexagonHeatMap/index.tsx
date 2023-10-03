import { useCallback, useState } from "react";
import { Polygon } from "@visx/shape";
import { Group } from "@visx/group";
import { Tooltip, useTooltip } from "@visx/tooltip";
import { localPoint } from "@visx/event";
import { EventType } from "@visx/event/lib/types";
import {
  HexagonHeatMapProps,
  HexagonPoints,
} from "../../../../../../../../../../../../../../../types/declarations/pods";

import "./style.css";

export const MAXIMUM_RADIUS = 15;
export const MAXIMUM_WIDTH = 400;
export const MAXIMUM_HEIGHT = 200;
export const TOOLTIP_OFFSET = 0;
export const MIN_HEXAGONS = 3;
export const DEFAULT_COLOR = "#76b3f7";
export const DEFAULT_STROKE = "rgb(25, 118, 210)"; //"#ffffff"
export const DEFAULT_OPACITY = 0.5;
export const HOVER_OPACITY = 0.8;
export const SELECTED_STROKE = "rgb(25, 118, 210)";

function HexagonHeatMap({
  data,
  handleClick,
  tooltipComponent,
  tooltipClass,
  selected,
}: HexagonHeatMapProps) {
  const [hover, setHover] = useState<null | string>(null);
  const margin = {
    top: 30,
    right: 0,
    bottom: 0,
    left: 30,
  };

  const sqrtOfTotal = Math.ceil(Math.sqrt(data.length));

  // The number of columns and rows of the heatmap
  // encourage more count along horizontal direction by adding 3
  const mapColumns =
    data.length > sqrtOfTotal + MIN_HEXAGONS
      ? sqrtOfTotal + MIN_HEXAGONS
      : data.length;
  const mapRows = mapColumns ? Math.ceil(data.length / mapColumns) : 0;

  // calculate expected width of svg with the given data
  const derivedWidth =
    MAXIMUM_RADIUS * (mapColumns + 0.5) * Math.sqrt(3) +
    margin.left +
    margin.right;
  const derivedHeight =
    MAXIMUM_RADIUS * 1.5 * (mapRows + 1 / 3) + margin.top + margin.bottom;

  // calculate actual width of svg by checking against max limit
  const width =
    derivedWidth > MAXIMUM_WIDTH
      ? MAXIMUM_WIDTH
      : derivedWidth - margin.left - margin.right;
  const height =
    derivedHeight > MAXIMUM_HEIGHT
      ? MAXIMUM_HEIGHT
      : derivedHeight - margin.top - margin.bottom;

  // calculate hexagon radius
  // The maximum radius the hexagons can have to still fit the screen
  const hexRadius = Math.min(
    width / ((mapColumns + 0.5) * Math.sqrt(3)),
    height / ((mapRows + 1 / 3) * 1.5),
    MAXIMUM_RADIUS
  );

  // Calculate the center position of each hexagon
  const hexagons: HexagonPoints[] = [];
  let index = 0;
  for (let i = 0; i < mapRows; i += 1) {
    for (let j = 0; j < mapColumns; j += 1) {
      let x = hexRadius * j * Math.sqrt(3);
      // Offset each uneven row by half of a "hex-width" to the right
      if (i % 2 === 1) {
        x += (hexRadius * Math.sqrt(3)) / 2;
      }
      const y = hexRadius * i * 1.5;
      if (data[index]) {
        hexagons.push({ x, y, data: data[index] });
      }
      index += 1;
    } // for j
  } // for i

  const {
    showTooltip,
    hideTooltip,
    tooltipData,
    tooltipLeft = 0,
    tooltipTop = 0,
  } = useTooltip();

  const handleTooltip = useCallback(
    (event: Element | EventType, hexagon: HexagonPoints) => {
      const { x, y } = localPoint(event) || { x: 0, y: 0 };
      const left = x - margin.left + TOOLTIP_OFFSET;
      const top = y - margin.top - TOOLTIP_OFFSET;
      showTooltip({
        tooltipData: hexagon,
        tooltipLeft: left + margin.left - 10,
        tooltipTop: top,
      });
    },
    []
  );

  return (
    <div className="hexagon-map">
      <svg
        width={width + margin.left + margin.right}
        height={height + margin.top + margin.bottom}
      >
        <Group top={margin.top} left={margin.left}>
          {hexagons.map((hexagon, i) => (
            <Polygon
              data-testid={`hexagon_${data[i].name}-${data[i]?.type}`}
              key={`hexagon_${data[i].name}-${data[i]?.type}`}
              style={{ cursor: "pointer" }}
              sides={6}
              size={hexRadius}
              center={hexagon}
              fill={hexagon?.data?.fill ? hexagon.data.fill : DEFAULT_COLOR}
              opacity={
                (hover && hover === hexagon.data.name) ||
                selected === hexagon.data.name
                  ? HOVER_OPACITY
                  : DEFAULT_OPACITY
              }
              rotate={90}
              stroke={
                selected === hexagon.data.name
                  ? SELECTED_STROKE
                  : DEFAULT_STROKE
              }
              strokeWidth={selected === hexagon.data.name ? 2.5 : 1}
              onClick={(e) => handleClick(e, hexagon.data)}
              onMouseMove={(e) => {
                setHover(hexagon.data.name);
                handleTooltip(e, hexagon);
              }}
              onMouseLeave={() => {
                setHover(null);
                hideTooltip();
              }}
            />
          ))}
        </Group>
      </svg>
      {tooltipData && (
        <Tooltip
          data-testid="tooltip"
          top={tooltipTop}
          left={tooltipLeft}
          className={tooltipClass}
        >
          {tooltipComponent(tooltipData)}
        </Tooltip>
      )}
    </div>
  );
}

export default HexagonHeatMap;
