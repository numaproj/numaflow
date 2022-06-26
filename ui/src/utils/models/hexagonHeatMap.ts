export interface Hexagon {
  name: string
  data?: any
  tooltip?: string
  healthPercent?: number
  fill?: string
  stroke?: string
  strokeWidth?: number
}

export interface HexagonPoints {
  x: number
  y: number
  data?: any
}

export interface HexagonHeatMapProps {
  data: Hexagon[]
  handleClick: any
  tooltipComponent: any
  tooltipClass?: string
  selected: string | undefined
}