import { FC, memo } from "react";
import { Tooltip } from "@mui/material";
import { EdgeProps, EdgeLabelRenderer, getSmoothStepPath } from "reactflow";

import "reactflow/dist/style.css";
import "./style.css";

const CustomEdge: FC<EdgeProps> = ({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
}) => {
  const obj = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const [edgePath, labelX] = obj;
  let [, , labelY] = obj;

  // Highlight the edge on isFull - thick red line
  let edgeStyle, wmStyle, pendingStyle;

  if (data?.isFull) {
    edgeStyle = {
      stroke: "red",
      strokeWidth: "0.125rem",
      fontWeight: 700,
    };
  }

  if (sourceY === targetY) {
    wmStyle = { marginTop: "-0.87rem" };
    pendingStyle = { marginTop: "0.15rem" };
  } else {
    wmStyle = { right: "0.1rem" };
    pendingStyle = { left: "0.1rem" };
    if (sourceY < targetY) {
      labelY = targetY - 14;
    } else {
      labelY = targetY + 2;
    }
  }
  const getMinWM = () => {
    if (data?.edgeWatermark?.watermarks) {
      return Math.min.apply(null, data?.edgeWatermark?.watermarks);
    } else {
      return -1;
    }
  };

  const getDelay = () => {
    const str = "Delay - ";
    const fetchWMTime = data?.edgeWatermark?.WMFetchTime;
    const minWM = getMinWM();
    let difference =
      (fetchWMTime ? fetchWMTime : Date.now()) - (minWM == -1 ? 0 : minWM);

    const years = Math.floor(difference / (1000 * 60 * 60 * 24 * 365));
    difference -= years * (1000 * 60 * 60 * 24 * 365);
    const months = Math.floor(difference / (1000 * 60 * 60 * 24 * 30));
    difference -= months * (1000 * 60 * 60 * 24 * 30);
    const days = Math.floor(difference / (1000 * 60 * 60 * 24));
    difference -= days * (1000 * 60 * 60 * 24);
    const hours = Math.floor(difference / (1000 * 60 * 60));
    difference -= hours * (1000 * 60 * 60);
    const minutes = Math.floor(difference / (1000 * 60));
    difference -= minutes * (1000 * 60);
    const seconds = Math.floor(difference / 1000);
    difference -= seconds * 1000;
    const milliseconds = difference;

    if (years > 0) {
      return str + `${years}yr ${months}mo`;
    } else if (months > 0) {
      return str + `${months}mo ${days}d`;
    } else if (days > 0) {
      return str + `${days}d ${hours}hr`;
    } else if (hours > 0) {
      return str + `${hours}hr ${minutes}min`;
    } else if (minutes > 0) {
      return str + `${minutes}min ${seconds}sec`;
    } else if (seconds > 0) {
      return str + `${seconds}sec ${milliseconds}ms`;
    } else {
      return str + `${milliseconds}ms`;
    }
  };

  return (
    <>
      <svg>
        <path
          id={id}
          className="react-flow__edge-path"
          d={edgePath}
          style={edgeStyle}
          data-testid={id}
        />
      </svg>
      <EdgeLabelRenderer>
        <div
          className={"edge"}
          style={{
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
          }}
        >
          {data?.edgeWatermark?.isWaterMarkEnabled && (
            <Tooltip
              title={
                <div className={"edge-tooltip"}>
                  <div>Watermark</div>
                  <div>{new Date(getMinWM()).toISOString()}</div>
                  <div>{getDelay()}</div>
                </div>
              }
              arrow
              placement={"top"}
            >
              <div className={"edge-label"} style={wmStyle}>
                {getMinWM()}
              </div>
            </Tooltip>
          )}
          <Tooltip
            title={<div className={"edge-tooltip"}>Pending</div>}
            arrow
            placement={"bottom"}
          >
            <div className={"edge-label"} style={pendingStyle}>
              {data?.backpressureLabel}
            </div>
          </Tooltip>
        </div>
      </EdgeLabelRenderer>
    </>
  );
};

export default memo(CustomEdge);
