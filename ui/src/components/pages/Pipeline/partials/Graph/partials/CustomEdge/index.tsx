import { FC, memo } from "react";
import { Tooltip } from "@mui/material";
import { EdgeProps, EdgeLabelRenderer, getSmoothStepPath } from "reactflow";
import { duration } from "moment";

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
  markerEnd,
}) => {
  const obj = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const [, labelX] = obj;
  let [edgePath, , labelY] = obj;
  let labelRenderer = "";

  if (data?.fwdEdge) {
    if (sourceY !== targetY) {
      if (data?.fromNodeOutDegree > 1) {
        if (sourceY < targetY) {
          labelY = targetY - 14;
        } else {
          labelY = targetY + 2;
        }
      } else {
        if (sourceY < targetY) {
          labelY = sourceY + 1;
        } else {
          labelY = sourceY - 13;
        }
      }
    }
    labelRenderer = `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`;
  }

  if (data?.backEdge) {
    const height = data?.backEdgeHeight * 35;
    edgePath = `M ${sourceX} ${sourceY} L ${sourceX} ${
      -height + 5
    } Q ${sourceX} ${-height} ${sourceX - 5} ${-height} L ${
      targetX + 5
    } ${-height} Q ${targetX} ${-height} ${targetX} ${
      -height + 5
    } L ${targetX} ${targetY}`;
    const centerX = (sourceX + targetX) / 2;
    labelRenderer = `translate(-50%, -50%) translate(${centerX}px,${-height}px)`;
  }

  if (data?.selfEdge) {
    edgePath = `M ${sourceX} ${sourceY} L ${sourceX} ${
      sourceY - 15
    } Q ${sourceX} ${sourceY - 20} ${sourceX - 5} ${sourceY - 20} L ${
      targetX + 5
    } ${targetY - 20} Q ${targetX} ${sourceY - 20} ${targetX} ${
      sourceY - 15
    } L ${targetX} ${sourceY}`;
    const centerX = (sourceX + targetX) / 2;
    labelRenderer = `translate(-50%, -50%) translate(${centerX}px,${labelY}px)`;
  }

  // Highlight the edge on isFull - thick red line
  let edgeStyle, wmStyle, pendingStyle;

  if (data?.isFull) {
    edgeStyle = {
      stroke: "red",
      strokeWidth: "0.125rem",
      fontWeight: 700,
    };
  }

  if (sourceY === targetY || data?.backEdge) {
    wmStyle = { marginTop: "-0.87rem" };
    pendingStyle = { marginTop: "0.15rem" };
  } else {
    wmStyle = { right: "0.1rem" };
    pendingStyle = { left: "0.1rem" };
  }
  const getMinWM = () => {
    if (data?.edgeWatermark?.watermarks) {
      return Math.min.apply(null, data?.edgeWatermark?.watermarks);
    } else {
      return -1;
    }
  };

  const getDelay = () => {
    const str = " behind now";
    const fetchWMTime = data?.edgeWatermark?.WMFetchTime;
    const minWM = getMinWM();
    const startTime = minWM === -1 ? 0 : minWM;
    const endTime = fetchWMTime || Date.now();
    const diff = duration(endTime - startTime);

    const years = diff.years();
    const months = diff.months();
    const days = diff.days();
    const hours = diff.hours();
    const minutes = diff.minutes();
    const seconds = diff.seconds();
    const milliseconds = diff.milliseconds();

    if (years > 0) {
      return `${years}yr ${months}mo` + str;
    } else if (months > 0) {
      return `${months}mo ${days}d` + str;
    } else if (days > 0) {
      return `${days}d ${hours}hr` + str;
    } else if (hours > 0) {
      return `${hours}hr ${minutes}min` + str;
    } else if (minutes > 0) {
      return `${minutes}min ${seconds}sec` + str;
    } else if (seconds > 0) {
      return `${seconds}sec ${milliseconds}ms` + str;
    } else {
      return `${milliseconds}ms` + str;
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
          markerEnd={markerEnd}
        />
      </svg>
      <EdgeLabelRenderer>
        <div
          className={"edge"}
          style={{
            transform: labelRenderer,
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
