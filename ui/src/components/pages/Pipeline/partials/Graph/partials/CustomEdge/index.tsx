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

  return (
    <>
      <path
        id={id}
        className="react-flow__edge-path"
        d={edgePath}
        style={edgeStyle}
      />
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
                  <div>
                    {new Date(
                      Math.min.apply(null, data?.edgeWatermark?.watermarks)
                    ).toISOString()}
                  </div>
                </div>
              }
              arrow
              placement={"top"}
            >
              <div className={"edge-label"} style={wmStyle}>
                {Math.min.apply(null, data?.edgeWatermark?.watermarks)}
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
