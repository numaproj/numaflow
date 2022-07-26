import { memo } from "react";
import { Handle, NodeProps, Position } from "react-flow-renderer";
import { Tooltip } from "@mui/material";
import "./Node.css";

const SourceNode = ({
  data,
  isConnectable,
  sourcePosition = Position.Bottom,
}: NodeProps) => {
  return (
    <div>
      <div
        className={"react-flow__node-input"}
        style={{
          background: "#34BFFF",
          boxShadow: "1",
          color: "#333",
          border: "1px solid #2382ad",
          cursor: "pointer",
          fontFamily: "IBM Plex Sans",
          fontWeight: 400,
          fontSize: "0.50rem",
        }}
      >
        {data?.vertexWatermark?.isWaterMarkEnabled && (
          <Tooltip
            title={
              <div className={"node-tooltip"}>
                <div>Watermark</div>
                <div>{data?.vertexWatermark?.watermarkLocalTime}</div>
              </div>
            }
            arrow
            placement={"top"}
          >
            <div className={"node-watermark"}>
              {data?.vertexWatermark?.watermark}
            </div>
          </Tooltip>
        )}
        <Tooltip
          title={
            <div className={"node-tooltip"}>
              <div>Processing Rates</div>
              <div>1 min: {data?.vertexMetrics?.ratePerMin}</div>
              <div>5 min: {data?.vertexMetrics?.ratePerFiveMin}</div>
              <div>15 min: {data?.vertexMetrics?.ratePerFifteenMin}</div>
            </div>
          }
          arrow
        >
          <div className={"node-rate"}>
            {data?.vertexMetrics?.ratePerMin}/min
          </div>
        </Tooltip>

        {data?.label}
        <Handle
          type="source"
          position={sourcePosition}
          isConnectable={isConnectable}
        />
      </div>
    </div>
  );
};
export default memo(SourceNode);
