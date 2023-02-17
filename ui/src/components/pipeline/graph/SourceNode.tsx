import { memo } from "react";
import { Handle, NodeProps, Position } from "reactflow";
import { Tooltip } from "@mui/material";
import "./Node.css";
import { GetNodeInfoValueComponent } from "./NodeUtil"

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
        <Tooltip
          title={
            <div className={"node-tooltip"}>
              <div>Processing Rates</div>
              <div>1 min: {data?.vertexMetrics?.ratePerMin}/sec</div>
              <div>5 min: {data?.vertexMetrics?.ratePerFiveMin}/sec</div>
              <div>15 min: {data?.vertexMetrics?.ratePerFifteenMin}/sec</div>
            </div>
          }
          arrow
        >
          <div className={"node-rate"}>
            {data?.vertexMetrics?.ratePerMin}/sec
          </div>
        </Tooltip>
        {GetNodeInfoValueComponent(data)}
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
