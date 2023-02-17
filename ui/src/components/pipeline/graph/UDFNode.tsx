import { memo } from "react";
import { Handle, NodeProps, Position } from "reactflow";
import { Tooltip } from "@mui/material";
import "./Node.css";
import { GetNodeInfoValueComponent } from "./NodeUtil"

const UDFNode = ({
  data,
  isConnectable,
  targetPosition = Position.Top,
  sourcePosition = Position.Bottom,
}: NodeProps) => {
  return (
    <div>
      <div
        className={"react-flow__node-default"}
        style={{
          background: "#82DBE4",
          boxShadow: "1",
          color: "#333",
          border: "1px solid #59959c",
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
        <Handle
          type="target"
          position={targetPosition}
          isConnectable={isConnectable}
        />
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

export default memo(UDFNode);
