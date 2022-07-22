import { memo } from "react";
import { Handle, NodeProps, Position } from "react-flow-renderer";
import { Tooltip } from "@mui/material";
import "./Node.css";

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
              <div>1 min: {data?.rate?.ratePerMin}</div>
              <div>5 min: {data?.rate?.ratePerFiveMin}</div>
              <div>15 min: {data?.rate?.ratePerFifteenMin}</div>
            </div>
          }
          arrow
        >
          <div className={"node-rate"}>{data?.rate?.ratePerMin}/min</div>
        </Tooltip>
        <Handle
          type="target"
          position={targetPosition}
          isConnectable={isConnectable}
        />
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

export default memo(UDFNode);
