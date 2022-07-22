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
