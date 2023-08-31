import { FC, memo, useState } from "react";
import { Tooltip } from "@mui/material";
import { Handle, NodeProps, Position } from "reactflow";
import { GetNodeInfoValueComponent } from "./partials/NodeLabelInfo";

import "reactflow/dist/style.css";
import "./style.css";

const getColor = (nodeType: string) => {
  return nodeType === "source"
    ? "#34BFFF"
    : nodeType === "udf"
    ? "#82DBE4"
    : "#82A9C9";
};

const getBorderColor = (nodeType: string) => {
  return nodeType === "source"
    ? "#2382ad"
    : nodeType === "udf"
    ? "#59959c"
    : "#5e7a91";
};

const CustomNode: FC<NodeProps> = ({
  data,
  isConnectable,
  sourcePosition = Position.Bottom,
  targetPosition = Position.Top,
}: NodeProps) => {
  const [bgColor, setBgColor] = useState(() => getColor(data?.type));
  const [isOpen, setIsOpen] = useState(false);

  const handleNodeEnter = () => {
    if (data?.vertexMetrics?.error) {
      setBgColor("gray");
      setIsOpen(true);
    }
  };

  const handleNodeLeave = () => {
    if (data?.vertexMetrics?.error) {
      setBgColor(() => getColor(data?.type));
      setIsOpen(false);
    }
  };

  return (
    <div data-testid={data?.name}>
      <Tooltip
        title={
          <div className={"node-tooltip"}> 1 or more pods are not running </div>
        }
        placement={"top"}
        open={isOpen}
        arrow
      >
        <div
          className={"react-flow__node-input"}
          style={{
            background: `${bgColor}`,
            boxShadow: "1",
            color: "#333",
            border: `0.0625rem solid ${getBorderColor(data?.type)}`,
            cursor: "pointer",
            fontFamily: "IBM Plex Sans",
            fontWeight: 400,
            fontSize: "0.50rem",
          }}
          onMouseEnter={handleNodeEnter}
          onMouseLeave={handleNodeLeave}
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
          {(data?.type === "udf" || data?.type === "sink") && (
            <Handle
              type="target"
              id="0"
              position={targetPosition}
              isConnectable={isConnectable}
            />
          )}
          {(data?.type === "source" || data?.type === "udf") && (
            <Handle
              type="source"
              id="0"
              position={sourcePosition}
              isConnectable={isConnectable}
            />
          )}
          {data?.centerSourceHandle && (
            <Handle type="source" id="1" position={Position.Top} />
          )}
          {data?.centerTargetHandle && (
            <Handle type="target" id="1" position={Position.Top} />
          )}
          {data?.quadHandle && (
            <>
              <Handle
                type="source"
                id="2"
                position={Position.Top}
                style={{
                  left: "75%",
                }}
              />
              <Handle
                type="target"
                id="2"
                position={Position.Top}
                style={{
                  left: "25%",
                }}
              />
            </>
          )}
        </div>
      </Tooltip>
    </div>
  );
};
export default memo(CustomNode);
