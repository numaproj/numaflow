import {memo, useState} from "react";
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

  const [ bgColor, setBgColor ] = useState("#82DBE4");
  const [ isOpen, setIsOpen ] = useState(false);

  const handleNodeEnter = () => {
    if (data?.vertexMetrics?.error) {
        setBgColor("gray");
        setIsOpen(true);
    }
  }

  const handleNodeLeave = () => {
    if (data?.vertexMetrics?.error) {
        setBgColor("#82DBE4");
        setIsOpen(false);
    }
  }

  return (
    <div>
      <Tooltip
        title={<div className={"node-tooltip"}> 1 or more pods are not running </div>}
        placement={"top"}
        open={isOpen}
        arrow
      >
        <div
          className={"react-flow__node-default"}
          style={{
            background: `${bgColor}`,
            boxShadow: "1",
            color: "#333",
            border: "1px solid #59959c",
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
      </Tooltip>
    </div>
  );
};

export default memo(UDFNode);
