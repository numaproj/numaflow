import {memo, useState} from "react";
import { Handle, NodeProps, Position } from "reactflow";
import { Tooltip } from "@mui/material";
import "./Node.css";
import { GetNodeInfoValueComponent } from "./NodeUtil"

const SinkNode = ({
  data,
  isConnectable,
  targetPosition = Position.Top,
}: NodeProps) => {

  const [ bgColor, setBgColor] = useState("#82A9C9");
  const [ isOpen, setIsOpen ] = useState(false);

  const handleNodeEnter = () => {
    if (data?.vertexMetrics?.error) {
      setBgColor("gray");
      setIsOpen(true);
    }
  }

  const handleNodeLeave = () => {
    if (data?.vertexMetrics?.error) {
      setBgColor("#82A9C9");
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
          className={"react-flow__node-output"}
          style={{
            background: `${bgColor}`,
            color: "#333",
            border: "1px solid #5e7a91",
            boxShadow: "1",
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
        </div>
      </Tooltip>
    </div>
  );
};

export default memo(SinkNode);
