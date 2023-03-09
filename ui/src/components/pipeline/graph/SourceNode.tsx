import {memo, useState} from "react";
import { Handle, NodeProps, Position } from "reactflow";
import { Tooltip } from "@mui/material";
import "./Node.css";
import { GetNodeInfoValueComponent } from "./NodeUtil"

const SourceNode = ({
  data,
  isConnectable,
  sourcePosition = Position.Bottom,
}: NodeProps) => {

  const [ bgColor, setBgColor] = useState("#34BFFF");
  const [ isOpen, setIsOpen ] = useState(false);

  const handleNodeEnter = () => {
    if (data?.vertexMetrics?.error) {
      setBgColor("gray");
      setIsOpen(true);
    }
  }

  const handleNodeLeave = () => {
    if (data?.vertexMetrics?.error) {
      setBgColor("#34BFFF");
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
          className={"react-flow__node-input"}
          style={{
            background: `${bgColor}`,
            boxShadow: "1",
            color: "#333",
            border: "1px solid #2382ad",
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
export default memo(SourceNode);
