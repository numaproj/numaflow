import {memo} from 'react';
import {Handle, NodeProps, Position} from 'react-flow-renderer';
import {Tooltip} from "@mui/material";
import "./Node.css";

const SinkNode = ({data, isConnectable, targetPosition = Position.Top}: NodeProps) => {
    return (
        <div>
            <div className={"react-flow__node-output"} style={{
                background: "#82A9C9",
                color: "#333",
                border: "1px solid #5e7a91",
                boxShadow: "1",
                cursor: "pointer",
                fontFamily: "IBM Plex Sans",
                fontWeight: 400,
                fontSize: "0.50rem"

            }}>
                <Tooltip title={<div className={"node-tooltip"}>
                    <div>Processing Rates</div>
                    <div>1 min: {data?.rate?.ratePerMin}</div>
                    <div>5 min: {data?.rate?.ratePerFiveMin}</div>
                    <div>15 min: {data?.rate?.ratePerFifteenMin}</div>
                </div>} arrow>
                    <div className={"node-rate"}>{data?.rate?.ratePerMin}/min</div>
                </Tooltip>
                <Handle type="target" position={targetPosition} isConnectable={isConnectable}/>
                {data?.label}
            </div>
        </div>

    );
}

export default memo(SinkNode);