import {memo} from 'react';
import {Handle, NodeProps, Position} from 'react-flow-renderer';
import {Tooltip} from "@mui/material";

const handleStyle = { left: 100 };


const SourceNode = ({ data, isConnectable, sourcePosition = Position.Bottom }: NodeProps) => {
    return (
    <div>
    <div className={"react-flow__node-default"} style = {{
        background: "#34BFFF",
        boxShadow: "1",
        color: "#333",
        border: "1px solid #b8cee2",
        cursor: "pointer",
        fontFamily: "IBM Plex Sans",
        fontWeight: 400,
        fontSize: "0.50rem"

    }}>
        {data?.label}
        <Handle type="source" position={sourcePosition} isConnectable={isConnectable}/>
    </div>
        <Handle position={Position.Top} id="a" style={handleStyle} >
            <Tooltip title="ProcessingRates &#010; 5min" placement="top" style={{cursor:"pointer"}} arrow>
            <svg  width={20} height={20}
                  viewBox="0 0 30 30" xmlns="http://www.w3.org/2000/svg">
                <g>
                <rect width="100" height="10" style={{fill: "white", backgroundColor: "black"}} />
                    <text x="5" y="5" fontSize={"0.8em"} fontWeight={"bold"}  color="blue" textAnchor="center" >{data?.rate.toFixed(2)}</text>

                </g>
            </svg>
            </Tooltip>
        </Handle>
    </div>

);
}
export default memo(SourceNode);