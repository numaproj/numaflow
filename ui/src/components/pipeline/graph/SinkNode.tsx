import {memo} from 'react';
import {Handle, NodeProps, Position} from 'react-flow-renderer';

const handleStyle = { left: 90 };


const SinkNode = ({ data, isConnectable, targetPosition = Position.Top }: NodeProps) => {
    return (
        <div>
            <div className={"react-flow__node-output"} style = {{
                background: "#82A9C9",
                color: "#333",
                border: "1px solid #b8cee2",
                boxShadow: "1",
                cursor: "pointer",
                fontFamily: "IBM Plex Sans",
                fontWeight: 400,
                fontSize: "0.50rem"

            }}>
                <Handle type="target" position={targetPosition} isConnectable={isConnectable} />
                {data?.label}
            </div>
            <Handle position={Position.Top} id="b" style={handleStyle} >
                <svg  width={20} height={20}
                      viewBox="0 0 30 30" xmlns="http://www.w3.org/2000/svg">
                    <g>
                        <rect width="40" height="10" style={{fill: "#7a8aa3"}} />
                        <text x="5" y="5" fontSize={"0.8em"} fontWeight={"bold"}  color="blue" textAnchor="center" >{data?.rate.toFixed(2)}</text>

                    </g>
                </svg>
            </Handle>
        </div>

    );
}

export default memo(SinkNode);