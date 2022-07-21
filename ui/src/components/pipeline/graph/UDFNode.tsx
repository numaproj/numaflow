import {memo} from 'react';
import {Handle, NodeProps, Position} from 'react-flow-renderer';

const handleStyle = { left: 100 };


const UDFNode = ({ data, isConnectable, targetPosition = Position.Top,
                     sourcePosition = Position.Bottom, }: NodeProps) => {
    return (
        <div>
            <div className={"react-flow__node-input"} style = {{
                background: "#82DBE4",
                boxShadow: "1",
                color: "#333",
                border: "1px solid #f1c5a8",
                cursor: "pointer",
                fontFamily: "IBM Plex Sans",
                fontWeight: 400,
                fontSize: "0.50rem"

            }}>
                <Handle type="target" position={targetPosition} isConnectable={isConnectable} />
                {data?.label}
                <Handle type="source" position={sourcePosition} isConnectable={isConnectable} />
            </div>
            <Handle position={Position.Top} id="a" style={handleStyle} >
                <svg  width={20} height={20}
                      viewBox="0 0 30 30" xmlns="http://www.w3.org/2000/svg">
                    <g>
                        <rect width="40" height="10" style={{fill: "#B1b192"}} />
                        <text x="5" y="5" fontSize={"0.8em"} fontWeight={"bold"}  color="blue" textAnchor="center" >{data?.rate.toFixed(2)}</text>

                    </g>
                </svg>
            </Handle>
        </div>

    );
}

//SourceNode.displayName = 'SourceNode';

export default memo(UDFNode);