import { FC, memo } from "react";
import { Tooltip } from "@mui/material";
import { EdgeProps, EdgeLabelRenderer, getSmoothStepPath } from "reactflow";
import "./CustomEdge.css";

const CustomEdge: FC<EdgeProps> = ({
                                       id,
                                       sourceX,
                                       sourceY,
                                       targetX,
                                       targetY,
                                       sourcePosition,
                                       targetPosition,
                                       data,
                                   }) => {
    const [edgePath, labelX, labelY] = getSmoothStepPath({
        sourceX,
        sourceY,
        sourcePosition,
        targetX,
        targetY,
        targetPosition,
    });

    // Color the edge on isFull
    let edgeStyle;
    if (data.isFull) {
        edgeStyle = {
            stroke: "red",
            strokeWidth: "4px",
            fontWeight: 700,
        };
    }

    return (
        <>
            <path id={id} className="react-flow__edge-path" d={edgePath}  style={edgeStyle}/>
            <EdgeLabelRenderer>
                <div
                    style={{
                        position: 'absolute',
                        transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
                        cursor: "pointer",
                        fontFamily: "IBM Plex Sans",
                        fontWeight: 400,
                        fontSize: "0.6rem",
                        width: "40px",
                        display: "flex",
                        justifyContent: "center",

                    }}
                    className="nodrag nopan"
                >
                    {data?.edgeWatermark?.isWaterMarkEnabled && (
                        <Tooltip
                            title={
                                <div className={"edge-tooltip"}>
                                    <div className={"edge-common-tooltip"}>Watermark</div>
                                </div>
                            }
                            arrow
                            placement={"top"}
                        >
                            <div className={"edge-watermark"}>
                                {Math.min.apply(null, data.edgeWatermark.watermarks)}
                            </div>
                        </Tooltip>
                    )}
                    <Tooltip
                        title={
                            <div className={"edge-tooltip"}>
                                <div className={"edge-common-tooltip"}>Back-pressure</div>
                            </div>
                        }
                        arrow
                        placement={"bottom"}
                    >
                        <div className={"edge-backpressure"}>
                            {data.backpressureLabel}
                        </div>
                    </Tooltip>
                </div>
            </EdgeLabelRenderer>
        </>
    );
};

export default memo(CustomEdge);