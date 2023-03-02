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
    let wmStyle;
    let pendingStyle;
    if (sourceY === targetY) {
        wmStyle = { marginTop: "-12px" }
        pendingStyle = { marginTop: "1px" }
    } else {
        wmStyle = { right: "1px" }
        pendingStyle = { left: "1px" }
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
                        width: "fit-content",
                        display: "flex",
                        justifyContent: "center",
                    }}
                    className="nodrag nopan"
                >
                    {data?.edgeWatermark?.isWaterMarkEnabled && (
                        <Tooltip
                            title={
                                <div className={"edge-tooltip"}>
                                    <div className={"edge-common-tooltip"}>
                                        <div>Watermark</div>
                                        <div>{new Date(Math.min.apply(null, data.edgeWatermark.watermarks)).toISOString()}</div>
                                    </div>
                                </div>
                            }
                            arrow
                            placement={"top"}
                        >
                            <div className={"edge-watermark"} style={wmStyle}>
                                {Math.min.apply(null, data.edgeWatermark.watermarks)}
                            </div>
                        </Tooltip>
                    )}
                    <Tooltip
                        title={
                            <div className={"edge-tooltip"}>
                                <div className={"edge-common-tooltip"}>Pending</div>
                            </div>
                        }
                        arrow
                        placement={"bottom"}
                    >
                        <div className={"edge-backpressure"} style={pendingStyle}>
                            {data.backpressureLabel}
                        </div>
                    </Tooltip>
                </div>
            </EdgeLabelRenderer>
        </>
    );
};

export default memo(CustomEdge);
